from __future__ import annotations

import os
import sys
import json
import logging
from typing import Any
from datetime import datetime, timedelta

import kafka
import pandas as pd
import pyspark.sql.functions as F
from cassandra.cluster import Cluster
from pyspark.sql import DataFrame
from kafka.admin import KafkaAdminClient, NewTopic

from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.providers.apache.kafka.sensors.kafka import AwaitMessageSensor
from airflow.models import Connection
from airflow.utils import db

from Operators.SparkProcessOperator import SparkProcessOperator

# Import schema
dags_dir = os.path.dirname(os.path.abspath(__file__))
streaming_processing_path = os.path.abspath(os.path.join(dags_dir, "../scripts/spark_streaming_processing"))
sys.path.append(streaming_processing_path)
import finnhub_processing

# Logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# Constants
CASSANDRA_KEYSPACE = "finance"
CASSANDRA_TABLE_NAME = "stock_trade"
KAFKA_TOPIC_NAME = "finnhub_stock"

# Airflow default args
default_args = {
    'owner': 'longdata',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'email': ['longdata.010203@gmail.com']
}

# Create Kafka connections
def load_connections() -> None:
    db.merge_conn(Connection(
        conn_id="t1-3",
        conn_type="kafka",
        extra=json.dumps({"socket.timeout.ms": 10, "bootstrap.servers": "kafka:9092"}),
    ))
    db.merge_conn(Connection(
        conn_id="t5",
        conn_type="kafka",
        extra=json.dumps({
            "bootstrap.servers": "kafka:9092",
            "group.id": "t5",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "security.protocol": "PLAINTEXT"
        }),
    ))

# Wait-for-message filter function (used directly in sensor)
def await_financial(message) -> bool:
    try:
        data = json.loads(message.value())
        if data.get("v", 0) > 0 and data.get("s"):
            logger.info(f"Received valid message: {data}")
            return True
        logger.warning(f"Invalid message: {data}")
        return False
    except json.JSONDecodeError:
        logger.error(f"JSON decode error: {message.value()}")
        return False

# Cassandra utility
def connect_to_cassandra():
    cluster = Cluster(["cassandra"], port=9042)
    return cluster.connect()

def create_keyspace(**kwargs) -> None:
    session = connect_to_cassandra()
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE}
        WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': 1 }};
    """)
    logger.info(f"Keyspace {CASSANDRA_KEYSPACE} created.")

def create_table(**kwargs):
    session = connect_to_cassandra()
    session.set_keyspace(CASSANDRA_KEYSPACE)
    session.execute(f"""
        CREATE TABLE IF NOT EXISTS {CASSANDRA_TABLE_NAME} (
            id UUID,
            trade_conditions list<text>,
            last_price float,
            symbol text,
            datetime timestamp,
            volume int,
            PRIMARY KEY (id)
        );
    """)
    logger.info(f"Table {CASSANDRA_TABLE_NAME} created in keyspace {CASSANDRA_KEYSPACE}.")

# DAG Definition
@dag(
    dag_id="FinnhubApi_to_Cassandra_v104",
    default_args=default_args,
    start_date=datetime(2024, 11, 15),
    schedule_interval="0 23 * * Mon,Wed,Fri",
    catchup=False,
    tags=["ETL", "Kafka", "Spark", "Cassandra"]
)
def etl_pipeline_FinnhubApi_to_Cassandra():

    connect_kafka = PythonOperator(
        task_id="connect_kafka",
        python_callable=load_connections
    )

    @task(multiple_outputs=True)
    def create_topic_finnhub_financial_kafka(**context) -> dict[str, Any]:
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=["kafka:9092"])
            if KAFKA_TOPIC_NAME not in admin_client.list_topics():
                topic = NewTopic(name=KAFKA_TOPIC_NAME, num_partitions=1, replication_factor=1)
                admin_client.create_topics([topic])
                logger.info(f"Kafka topic {KAFKA_TOPIC_NAME} created.")
            else:
                logger.info(f"Kafka topic {KAFKA_TOPIC_NAME} already exists.")
            context["ti"].xcom_push(key="topic_name", value=KAFKA_TOPIC_NAME)
            return {"bootstrap_servers": "kafka:9092", "topic_name": KAFKA_TOPIC_NAME}
        except Exception as e:
            logger.error(f"Error creating topic: {e}")
            raise

    wait_financial_message_sensor = AwaitMessageSensor(
        kafka_config_id="t5",
        task_id="wait_financial_message_sensor",
        topics=["{{ ti.xcom_pull(key='topic_name') }}"],
        apply_function=await_financial,  # ✅ Direct reference!
        poll_timeout=360,
        poll_interval=5,
        xcom_push_key="retrieved_message",
    )

    @task(templates_dict={"received": "{{ ti.xcom_pull(key='topic_name') }}"})
    def processing_with_spark_streaming(**context) -> None:
        spark = SparkProcessOperator(
            task_id="processing_with_spark_streaming",
            appName=f"spark-streaming-{datetime.today()}",
            config={}
        )

        spark_streaming_df: DataFrame = spark.connect_to_kafka(
            schema=finnhub_processing.schema,
            topic_name=context["templates_dict"]["received"]
        )

        select_df = spark_streaming_df.selectExpr("CAST(value AS STRING)") \
            .select(F.from_json(F.col("value"), finnhub_processing.schema).alias("data")) \
            .select("data.*") \
            .withColumn("timestamp", F.from_unixtime(F.col("datetime") / 1000)) \
            .withColumn("id", F.expr("uuid()")) \
            .select(
                F.col('c').alias('trade_conditions'),
                F.col('p').alias('last_price'),
                F.col('s').alias('symbol'),
                F.col('t').alias('datetime'),
                F.col('v').alias('volume'),
                F.col("id")
            )

        logger.info("Starting Spark streaming write to Cassandra...")
        streaming_query = select_df.writeStream \
            .foreachBatch(lambda batch_df, _: batch_df.write \
                .format("org.apache.spark.sql.cassandra") \
                .options(table=CASSANDRA_TABLE_NAME, keyspace=CASSANDRA_KEYSPACE) \
                .mode("append") \
                .save()) \
            .outputMode("update") \
            .start()

        streaming_query.awaitTermination()

    create_keyspace_cassandra = PythonOperator(
        task_id="create_keyspace",
        python_callable=create_keyspace
    )

    create_table_cassandra = PythonOperator(
        task_id="create_table",
        python_callable=create_table
    )

    # DAG flow
    topic_info = create_topic_finnhub_financial_kafka()
    spark_task = processing_with_spark_streaming()
    
    connect_kafka >> topic_info >> wait_financial_message_sensor >> spark_task
    create_keyspace_cassandra >> create_table_cassandra >> spark_task

etl_pipeline_FinnhubApi_to_Cassandra()
