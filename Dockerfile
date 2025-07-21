FROM apache/airflow:2.10.3

# Set user to airflow to avoid permission issues on copy
USER airflow

# Copy requirements file
COPY requirements.txt /requirements.txt

# Switch to root for installing system dependencies
USER root

# Install Java (required by PySpark) and ANT (if needed)
RUN apt update && \
    apt-get install -y openjdk-17-jdk ant && \
    apt-get clean

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:$PATH"

# Switch back to airflow user
USER airflow

# Upgrade pip and install Python packages
RUN pip install --upgrade pip && \
    pip install -r /requirements.txt && \
    pip install pyspark kafka-python
