FROM jupyter/pyspark-notebook:latest

USER root

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    libsasl2-dev \
    python3-dev \
    gcc \
    iputils-ping \
    net-tools \
    curl \
    wget \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Install Hadoop client
RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-3.2.1/hadoop-3.2.1.tar.gz \
    && tar -xzf hadoop-3.2.1.tar.gz -C /opt \
    && mv /opt/hadoop-3.2.1 /opt/hadoop \
    && rm hadoop-3.2.1.tar.gz \
    && ln -s /opt/hadoop/bin/hdfs /usr/bin/hdfs \
    && mkdir -p /opt/hadoop/etc/hadoop/

# Copy Hadoop configuration
COPY core-site.xml /opt/hadoop/etc/hadoop/

# Install additional Python packages
RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    findspark \
    pyhive \
    thrift \
    pure-sasl \
    thrift-sasl

# Create necessary directories
RUN mkdir -p /home/jovyan/.local/share/jupyter/kernels/pyspark
RUN mkdir -p /home/jovyan/work/util

# Configure PySpark kernel
COPY kernel.json /home/jovyan/.local/share/jupyter/kernels/pyspark/
COPY pyspark-init.py /home/jovyan/work/util/

# Set permissions
RUN chown -R jovyan:users /home/jovyan/.local /home/jovyan/work /opt/hadoop

USER jovyan

# Set environment variables
ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9-src.zip:${PYTHONPATH}"
ENV SPARK_OPTS="--driver-java-options=-Xms1024M --driver-java-options=-Xmx4096M --driver-java-options=-Dlog4j.logLevel=info" 
ENV SPARK_MASTER=spark://spark-master:7077
ENV SPARK_DRIVER_HOST=jupyter
ENV SPARK_DRIVER_BINDADDRESS=0.0.0.0
ENV HADOOP_HOME=/opt/hadoop
ENV PATH=$PATH:/opt/hadoop/bin

# Create a README with instructions
WORKDIR /home/jovyan/work 