FROM bde2020/hive:2.3.2-postgresql-metastore

# Install additional dependencies
RUN apt-get update && \
    apt-get install -y wget curl netcat-openbsd && \
    rm -rf /var/lib/apt/lists/*

# Download and install Hadoop
RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-3.2.1/hadoop-3.2.1.tar.gz && \
    tar -xzf hadoop-3.2.1.tar.gz -C /opt && \
    rm hadoop-3.2.1.tar.gz && \
    ln -s /opt/hadoop-3.2.1/etc/hadoop /etc/hadoop && \
    mkdir -p /opt/hadoop-3.2.1/logs

# Copy configuration files
COPY conf/hive-site.xml /opt/hive/conf/
COPY wait-for-it.sh /opt/hive/
COPY core-site.xml hdfs-site.xml /opt/hadoop-3.2.1/etc/hadoop/

# Set permissions
RUN chmod 644 /opt/hive/conf/hive-site.xml && \
    chmod +x /opt/hive/wait-for-it.sh && \
    chmod 644 /opt/hadoop-3.2.1/etc/hadoop/core-site.xml && \
    chmod 644 /opt/hadoop-3.2.1/etc/hadoop/hdfs-site.xml && \
    sed -i 's/<n>/<name>/g' /opt/hadoop-3.2.1/etc/hadoop/core-site.xml && \
    sed -i 's/<n>/<name>/g' /opt/hadoop-3.2.1/etc/hadoop/hdfs-site.xml

# Create entrypoint script with proper Hadoop path setup
RUN echo '#!/bin/bash\n\
export HADOOP_HOME=/opt/hadoop-3.2.1\n\
export HADOOP_CONF_DIR=/opt/hadoop-3.2.1/etc/hadoop\n\
export PATH=$HADOOP_HOME/bin:$PATH\n\
\n\
# Verify Hadoop installation\n\
if [ ! -d "$HADOOP_HOME" ]; then\n\
  echo "Error: Hadoop installation not found at $HADOOP_HOME"\n\
  exit 1\n\
fi\n\
\n\
echo "Waiting for PostgreSQL..."\n\
until nc -z postgres 5432; do\n\
  echo "Waiting for PostgreSQL server..."\n\
  sleep 2\n\
done\n\
\n\
echo "Waiting for namenode..."\n\
until nc -z namenode 9000; do\n\
  echo "Waiting for namenode..."\n\
  sleep 2\n\
done\n\
\n\
echo "Creating necessary HDFS directories..."\n\
hdfs dfs -mkdir -p /tmp\n\
hdfs dfs -mkdir -p /user/hive/warehouse\n\
hdfs dfs -chmod g+w /tmp\n\
hdfs dfs -chmod g+w /user/hive/warehouse\n\
\n\
echo "Initializing Hive schema..."\n\
schematool -dbType postgres -initSchema\n\
\n\
echo "Starting HiveServer2..."\n\
hiveserver2 --hiveconf hive.server2.enable.doAs=false\n\
' > /opt/hive/entrypoint.sh && \
    chmod +x /opt/hive/entrypoint.sh

# Expose ports
EXPOSE 10000 10002

# Set environment variables
ENV HADOOP_HOME=/opt/hadoop-3.2.1 \
    HADOOP_CONF_DIR=/opt/hadoop-3.2.1/etc/hadoop \
    PATH=/opt/hadoop-3.2.1/bin:/opt/hive/bin:$PATH

# Use entrypoint script
ENTRYPOINT ["/opt/hive/entrypoint.sh"] 