FROM bde2020/hive:2.3.2-postgresql-metastore

# Copy custom hive-site.xml and wait-for-it script
COPY conf/hive-site.xml /opt/hive/conf/
COPY wait-for-it.sh /opt/hive/

# Set permissions
RUN chmod 644 /opt/hive/conf/hive-site.xml && \
    chmod +x /opt/hive/wait-for-it.sh

# Create entrypoint script with proper Hadoop path setup
RUN echo '#!/bin/bash\n\
export HADOOP_HOME=/opt/hadoop-3.2.1\n\
export HADOOP_CONF_DIR=/opt/hadoop-3.2.1/etc/hadoop\n\
export PATH=$HADOOP_HOME/bin:$PATH\n\
\n\
until nc -z postgres 5432; do\n\
  echo "Waiting for postgres..."\n\
  sleep 1\ndone\n\
schematool -dbType postgres -initSchema\n\
hiveserver2' > /opt/hive/entrypoint.sh && \
    chmod +x /opt/hive/entrypoint.sh

# Expose ports
EXPOSE 10000 10002

# Use entrypoint script
ENTRYPOINT ["/opt/hive/entrypoint.sh"] 