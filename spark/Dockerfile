FROM bitnami/spark:3.5.0

USER root

# Install wget
RUN apt-get update && apt-get install -y wget

# Download Hudi JARs
RUN mkdir -p /opt/bitnami/spark/jars && \
    cd /opt/bitnami/spark/jars && \
    wget https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark3-bundle_2.12/0.14.0/hudi-spark3-bundle_2.12-0.14.0.jar

# Set permissions
RUN chown -R 1001:1001 /opt/bitnami/spark/jars

USER 1001 