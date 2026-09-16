# Multi-stage Dockerfile for distributed data processing runtime
FROM ubuntu:22.04 AS base

ENV DEBIAN_FRONTEND=noninteractive
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV HADOOP_HOME=/opt/hadoop
ENV PATH=$PATH:$JAVA_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

# Install core system packages, OpenJDK 8, and Python 3
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-8-jdk-headless \
    python3 \
    python3-pip \
    curl \
    tar \
    ssh \
    rsync \
    net-tools \
    iputils-ping \
    && rm -rf /var/lib/apt/lists/*

# Install PySpark & dependencies
RUN pip3 install --no-cache-dir pyspark==3.5.1 numpy matplotlib

WORKDIR /workspace
CMD ["/bin/bash"]
