FROM openjdk:8-jdk-slim

#COPY --from=openjdk:8-jdk-slim /usr/local/openjdk-8 /usr/local/openjdk-8
#RUN export JAVA_HOME=/usr/local/openjdk-8
RUN echo "deb http://ftp.us.debian.org/debian sid main" >> /etc/apt/sources.list

RUN apt update && DEBIAN_FRONTEND=noninteractive apt install -y --no-install-recommends \
      git \
      curl \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /target && git clone https://github.com/apache/spark.git

ARG SPARK_VERSION=3.1.2
ARG SCALA_VERSION=2.12
ARG HADOOP_VERSION=3.3.0

RUN cd spark && git checkout v$SPARK_VERSION \
 && ./dev/change-scala-version.sh $SCALA_VERSION

RUN cd spark &&./dev/make-distribution.sh --name spark --tgz -Phive -Phive-thriftserver -Pyarn -Pscala-$SCALA_VERSION -DskipTests -Phadoop-provided -Dhadoop.version=$HADOOP_VERSION 

