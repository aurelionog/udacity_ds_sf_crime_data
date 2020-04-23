
FROM ubuntu:18.04

LABEL image=Spark-base-image
ENV SPARK_VERSION=2.4.5
ENV HADOOP_VERSION=2.7

RUN apt-get update -qq && \
    apt-get install -qq -y gnupg2 wget openjdk-8-jdk scala
    
WORKDIR /
RUN wget --no-verbose http://ftp.unicamp.br/pub/apache/spark/spark-2.4.5/spark-2.4.5-bin-hadoop2.7.tgz

RUN tar -xzf /spark-2.4.5-bin-hadoop2.7.tgz && \
    mv spark-2.4.5-bin-hadoop2.7 spark && \
    echo "export PATH=$PATH:/spark/bin" >> ~/.bashrc

#Expose the UI Port 4040
EXPOSE 4040