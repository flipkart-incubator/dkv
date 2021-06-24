FROM openjdk:8u292-jre-slim as jre

FROM python:3-slim

COPY --from=jre /usr/local/openjdk-8 /opt/java/openjdk-8

RUN ln -s /opt/java/openjdk-8 /opt/java/current
ENV PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/opt/java/current/bin
ENV JAVA_HOME=/opt/java/current

# RUN pip install -U setuptools pymongo==3.11 requests

COPY distribution/target/ycsb-0.18.0-SNAPSHOT.tar.gz /usr/share/dkv-ycsb.tar.gz
RUN mkdir /usr/share/dkv-ycsb && \
    cd /usr/share && \
    tar xfz dkv-ycsb.tar.gz -C dkv-ycsb --strip-components 1 && \
    rm dkv-ycsb.tar.gz

RUN ln -s /usr/share/dkv-ycsb /opt/ycsb-0.18

COPY k8s/start.sh /start.sh

ENV ACTION='' DBTYPE='' WORKLOAD='' DBARGS='' RECNUM='' OPNUM='' JVM_ARGS='' NUM_THREADS=''

WORKDIR "/opt/ycsb-0.18"

ENTRYPOINT ["/start.sh"]
