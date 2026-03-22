FROM flink:2.0-java17

RUN wget -q -O /opt/flink/lib/flink-metrics-prometheus-2.0.0.jar \
    https://repo1.maven.org/maven2/org/apache/flink/flink-metrics-prometheus/2.0.0/flink-metrics-prometheus-2.0.0.jar

ENV HADOOP_CONF_DIR=/opt/flink/conf
ENV HADOOP_HOME=/opt/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin
