#!/bin/sh

mkdir -p /opt/flink/plugins/s3-fs-hadoop
cp /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/s3-fs-hadoop/

cat > /opt/flink/conf/core-site.xml <<EOF
<?xml version="1.0"?>
<configuration>
  <property>
    <name>fs.s3a.endpoint</name>
    <value>http://rustfs.railway.internal:9000</value>
  </property>
  <property>
    <name>fs.s3a.access.key</name>
    <value>${RUSTFS_ACCESS_KEY}</value>
  </property>
  <property>
    <name>fs.s3a.secret.key</name>
    <value>${RUSTFS_SECRET_KEY}</value>
  </property>
  <property>
    <name>fs.s3a.path.style.access</name>
    <value>true</value>
  </property>
  <property>
    <name>fs.s3a.connection.ssl.enabled</name>
    <value>false</value>
  </property>
  <property>
    <name>fs.s3a.aws.credentials.provider</name>
    <value>org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider</value>
  </property>
</configuration>
EOF

cat /opt/flink/conf/core-site.xml
export HADOOP_CONF_DIR=/opt/flink/conf
export AWS_ACCESS_KEY_ID=${RUSTFS_ACCESS_KEY}
export AWS_SECRET_ACCESS_KEY=${RUSTFS_SECRET_KEY}

echo "Submitting batch job..."
flink run -m flink-jobmanager:8081 -c com.poc.BatchJob /opt/flink/usrlib/flink-job.jar --from ${JOB_FROM_DATE} --to ${JOB_TO_DATE}
echo "Batch job completed."

java -cp /opt/flink/usrlib/flink-job.jar com.poc.LineageReporter --from ${JOB_FROM_DATE} --to ${JOB_TO_DATE} --run-id 12345 --metrics-file /tmp/job-metrics.json
