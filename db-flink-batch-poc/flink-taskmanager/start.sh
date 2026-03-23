#!/bin/sh
set -e

mkdir -p /opt/flink/plugins/s3-fs-hadoop
cp /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/s3-fs-hadoop/

# Flink-native S3 config (maps to Hadoop fs.s3a.* internally)
cat >> /opt/flink/conf/flink-conf.yaml << 'EOF'
s3.access-key: rustfsaccess
s3.secret-key: rustfssecret
s3.endpoint: http://rustfs.railway.internal:9000
s3.path.style.access: true
s3.connection.ssl.enabled: false
EOF

# Hadoop S3A config (belt-and-suspenders)
cat > /opt/flink/conf/core-site.xml << 'EOF'
<?xml version="1.0"?>
<configuration>
  <property><name>fs.s3a.endpoint</name><value>http://rustfs.railway.internal:9000</value></property>
  <property><name>fs.s3a.access.key</name><value>rustfsaccess</value></property>
  <property><name>fs.s3a.secret.key</name><value>rustfssecret</value></property>
  <property><name>fs.s3a.path.style.access</name><value>true</value></property>
  <property><name>fs.s3a.connection.ssl.enabled</name><value>false</value></property>
  <property><name>fs.s3a.aws.credentials.provider</name><value>org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider</value></property>
</configuration>
EOF

exec /docker-entrypoint.sh taskmanager
