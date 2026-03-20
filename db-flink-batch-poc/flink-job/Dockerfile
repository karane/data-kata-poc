# Build stage
FROM maven:3.9-eclipse-temurin-17 AS builder

WORKDIR /app
COPY pom.xml .
RUN mvn org.apache.maven.plugins:maven-dependency-plugin:3.6.1:go-offline -q

COPY src ./src
RUN mvn package -DskipTests -q

# Runtime / submitter stage
FROM flink:2.0-java17

COPY start.sh /opt/flink/bin/
RUN chmod +x /opt/flink/bin/start.sh

ENTRYPOINT ["/opt/flink/bin/start.sh"]

# The Flink cluster will load the JAR from usrlib when submitted
COPY --from=builder /app/target/flink-job-*.jar /opt/flink/usrlib/flink-job.jar
