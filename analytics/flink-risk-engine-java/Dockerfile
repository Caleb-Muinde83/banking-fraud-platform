FROM maven:3.9.9-eclipse-temurin-11 AS build
WORKDIR /build
COPY pom.xml ./
COPY src ./src
RUN mvn -q -DskipTests package

# CHANGED (P1 resolution): base image switched from eclipse-temurin:11-jre
# to the official Flink image -- this is the same image
# flink-jobmanager/flink-taskmanager already use, so it ships with
# /opt/flink/bin/flink (the CLI needed to SUBMIT the job to that real,
# already-running cluster) and is guaranteed version-compatible with it.
# Previously this container ran the job's main() directly via plain
# `java -cp`, which silently fell back to Flink's embedded
# LocalStreamEnvironment inside this one process -- a fully separate,
# invisible mini-cluster, never connected to flink-jobmanager/
# flink-taskmanager at all. See P1 in the handoff doc.
FROM flink:1.18-scala_2.12-java11
WORKDIR /app
COPY --from=build /build/target/flink-risk-engine-java-1.0-SNAPSHOT.jar /app/app.jar
COPY submit-job.sh /app/submit-job.sh

# Invoked via `/bin/bash /app/submit-job.sh` rather than making the script
# executable and calling it directly -- sidesteps any COPY-time permission
# bit weirdness entirely, one less thing to debug.
ENTRYPOINT ["/bin/bash", "/app/submit-job.sh"]