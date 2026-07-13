FROM maven:3.9.9-eclipse-temurin-11 AS build
WORKDIR /build
COPY pom.xml ./
COPY src ./src
RUN mvn -q -DskipTests package

FROM eclipse-temurin:11-jre
WORKDIR /app
COPY --from=build /build/target/flink-risk-engine-java-1.0-SNAPSHOT.jar /app/app.jar
ENTRYPOINT ["java", "-cp", "/app/app.jar", "com.banking.fraud.RiskScoringJob"]
