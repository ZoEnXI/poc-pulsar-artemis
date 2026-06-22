###############################################################################
# Spring Boot POC — Java 25 / Spring Boot 4.1.0 / Spring Pulsar 2.0.6
###############################################################################
FROM eclipse-temurin:25-jdk AS builder

WORKDIR /build
COPY pom.xml .
COPY src ./src

# Build — Maven Wrapper optionnel ; on suppose Maven disponible dans l'image JDK
RUN apt-get update && apt-get install -y --no-install-recommends maven && \
    mvn package -DskipTests --no-transfer-progress && \
    rm -rf /var/lib/apt/lists/*

###############################################################################
FROM eclipse-temurin:25-jre

WORKDIR /app

COPY --from=builder /build/target/insurance-events-app-*.jar app.jar

EXPOSE 8081

ENTRYPOINT ["java", "-jar", "app.jar"]
