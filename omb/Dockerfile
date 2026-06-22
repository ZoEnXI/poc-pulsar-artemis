###############################################################################
# OMB runner image — JDK 17 / Maven 3.9.9
# Clones OpenMessaging Benchmark master, builds, installs package into /omb/.
###############################################################################
FROM eclipse-temurin:17-jdk AS builder

ARG MAVEN_VERSION=3.9.9
ARG OMB_REPO=https://github.com/openmessaging/benchmark.git
ARG OMB_COMMIT=master

# Install Maven
RUN apt-get update && apt-get install -y --no-install-recommends git curl && \
    curl -fsSL https://archive.apache.org/dist/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz \
      | tar -xz -C /opt && \
    ln -s /opt/apache-maven-${MAVEN_VERSION}/bin/mvn /usr/local/bin/mvn && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Clone OMB
RUN git clone --depth 1 --branch ${OMB_COMMIT} ${OMB_REPO} .

# Build — skip tests, skip checkstyle for speed; production use: remove -Dcheckstyle.skip
RUN mvn clean install -DskipTests -Dcheckstyle.skip -T 4 --no-transfer-progress

# Extract the distribution package
RUN cp -r package/target/openmessaging-benchmark-*-bin/openmessaging-benchmark-*/ /omb-dist/

###############################################################################
FROM eclipse-temurin:17-jre

RUN apt-get update && apt-get install -y --no-install-recommends bash curl && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /omb-dist /omb

WORKDIR /omb

# Directories for driver overrides and workloads (mounted from host)
RUN mkdir -p /omb/drivers /omb/workloads /omb/results

ENTRYPOINT ["tail", "-f", "/dev/null"]
