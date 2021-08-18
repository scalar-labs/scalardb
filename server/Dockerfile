FROM docker.io/busybox:1.32 AS tools

ENV GRPC_HEALTH_PROBE_VERSION v0.3.2

# Install grpc_health_probe for kubernetes.
# https://kubernetes.io/blog/2018/10/01/health-checking-grpc-servers-on-kubernetes/
RUN set -x && \
    wget -q -O grpc_health_probe "https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-amd64" && \
    chmod +x grpc_health_probe

FROM openjdk:8u292-jre-slim

COPY --from=tools grpc_health_probe /usr/local/bin/

# Fix CVE-2021-3520, CVE-2021-33560, CVE-2021-20231, CVE-2021-20232, CVE-2020-24659, and CVE-2021-20305
RUN apt-get update && \
    apt-get install -y --no-install-recommends liblz4-1=1.8.3-1+deb10u1 libgcrypt20=1.8.4-5+deb10u1 libgnutls30=3.6.7-4+deb10u7 libhogweed4=3.4.1-1+deb10u1 && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /scalardb

# The path should be relative from build/docker. Running `gradle docker`
# (provided by com.palantir.docker plugin) will copy this Dockerfile and
# server.tar and log4j.properties to build/docker.
ADD server.tar .

WORKDIR /scalardb/server

COPY log4j.properties .

ENV SCALARDB_SERVER_OPTS -Dlog4j.configuration=file:log4j.properties

ENTRYPOINT ["./bin/scalardb-server"]
CMD ["--config", "database.properties"]

EXPOSE 60051 8080
