FROM docker.io/busybox:1.36 AS tools

ENV GRPC_HEALTH_PROBE_VERSION v0.4.22

# Install grpc_health_probe for kubernetes.
# https://kubernetes.io/blog/2018/10/01/health-checking-grpc-servers-on-kubernetes/
RUN set -x && \
    wget -q -O grpc_health_probe "https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-amd64" && \
    chmod +x grpc_health_probe

FROM ghcr.io/scalar-labs/jre8:1.1.16

COPY --from=tools grpc_health_probe /usr/local/bin/

WORKDIR /scalardb

# The path should be relative from build/docker. Running `gradle docker`
# (provided by com.palantir.docker plugin) will copy this Dockerfile and
# server.tar, log4j2.properties and database.properties to build/docker.
ADD server.tar .

WORKDIR /scalardb/server

COPY database.properties .
COPY log4j2.properties .
COPY docker-entrypoint.sh .

RUN groupadd -r --gid 201 scalardb && \
    useradd -r --uid 201 -g scalardb scalardb
RUN chown -R scalardb:scalardb /scalardb/server

USER 201

ENV JAVA_OPT_MAX_RAM_PERCENTAGE '70.0'
ENV JAVA_OPTS '-Dlog4j.configurationFile=file:log4j2.properties'

ENTRYPOINT ["./docker-entrypoint.sh"]

CMD ["./bin/scalardb-server", "--config=database.properties"]

EXPOSE 60051 8080
