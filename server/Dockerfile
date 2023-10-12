FROM docker.io/busybox:1.36 AS tools

ENV GRPC_HEALTH_PROBE_VERSION v0.4.17
ENV DOCKERIZE_VERSION v0.6.1

# Install grpc_health_probe for kubernetes.
# https://kubernetes.io/blog/2018/10/01/health-checking-grpc-servers-on-kubernetes/
RUN set -x && \
    wget -q -O grpc_health_probe "https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-amd64" && \
    chmod +x grpc_health_probe

# Install dockerize
# Support for use cases where environment variables are used to configure the database
RUN set -x && \
    wget -q "https://github.com/jwilder/dockerize/releases/download/${DOCKERIZE_VERSION}/dockerize-linux-amd64-${DOCKERIZE_VERSION}.tar.gz" && \
    tar -xzvf "dockerize-linux-amd64-${DOCKERIZE_VERSION}.tar.gz" && \
    ./dockerize --version

FROM ghcr.io/scalar-labs/jre8:1.1.15

COPY --from=tools dockerize /usr/local/bin/
COPY --from=tools grpc_health_probe /usr/local/bin/

WORKDIR /scalardb

# The path should be relative from build/docker. Running `gradle docker`
# (provided by com.palantir.docker plugin) will copy this Dockerfile and
# server.tar, log4j2.properties and database.properties.tmpl to build/docker.
ADD server.tar .

ENV SCALAR_DB_CONTACT_POINTS 'cassandra'
ENV SCALAR_DB_CONTACT_PORT '9042'
ENV SCALAR_DB_USERNAME ''
ENV SCALAR_DB_PASSWORD ''
ENV SCALAR_DB_STORAGE 'cassandra'
ENV SCALAR_DB_TRANSACTION_MANAGER 'consensus-commit'
ENV SCALAR_DB_ISOLATION_LEVEL ''
ENV SCALAR_DB_CONSENSUSCOMMIT_SERIALIZABLE_STRATEGY ''

WORKDIR /scalardb/server

COPY database.properties.tmpl .
COPY log4j2.properties.tmpl .
COPY docker-entrypoint.sh .

RUN groupadd -r --gid 201 scalardb && \
    useradd -r --uid 201 -g scalardb scalardb
RUN chown -R scalardb:scalardb /scalardb/server

USER 201

ENV JAVA_OPT_MAX_RAM_PERCENTAGE '70.0'
ENV JAVA_OPTS '-Dlog4j.configurationFile=file:log4j2.properties'

ENTRYPOINT ["./docker-entrypoint.sh"]

CMD ["dockerize", "-template", "database.properties.tmpl:database.properties", \
    "-template", "log4j2.properties.tmpl:log4j2.properties", \
    "./bin/scalardb-server", "--config=database.properties"]

EXPOSE 60051 8080
