FROM docker.io/busybox:1.32 AS tools

ENV DOCKERIZE_VERSION v0.6.1

# Install dockerize
# Support for use cases where environment variables are used to configure the database
RUN set -x && \
    wget -q "https://github.com/jwilder/dockerize/releases/download/${DOCKERIZE_VERSION}/dockerize-linux-amd64-${DOCKERIZE_VERSION}.tar.gz" && \
    tar -xzvf "dockerize-linux-amd64-${DOCKERIZE_VERSION}.tar.gz" && \
    ./dockerize --version

FROM ghcr.io/scalar-labs/jre8:1.0.0

COPY --from=tools dockerize /usr/local/bin/

WORKDIR /scalardb

# The path should be relative from build/docker. Running `gradle docker`
# (provided by com.palantir.docker plugin) will copy this Dockerfile and
# graphql.tar, log4j2.properties and database.properties.tmpl to build/docker.
ADD graphql.tar .

ENV SCALAR_DB_CONTACT_POINTS 'cassandra'
ENV SCALAR_DB_CONTACT_PORT '9042'
ENV SCALAR_DB_USERNAME ''
ENV SCALAR_DB_PASSWORD ''
ENV SCALAR_DB_STORAGE 'cassandra'
ENV SCALAR_DB_TRANSACTION_MANAGER 'consensus-commit'
ENV SCALAR_DB_ISOLATION_LEVEL ''
ENV SCALAR_DB_CONSENSUSCOMMIT_SERIALIZABLE_STRATEGY ''

WORKDIR /scalardb/graphql

COPY log4j2.properties .
# Support use cases where environment variables are used to configure the database
COPY database.properties.tmpl .
COPY docker-entrypoint.sh .

RUN groupadd -r --gid 201 scalardb && \
    useradd -r --uid 201 -g scalardb scalardb
RUN chown -R scalardb:scalardb /scalardb/graphql

USER 201

ENV JAVA_OPT_MAX_RAM_PERCENTAGE '70.0'
ENV SCALARDB_GRAPHQL_SERVER_OPTS -Dlog4j.configurationFile=file:log4j2.properties

ENTRYPOINT ["./docker-entrypoint.sh"]

CMD ["./bin/scalardb-graphql-server", "--config", "database.properties"]

EXPOSE 8080
