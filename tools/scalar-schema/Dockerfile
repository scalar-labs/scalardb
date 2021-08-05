FROM clojure:openjdk-11-lein-slim-buster AS BUILD
COPY . /code
WORKDIR /code
RUN lein uberjar

FROM openjdk:11-jre-slim
WORKDIR /app
COPY --from=BUILD /code/target/scalar-schema-standalone-*-SNAPSHOT.jar ./app.jar

# ENV variables
ENV SCALAR_DB_STORAGE cassandra
ENV SCALAR_DB_HOST localhost
ENV SCALAR_DB_USERNAME user
ENV SCALAR_DB_PASSWORD pass
ENV SCALAR_DB_SCHEMA schema.json
ENV SCALAR_DB_REPLICATION_FACTOR 1

# Run command
ENTRYPOINT java -jar app.jar --$SCALAR_DB_STORAGE -h $SCALAR_DB_HOST -u $SCALAR_DB_USERNAME -p $SCALAR_DB_PASSWORD -f $SCALAR_DB_SCHEMA -R $SCALAR_DB_REPLICATION_FACTOR