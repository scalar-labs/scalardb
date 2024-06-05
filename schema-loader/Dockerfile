FROM eclipse-temurin:8-jre-jammy

RUN apt-get update && apt-get upgrade -y \
 && rm -rf /var/lib/apt/lists/*

COPY scalardb-schema-loader-*.jar /app.jar

RUN groupadd -r --gid 201 scalar && \
    useradd -r --uid 201 -g scalar scalar

USER 201

ENTRYPOINT ["java", "-jar", "/app.jar"]
