# You can see sha256 hash in the following page:
# - https://hub.docker.com/_/eclipse-temurin/tags
FROM eclipse-temurin:21.0.13.1_2-jre-resolute@sha256:4249bd74371ea0e4b7d40da7f1d4354a894be80812abf7e4ea5bdd421dc3f6c8

RUN export DEBIAN_FRONTEND=noninteractive \
 && apt-get update \
 && apt-get upgrade -y --no-install-recommends \
 && apt-get autoremove -y \
 && rm -rf /var/lib/apt/lists/* \
 && groupadd -r --gid 201 scalardb \
 && useradd -r --uid 201 -g scalardb -s /usr/sbin/nologin scalardb

COPY scalardb-schema-loader-*.jar /app.jar

USER 201:201

ENTRYPOINT ["java", "-jar", "/app.jar"]
