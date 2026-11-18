# You can see sha256 hash in the following page:
# - https://hub.docker.com/_/eclipse-temurin/tags
FROM eclipse-temurin:21.0.13_9-jre-resolute@sha256:f8622d0220c9e218d7a33f24a7b9e1ad6bc726085f8b0ae74e1adf85f67f05c4

RUN export DEBIAN_FRONTEND=noninteractive \
 && apt-get update \
 && apt-get upgrade -y --no-install-recommends \
 && apt-get autoremove -y \
 && rm -rf /var/lib/apt/lists/* \
 && groupadd -r --gid 201 scalardb \
 && useradd -r --uid 201 -g scalardb -s /usr/sbin/nologin scalardb

COPY scalardb-data-loader-*.jar /app.jar

USER 201:201

ENTRYPOINT ["java", "-jar", "/app.jar"]
