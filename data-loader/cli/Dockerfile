# You can see sha256 hash in the following page:
# - https://hub.docker.com/_/eclipse-temurin/tags
FROM eclipse-temurin:21.0.13_9-jre-resolute@sha256:556903b1f40c0a0ddc0bd05cf413436a3b2ce48c9ee19fc199434898fd3ea2b9

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
