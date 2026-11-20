# Scalar managed base image:
# - https://github.com/orgs/scalar-labs/packages/container/package/jre21
# You can confirm the digest with:
#   docker buildx imagetools inspect ghcr.io/scalar-labs/jre21:<TAG>
FROM ghcr.io/scalar-labs/jre21:21.0.13_9.202611200209@sha256:40eecc3df566280bf378c8e57bb7fbf5edefdc706b2bf759e47cc5b41d35c106

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
