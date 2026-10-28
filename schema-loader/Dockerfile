# You can see sha256 hash in the following page:
# - https://hub.docker.com/_/eclipse-temurin/tags
FROM eclipse-temurin:21.0.12.1_1-jre-resolute@sha256:c010ef3a101af9fa953f2a844238292c1772ae224ab65f0afdd3997c86b05f90

RUN export DEBIAN_FRONTEND=noninteractive \
 && apt-get update \
 && apt-get upgrade -y --no-install-recommends \
 && apt-get autoremove -y \
 && rm -rf /var/lib/apt/lists/* \
 && groupadd -r --gid 201 scalardb \
 && useradd -r --uid 201 -g scalardb -s /usr/sbin/nologin scalardb

# Temporary fix for https://emudhra.we-have-a-problem.com/
# We can remove this RUN after we update to the fixed version of Java.
RUN keytool -delete -cacerts -storepass changeit -alias "cn_emsign_ecc_root_ca__g3,o_emudhra_technologies_limited,ou_emsign_pki,c_in [jdk]" \
 && keytool -delete -cacerts -storepass changeit -alias "cn_emsign_root_ca__g1,o_emudhra_technologies_limited,ou_emsign_pki,c_in [jdk]" \
 && keytool -delete -cacerts -storepass changeit -alias "cn_emsign_ecc_root_ca__c3,o_emudhra_inc,ou_emsign_pki,c_us [jdk]" \
 && keytool -delete -cacerts -storepass changeit -alias "cn_emsign_root_ca__c1,o_emudhra_inc,ou_emsign_pki,c_us [jdk]"

COPY scalardb-schema-loader-*.jar /app.jar

USER 201:201

ENTRYPOINT ["java", "-jar", "/app.jar"]
