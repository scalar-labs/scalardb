# CBRL demo app runner image.
#
# A slim JDK 8 image bundling the LOCALLY-BUILT ScalarDB (4.0.0-SNAPSHOT) CBRL fat jar
# plus the compiled demo driver and CbrlRestore launcher. The fat jar is the
# schema-loader shadow jar, which bundles core + CBRL + the JDBC drivers + the
# SchemaLoader entrypoint, so ONE image can run every ScalarDB step of the demo:
#
#   schema-loader : java ... com.scalar.db.schemaloader.SchemaLoader --coordinator ...
#   workload      : java ... CbrlDemoDriver populate|open|transfer|check ...
#   restore       : java ... CbrlDemoDriver restore ...  (-> CbrlRestoreMain -> CbrlRestore)
#
# The entrypoint sets the classpath (driver classes + fat jar); `docker compose run app
# <MainClass> <args...>` supplies the main class and its arguments.
FROM eclipse-temurin:8-jdk

WORKDIR /app

# The local CBRL fat jar (schema-loader shadow jar). Copied into the build context by run.sh.
COPY app/scalardb-app.jar /app/scalardb-app.jar

# Compile the two demo sources against the fat jar (default-package classes -> /app/classes).
COPY driver/CbrlDemoDriver.java /app/srcjava/CbrlDemoDriver.java
COPY src/CbrlRestoreMain.java /app/srcjava/CbrlRestoreMain.java
RUN mkdir -p /app/classes && javac -cp /app/scalardb-app.jar -d /app/classes \
      /app/srcjava/CbrlDemoDriver.java /app/srcjava/CbrlRestoreMain.java

# Container-specific properties (service hostnames) + the schema.
COPY container/scalardb-multi-storage.properties /app/scalardb-multi-storage.properties
COPY container/scalardb-multi-storage-copy.properties /app/scalardb-multi-storage-copy.properties
COPY schema/transfer-multi-storage-schema.json /app/schema.json

# Driver classes first, then the fat jar. The command (main class + args) is appended by
# `docker compose run app ...`.
ENTRYPOINT ["java", "-cp", "/app/classes:/app/scalardb-app.jar"]
