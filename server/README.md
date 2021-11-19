# Scalar DB Server

## Build & Install

To build and install the Scalar DB Server, use gradle installDist, which will build the source files and install an executable, required jars:

```
$ ./gradlew installDist
```

## Run

This runs Scalar DB Server:

```
$ cd server/build/install/server
$ export SCALARDB_SERVER_OPTS="<your JVM options>"
$ bin/scalardb-server --config <your configuration file path>
```

## Docker

### Build

This builds the Scalar DB Server Docker image:

```
$ ./gradlew docker
```

### Run

This runs the Scalar DB Server (you need to specify your local configuration file path with `-v` flag):

```
$ docker run -v <your local configuration file path>:/scalardb/server/database.properties -d -p 60051:60051 -p 8080:8080 ghcr.io/scalar-labs/scalardb-server:<version>

# For DEBUG logging
$ docker run -v <your local configuration file path>:/scalardb/server/database.properties -e JAVA_OPTS=-Dlog4j.logLevel=DEBUG -d -p 60051:60051 -p 8080:8080 ghcr.io/scalar-labs/scalardb-server:<version>

# For custom log configuration
$ docker run -v <your local configuration file path>:/scalardb/server/database.properties -v <your custom log4j2 configuration file path>:/scalardb/server/log4j2.properties -d -p 60051:60051 -p 8080:8080 ghcr.io/scalar-labs/scalardb-server:<version>

# Use environment variables
docker run --env SCALAR_DB_CONTACT_POINTS=cassandra --env SCALAR_DB_CONTACT_PORT=9042 --env SCALAR_DB_USERNAME=cassandra --env SCALAR_DB_PASSWORD=cassandra --env SCALAR_DB_STORAGE=cassandra -d -p 60051:60051 -p 8080:8080 ghcr.io/scalar-labs/scalardb-server:<version> dockerize -template database.properties.tmpl:database.properties /scalardb/server/bin/scalardb-server --config=database.properties

# For JMX
$ docker run -v <your local configuration file path>:/scalardb/server/database.properties -e JAVA_OPTS="-Djava.rmi.server.hostname=<your container hostname or IP address> -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote=true -Dcom.sun.management.jmxremote.local.only=false -Dcom.sun.management.jmxremote.port=9990 -Dcom.sun.management.jmxremote.rmi.port=9990 -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false" -d -p 60051:60051 -p 8080:8080 -p 9990:9990 ghcr.io/scalar-labs/scalardb-server:<version>
```
