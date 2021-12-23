# Scalar DB GraphQL Server

## Build & Install

To build and install the Scalar DB GraphQL Server, use gradle installDist, which will build the source files and install an executable, required jars:

```
$ ./gradlew installDist
```

## Run

This runs Scalar DB GraphQL Server:

```
$ cd graphql/build/install/graphql
$ export SCALARDB_GRAPHQL_SERVER_OPTS="<your JVM options>"
$ bin/scalardb-graphql-server --config <your configuration file path>
```

## Docker

### Build

This builds the Scalar DB GraphQL Server Docker image:

```
$ ./gradlew docker
```

### Run

This runs the Scalar DB GraphQL Server (you need to specify your local configuration file path with `-v` flag):

```
$ docker run -v <your local configuration file path>:/scalardb/graphql/database.properties -d -p 8080:8080 ghcr.io/scalar-labs/scalardb-graphql:<version>

# For DEBUG logging
$ docker run -v <your local configuration file path>:/scalardb/graphql/database.properties -e JAVA_OPTS=-Dlog4j.logLevel=DEBUG -d -p 8080:8080 ghcr.io/scalar-labs/scalardb-graphql:<version>

# For custom log configuration
$ docker run -v <your local configuration file path>:/scalardb/graphql/database.properties -v <your custom log4j2 configuration file path>:/scalardb/graphql/log4j2.properties -d -p 8080:8080 ghcr.io/scalar-labs/scalardb-graphql:<version>

# For JMX
$ docker run -v <your local configuration file path>:/scalardb/graphql/database.properties -e JAVA_OPTS="-Djava.rmi.server.hostname=<your container hostname or IP address> -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote=true -Dcom.sun.management.jmxremote.local.only=false -Dcom.sun.management.jmxremote.port=9990 -Dcom.sun.management.jmxremote.rmi.port=9990 -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false" -d -p 8080:8080 -p 9990:9990 ghcr.io/scalar-labs/scalardb-graphql:<version>
```
