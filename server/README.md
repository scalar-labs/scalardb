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
$ bin/scalardb-server
```

## Docker

### Build

This builds the Scalar DB Server Docker image:

```
$ ./gradlew docker
```

### Run

This runs the Scalar DB Server (you need to specify your local `database.properties` path with `-v` flag):

```
$ docker run -v <your local database.properties path>:/scalardb/server/database.properties -d -p 60051:60051 ghcr.io/scalar-labs/scalardb-server:<version>

# For DEBUG logging
$ docker run -v <your local database.properties path>:/scalardb/server/database.properties -e JAVA_OPTS=-Dlog4j.logLevel=DEBUG -d -p 60051:60051 ghcr.io/scalar-labs/scalardb-server:<version>
```
