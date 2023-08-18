{% include end-of-support.html %}

# Scalar DB server

Scalar DB server is a gRPC server that implements Scalar DB interface. 
With Scalar DB server, you can use Scalar DB features from multiple programming languages that are supported by gRPC.

Currently, we provide only a Java client officially, and we will support other language clients officially in the future.
Of course, you can generate language-specific client stubs by yourself.
However, note that it is not necessarily straightforward to implement a client since it's using a bidirectional streaming RPC in gRPC, and you need to be familiar with it.

This document explains how to install and use Scalar DB server.

## Install prerequisites

Scalar DB server is written in Java. So the following software is required to run it.

* [Oracle JDK 8](https://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html) (OpenJDK 8) or higher

## Install Scalar DB server

We have Docker images in [our repository](https://github.com/orgs/scalar-labs/packages) and zip archives of Scalar DB server available in [releases](https://github.com/scalar-labs/scalardb/releases).

If you are interested in building from source, run the following command: 

```
./gradlew installDist
```

Of course, you can archive the jar and libraries by `./gradlew distZip` and so on.

## Configure Scalar DB Server

You need a property file holding the configuration for Scalar DB Server. 
It contains two sections, Server configurations and Underlining storage/database configurations.

```
#
# Server configurations
#

# Port number of Scalar DB server. 60051 by deafult
#scalar.db.server.port=60051

# Prometheus exporter port. Use 8080 if this is not given. Prometheus exporter will not be started if a negative number is given.
#scalar.db.server.prometheus_exporter_port=8080

#
# Underlining storage/database configurations
#

# Comma separated contact points
scalar.db.contact_points=localhost

# Port number for all the contact points. Default port number for each database is used if empty.
#scalar.db.contact_port=9042

# Credential information to access the database
#scalar.db.username=cassandra
#scalar.db.password=cassandra

# Storage implementation. Either cassandra or cosmos or dynamo or jdbc can be set. Default storage is cassandra.
#scalar.db.storage=cassandra
```

### Start Scalar DB server

For Docker images, you can start Scalar DB server with the following commands:

```
docker pull ghcr.io/scalar-labs/scalardb-server:<version>
docker run -v <your local property file path>:/scalardb/server/database.properties -d ghcr.io/scalar-labs/scalardb-server:<version>
```

For zip archives, you can start Scalar DB server with the following commands:

```
unzip scalardb-server-<version>.zip
cd scalardb-server-<version>
bin/scalardb-server --config <your property file path>
```

## Usage of the Java client of Scalar DB server

You can use the Java client of Scalar DB server in almost the same way as other storages/databases.
The difference is that you need to set `scalar.db.storage` and `scalar.db.transaction_manager` to `grpc` in your client side property file.

```
# Comma separated contact points
scalar.db.contact_points=<Scalar DB server host>

# Port number for all the contact points
scalar.db.contact_port=60051

# Storage implementation
scalar.db.storage=grpc

# The type of the transaction manager
scalar.db.transaction_manager=grpc
```

## Further documentation

[Scalar DB Server Sample](https://github.com/scalar-labs/scalardb-samples/tree/main/scalardb-server-sample)
