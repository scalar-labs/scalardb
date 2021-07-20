# Scalar DB server

This document explains the Scalar DB server that is a gRPC interface of Scalar DB.

## Usage

### Install

We have Docker images in [our repository](https://github.com/orgs/scalar-labs/packages) and zip archives of Scalar DB server available in [releases](https://github.com/scalar-labs/scalardb/releases).

If you are interested in building from source, run the following command: 

```
./gradlew installDist
```

Of course, you can archive the jar and libraries by `./gradlew distZip` and so on.

### Start

For Docker images, you can start Scalar DB server with the following commands:

```
docker pull ghcr.io/scalar-labs/scalardb-server:<version>
docker run -v <your local configration file path>:/scalardb/server/database.properties -d ghcr.io/scalar-labs/scalardb-server:<version>
```

For zip archives, you can start Scalar DB server with the following commands:

```
unzip server.zip
cd server
bin/scalardb-server --config <your configration file path>
```

## Further documentation

[scalardb-samples](https://github.com/scalar-labs/scalardb-samples)
