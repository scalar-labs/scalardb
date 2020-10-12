## Getting Started with Scalar DB and Cosmos DB

## Overview
This document briefly explains how you can get started a simple electronic money application with scalar DB and Cosmos DB.

## Install prerequisites

Scalar DB is written in Java and uses Cosmos DB as an underlining storage implementation, so the following software is required to run it.

* [Oracle JDK 8](https://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html) (OpenJDK 8) or higher
* Other libraries used from the above are automatically installed through gradle

In addition to the above, the following software is needed to use schema tools.

* [leiningen](https://leiningen.org/)

### Cosmos DB setup

* [Azure Cosmos DB](https://docs.microsoft.com/en-us/azure/cosmos-db/introduction)
    * Take a look at [this document](https://docs.microsoft.com/en-us/azure/cosmos-db/create-cosmosdb-resources-portal#create-an-azure-cosmos-db-account) for how to set up Cosmos DB.
    * Choose API as `Core (SQL)` while creating the cosmos db.
    * Change default `Consistency Level` from `SESSION` to `STRONG` in Settings of `Default Consistency`.

From here, we assume Oracle JDK 8 and leiningen are properly installed in your local environment, and Cosmos DB is running in Azure environment.

## Build

Please use [this](getting-started.md#Build) to create the build.

## Configure the Cosmos DB connection
    
The [**scalardb.properties**](../conf/database.properties) file holds the configuration for Scalar DB. Basically, Cosmos DB account_uri and primary_key will get from Azure Cosmos DB account.
    
```
# Comma separated contact points
scalar.db.contact_points=<YOUR_ACCOUNT_URI>

# Port number for all the contact points. Default port number for each database is used if empty.
#scalar.db.contact_port=

# Credential information to access the database
scalar.db.username=
scalar.db.password=<YOUR_ACCOUNT_PASSWORD>

# Storage implementation. Either cassandra or cosmos can be set. Default storage is cassandra.
scalar.db.storage=cosmos
```

## Set up database schema

First of all, you need to define how the data will be organized (a.k.a database schema) in the application with Scalar DB database schema.
Here is a database schema for the sample application. For the supported data types, please see [this doc](schema.md) for more details.

```json
{
  "emoney.account": {
    "transaction": false,
    "partition-key": [
      "id"
    ],
    "clustering-key": [],
    "columns": {
      "id": "TEXT",
      "balance": "INT"
    },
    "ru": 400
  }
}
```

To load the schema file, please run the following command.
```
$ java -jar $PATH_TO_SCALARDB/target/scalar-schema.jar --cosmos -h <YOUR_ACCOUNT_URI> -p <YOUR_ACCOUNT_PASSWORD> -f emoney-storage.json
```

Please use [this](getting-started.md#store--retrieve-data-with-storage-service) to check different storage functions.

## Set up database schema with transaction

To apply transaction, we can just add a key `transaction` and value as `true` in Scalar DB scheme. For instance, we modify our qa.question schema.

```json
{
  "emoney.account": {
    "transaction": true,
    "partition-key": [
      "id"
    ],
    "clustering-key": [],
    "columns": {
      "id": "TEXT",
      "balance": "INT"
    },
    "ru": 400
  }
}
```

Before reapplying the schema, please drop the existing namespace first by issuing the following.
(Sorry you need to issue implementation specific commands to do this.)
```
$ java -jar $PATH_TO_SCALARDB/target/scalar-schema.jar --cosmos -h <YOUR_ACCOUNT_URI> -p <YOUR_ACCOUNT_PASSWORD> -D
$ java -jar $PATH_TO_SCALARDB/target/scalar-schema.jar --cosmos -h <YOUR_ACCOUNT_URI> -p <YOUR_ACCOUNT_PASSWORD> -f emoney-transaction.json
```

Please use [this](getting-started.md#store--retrieve-data-with-transaction-service) to check different storage functions.
