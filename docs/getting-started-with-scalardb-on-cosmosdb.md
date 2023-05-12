# Getting Started with ScalarDB on Cosmos DB for NoSQL

## Overview
This document briefly explains how you can get started with ScalarDB on Cosmos DB for NoSQL with a simple electronic money application.

## Install prerequisites

ScalarDB is written in Java. So the following software is required to run it.

* [Oracle JDK 8](https://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html) (or OpenJDK 8) or higher
* Other libraries used from the above are automatically installed through gradle

## Cosmos DB for NoSQL setup
You also need to set up a Cosmos DB for NoSQL account to get started with ScalarDB on Cosmos DB for NoSQL.

1. Create a Cosmos DB for NoSQL account according to the official document [Create an Azure Cosmos DB account](https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/quickstart-portal#create-account)
1. Configure the **default consistency level** to **STRONG** according to the official document [Configure the default consistency level](https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/how-to-manage-consistency#configure-the-default-consistency-level)

From here, we assume Oracle JDK 8 is properly installed in your local environment and the Azure Cosmos DB for NoSQL account is properly configured in Azure.

## Configure ScalarDB
    
The **scalardb.properties** (getting-started/scalardb.properties) file holds the configuration for ScalarDB. You need to update `contact_points` and `password` with your Cosmos DB for NoSQL URI and Cosmos DB for NoSQL key respectively, and `storage` with `cosmos`.
    
```properties
# The Cosmos DB for NoSQL URI
scalar.db.contact_points=<COSMOS_DB_FOR_NOSQL_URI>

# The Cosmos DB for NoSQL key
scalar.db.password=<COSMOS_DB_FOR_NOSQL_KEY>

# Cosmos DB for NoSQL storage implementation
scalar.db.storage=cosmos

# The database name for the table metadata. If not specified, the default name ("scalardb") is used
scalar.db.cosmos.table_metadata.database=
```
Note that you can use a primary key or a secondary key for `<COSMOS_DB_FOR_NOSQL_KEY>`.

Please follow [Getting Started with ScalarDB](getting-started-with-scalardb.md) to run the application.
