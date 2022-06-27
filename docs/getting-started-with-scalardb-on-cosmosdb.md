# Getting Started with Scalar DB on Cosmos DB

## Overview
This document briefly explains how you can get started with Scalar DB on Cosmos DB with a simple electronic money application.

## Install prerequisites

Scalar DB is written in Java. So the following software is required to run it.

* [Oracle JDK 8](https://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html) (or OpenJDK 8)
* Other libraries used from the above are automatically installed through gradle

## Cosmos DB setup
You also need to set up a Cosmos DB account to get started with Scalar DB on Cosmos DB.

This section explains how to set up [Azure Cosmos DB](https://docs.microsoft.com/en-us/azure/cosmos-db/introduction) with Azure portal.
1. Select **Azure Cosmos DB** service from the services on Azure portal.
2. Select **Add**
3. On the **Create Azure Cosmos DB Account** page, enter the basic settings for the new **Azure Cosmos DB** account.
    * Create new or choose the existing **Resource Group**
    * Enter the Cosmos DB **Account Name**
    * Choose **API** as `Core (SQL)`
    * Choose **Location**
    * Select **Review + create**. You can skip the **Network** and **Tags** sections.
    * Review the account settings, and then select **Create**.
    *  Wait some time for **Azure Cosmos DB** account creation.
 4. Select **Go to resource** to go to the Azure Cosmos DB account page.
 5. Select **Default consistency** from the left navigation on your Azure Cosmos DB account page.
    * Change `Consistency Level` from `SESSION` to `STRONG`.
    * Select **Save**
        
From here, we assume Oracle JDK 8 is properly installed in your local environment and the Azure Cosmos DB account is properly configured in Azure.

## Configure Scalar DB
    
The **scalardb.properties** (getting-started/scalardb.properties) file holds the configuration for Scalar DB. You need to update `contact_points` and `password` with your Cosmos DB URI and Cosmos DB key respectively, and `storage` with `cosmos`.
    
```properties
# The Cosmos DB URI
scalar.db.contact_points=<COSMOS_DB_URI>

# The Cosmos DB key
scalar.db.password=<COSMOS_DB_KEY>

# Cosmos DB storage implementation
scalar.db.storage=cosmos

# The database name for the table metadata. If not specified, the default name ("scalardb") is used
scalar.db.cosmos.table_metadata.database=
```
Note that you can use a primary key or a secondary key for `<COSMOS_DB_KEY>`.

Please follow [Getting Started with Scalar DB](getting-started-with-scalardb.md) to run the application.
