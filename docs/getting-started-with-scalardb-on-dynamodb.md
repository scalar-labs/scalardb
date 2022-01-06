# Getting Started with Scalar DB on DynamoDB

## Overview
This document briefly explains how you can get started with Scalar DB on DynamoDB with a simple electronic money application.

## Install prerequisites

Scalar DB is written in Java. So the following software is required to run it.

* [Oracle JDK 8](https://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html) (OpenJDK 8) or higher
* Other libraries used from the above are automatically installed through gradle
        
From here, we assume Oracle JDK 8 is properly installed in your local environment.

## Configure Scalar DB
    
The **scalardb.properties** (getting-started/scalardb.properties) file holds the configuration for Scalar DB. You need to update `contact_points` with AWS region, `username` with your AWS access key ID, `password` with your AWS secret access key, and `storage` with `dynamo`.
```
# The AWS region
scalar.db.contact_points=<REGION>

# The AWS access key ID and secret access key
scalar.db.username=<ACCESS_KEY_ID>
scalar.db.password=<SECRET_ACCESS_KEY>

# DynamoDB storage implementation
scalar.db.storage=dynamo

# Override the DynamoDB endpoint to use a local instance instead of an AWS service
#scalar.db.dynamo.endpoint-override=

# The namespace name for the table metadata (used as a table prefix of the table metadata)
#scalar.db.dynamo.table_metadata.namespace=
```

Please follow [Getting Started with Scalar DB](getting-started-with-scalardb.md) to run the application.
