# Getting Started with Scalar DB on DynamoDB

## Overview
This document briefly explains how you can get started with Scalar DB on DynamoDB with a simple electronic money application.

## Install prerequisites

Scalar DB is written in Java. So the following software is required to run it.

* [Oracle JDK 8](https://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html) (or OpenJDK 8)
* Other libraries used from the above are automatically installed through gradle
        
From here, we assume Oracle JDK 8 is properly installed in your local environment.

## Configure Scalar DB
    
The **scalardb.properties** (getting-started/scalardb.properties) file holds the configuration for Scalar DB. You need to update `contact_points` with AWS region, `username` with your AWS access key ID, `password` with your AWS secret access key, and `storage` with `dynamo`.

```properties
# The AWS region
scalar.db.contact_points=<REGION>

# The AWS access key ID and secret access key
scalar.db.username=<ACCESS_KEY_ID>
scalar.db.password=<SECRET_ACCESS_KEY>

# DynamoDB storage implementation
scalar.db.storage=dynamo

# Override the DynamoDB endpoint to use a local instance instead of an AWS service
scalar.db.dynamo.endpoint-override=

# The namespace name for the table metadata (used as a table prefix of the table metadata)
scalar.db.dynamo.table_metadata.namespace=

# Set a prefix for the user namespaces and metadata namespace names. Since AWS requires to have unique 
# tables names in a single AWS region, this is useful if you want to use multiple Scalar DB environments 
# (development, production, etc.) in a single AWS region.
scalar.db.dynamo.namespace.prefix=
```

Please follow [Getting Started with Scalar DB](getting-started-with-scalardb.md) to run the application.
