# Getting Started with ScalarDB on DynamoDB

## Overview
This document briefly explains how you can get started with ScalarDB on DynamoDB with a simple electronic money application.

## Install prerequisites

ScalarDB is written in Java. So the following software is required to run it.

* [Oracle JDK 8](https://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html) (or OpenJDK 8) or higher
* Other libraries used from the above are automatically installed through gradle
        
From here, we assume Oracle JDK 8 is properly installed in your local environment.

## Configure ScalarDB
    
The **scalardb.properties** (getting-started/scalardb.properties) file holds the configuration for ScalarDB. You need to update `contact_points` with AWS region, `username` with your AWS access key ID, `password` with your AWS secret access key, and `storage` with `dynamo`.

```properties
# DynamoDB storage implementation is used for Consensus Commit.
scalar.db.storage=dynamo

# The AWS region.
scalar.db.contact_points=<REGION>

# The AWS access key ID and secret access key.
scalar.db.username=<ACCESS_KEY_ID>
scalar.db.password=<SECRET_ACCESS_KEY>
```

For details about configurations, see [ScalarDB Configurations](configurations.md).

To run the application, follow the instructions in [Getting Started with ScalarDB](getting-started-with-scalardb.md).
