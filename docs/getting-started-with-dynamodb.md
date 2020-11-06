# Getting Started with Scalar DB on Dynamo DB

## Overview
This document briefly explains how you can get started with Scalar DB on Dynamo DB with a simple electronic money application.

## Install prerequisites

Scalar DB is written in Java. So the following software is required to run it.

* [Oracle JDK 8](https://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html) (OpenJDK 8) or higher
* Other libraries used from the above are automatically installed through gradle
        
From here, we assume Oracle JDK 8 is properly installed in your local environment.

## Configure Scalar DB
    
The **scalardb.properties** (getting-started/scalardb.properties) file holds the configuration for Scalar DB. You need to update `contact_points` with aws region, `username` with your aws access key id, `password` with your aws access secret key and `storage` with `dynamo`.
    
```
# Comma separated contact points
scalar.db.contact_points=<REGION>

# Port number for all the contact points. Default port number for each database is used if empty.
#scalar.db.contact_port=

# Credential information to access the database
scalar.db.username=<AWS_ACCESS_KEY_ID>
scalar.db.password=<AWS_ACCESS_SECRET_KEY>

# Storage implementation. Either cassandra or cosmos or dynamo can be set. Default storage is cassandra.
scalar.db.storage=dynamo
```

Please follow [Getting Started with Scalar DB](getting-started-with-scalardb.md) to run the application.
