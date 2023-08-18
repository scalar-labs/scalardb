{% include end-of-support.html %}

# Getting Started with Scalar DB on Cassandra
    
## Overview
This document briefly explains how you can get started with Scalar DB on Cassandra with a simple electronic money application.

## Install prerequisites

Scalar DB is written in Java and uses Cassandra as an underlying storage implementation, so the following software is required to run it.

* [Oracle JDK 8](https://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html) (OpenJDK 8) or higher
* [Casssandra](http://cassandra.apache.org/) 3.11.x (the current stable version as of writing)
    * Take a look at [this document](http://cassandra.apache.org/download/) for how to set up Cassandra.
    * Change `commitlog_sync` from `periodic` to `batch` in `cassandra.yaml` not to lose data when quorum of replica nodes go down
* Other libraries used from the above are automatically installed through gradle

From here, we assume Oracle JDK 8 and Cassandra 3.11.x are properly installed in your local environment, and Cassandra is running in your localhost.

## Configure Scalar DB

The **scalardb.properties** (getting-started/scalardb.properties) file holds the configuration for Scalar DB. Basically, it describes the Cassandra installation that will be used.

```
# Comma separated contact points
scalar.db.contact_points=localhost

# Port number for all the contact points. Default port number for each database is used if empty.
scalar.db.contact_port=9042

# Credential information to access the database
scalar.db.username=cassandra
scalar.db.password=cassandra

# Storage implementation. Either cassandra or cosmos or dynamo or jdbc can be set. Default storage is cassandra.
#scalar.db.storage=cassandra
```

Please follow [Getting Started with Scalar DB](getting-started-with-scalardb.md) to run the application.
