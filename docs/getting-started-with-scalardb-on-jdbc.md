# Getting Started with ScalarDB on JDBC databases

## Overview
This document briefly explains how you can get started with ScalarDB on JDBC databases with a simple electronic money application.

## Install prerequisites

ScalarDB is written in Java and uses a JDBC database as an underlying storage implementation, so the following software is required to run it.

* [Oracle JDK 8](https://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html) (or OpenJDK 8) or higher
* A JDBC database instance. Currently, MySQL, PostgreSQL, Oracle Database, SQL Server, Amazon Aurora, and SQLite are officially supported
* Other libraries used from the above are automatically installed through gradle

From here, we assume Oracle JDK 8 and a JDBC database is properly installed in your local environment, and it is running in your localhost.

## Configure ScalarDB

The **scalardb.properties** (getting-started/scalardb.properties) file holds the configuration for ScalarDB. Basically, it describes the JDBC database installation that will be used.

```properties
# JDBC storage implementation is used for Consensus Commit.
scalar.db.storage=jdbc

# The JDBC URL.
scalar.db.contact_points=jdbc:mysql://localhost:3306/

# The username and password.
scalar.db.username=root
scalar.db.password=mysql
```

For details about configurations, see [ScalarDB Configurations](configurations.md).

To run the application, follow the instructions in [Getting Started with ScalarDB](getting-started-with-scalardb.md).
