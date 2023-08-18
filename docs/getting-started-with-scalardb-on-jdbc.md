{% include end-of-support.html %}

# Getting Started with Scalar DB on JDBC databases

## Overview
This document briefly explains how you can get started with Scalar DB on JDBC databases with a simple electronic money application.

## Install prerequisites

Scalar DB is written in Java and uses a JDBC database as an underlining storage implementation, so the following software is required to run it.

* [Oracle JDK 8](https://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html) (OpenJDK 8) or higher
* A JDBC database instance. We officially support MySQL, PostgreSQL, and Oracle Database for now (we also have the SQL Server implementation though)
* Other libraries used from the above are automatically installed through gradle

From here, we assume Oracle JDK 8 and a JDBC database is properly installed in your local environment, and it is running in your localhost.

## Configure Scalar DB

The **scalardb.properties** (getting-started/scalardb.properties) file holds the configuration for Scalar DB. Basically, it describes the JDBC database installation that will be used.

```
# The JDBC URL
scalar.db.contact_points=jdbc:mysql://localhost:3306/

# Credential information to access the database
scalar.db.username=root
scalar.db.password=mysql

# Storage implementation. Either cassandra or cosmos or dynamo or jdbc can be set. Default storage is cassandra.
scalar.db.storage=jdbc

# The minimum number of idle connections in the connection pool. The default is 5
#scalar.db.jdbc.connection_pool.min_idle=5

# The maximum number of connections that can remain idle in the connection pool. The default is 10
#scalar.db.jdbc.connection_pool.max_idle=10

# The maximum total number of idle and borrowed connections that can be active at the same time. Use a negative value for no limit. The default is 25
#scalar.db.jdbc.connection_pool.max_total=25

# Setting true to this property enables prepared statement pooling. The default is false
#scalar.db.jdbc.prepared_statements_pool.enabled=false

# The maximum number of open statements that can be allocated from the statement pool at the same time, or negative for no limit. The default is -1
#scalar.db.jdbc.prepared_statements_pool.max_open=-1
```

Please follow [Getting Started with Scalar DB](getting-started-with-scalardb.md) to run the application.
