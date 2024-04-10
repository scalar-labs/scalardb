# ScalarDB Overview

## What is ScalarDB?

ScalarDB is an HTAP engine for diverse databases. It achieves ACID transactions and real-time analytics across databases to simplify the complexity of managing multiple databases. Existing solutions are designed to run only transactions or analytical queries across heterogeneous databases or both on homogeneous databases.

ScalarDB is a versatile solution that currently supports a wide range of databases. This includes major relational databases such as Oracle Database, PostgreSQL, MariaDB, MySQL, Microsoft SQL Server, SQLite, and their compatible databases (e.g., Amazon Aurora, Google AlloyDB, TiDB, YugabyteDB), as well as NoSQL databases like Amazon DynamoDB, Azure Cosmos DB, and Apache Cassandra.
For details on which databases ScalarDB supports, refer to the [Supported Databases](docs/scalardb-supported-databases.md).

## ScalarDB use cases

ScalarDB can be used in various ways.
Here are the three primary use cases of ScalarDB.

### Manages siloed databases simply
Many enterprises comprise several organizations, departments, and business units to support agile business operations, which leads to siloed information systems; different organizations likely manage different applications with different databases. Managing such siloed databases is challenging because applications must communicate each database separately and properly deal with the differences between databases. ScalarDB simplifies the management of siloed databases with a unified interface, enabling users to treat the databases as if there were a single database.

### Manages consistency between multiple database
Modern architectures, like a microservice architecture, encourage a system to separate a service and its database into smaller subsets to increase system modularity and development efficiency. However, managing disparate databases, especially of different kinds, is challenging because applications must ensure the correct states (or in other words, consistencies) of those databases.

ScalarDB simplifies managing such disparate databases with a correctness guarantee (or in other words, ACID with strict serializability), enabling you to focus on application development without worrying about guaranteeing consistency between databases.

### Reducing database migration hurdles

Applications tend to be locked into a database by, for example, using database-specific capabilities. Such database lock-in discourages upgrading or changing the database because it requires rewriting the application. ScalarDB provides a unified interface over diverse databases; thus, once an application is written with ScalarDB, the application becomes portable, which helps achieve database migration without application rewrite.