# ScalarDB Overview

This page describes what ScalarDB is and its primary use cases.

## What is ScalarDB?

ScalarDB is a cross-database HTAP engine. It achieves ACID transactions and real-time analytics across diverse databases to simplify the complexity of managing multiple databases. Existing solutions are designed to run only transactions or analytical queries across heterogeneous databases, or they run transactions and analytical queries only on homogeneous databases.

As a versatile solution, ScalarDB supports a range of databases, including:

- Relational databases that support JDBC, such as MariaDB, Microsoft SQL Server, MySQL, Oracle Database, PostgreSQL, SQLite, and their compatible databases, like Amazon Aurora, Google AlloyDB, TiDB, and YugabyteDB.
- NoSQL databases like Amazon DynamoDB, Apache Cassandra, and Azure Cosmos DB.

For details on which databases ScalarDB supports, refer to [Supported Databases](scalardb-supported-databases.md).

## ScalarDB use cases

ScalarDB can be used in various ways. Here are the three primary use cases of ScalarDB.

### Managing siloed databases easily
Many enterprises comprise several organizations, departments, and business units to support agile business operations, which often leads to siloed information systems; different organizations likely manage different applications with different databases. Managing such siloed databases is challenging because applications must communicate each database separately and properly deal with the differences between databases.

ScalarDB simplifies the management of siloed databases with a unified interface, enabling users to treat the databases as if they were a single database. For example, users can run (analytical) join queries over multiple databases without interacting with the databases respectively.

### Managing consistency between multiple database
Modern architectures, like a microservice architecture, encourage a system to separate a service and its database into smaller subsets to increase system modularity and development efficiency. However, managing disparate databases, especially of different kinds, is challenging because applications must ensure the correct states (or in other words, consistencies) of those databases.

ScalarDB simplifies managing such disparate databases with a correctness guarantee (or in other words, ACID with strict serializability), enabling you to focus on application development without worrying about guaranteeing consistency between databases.

### Reducing database migration hurdles

Applications tend to be locked into using a certain database because of the specific capabilities that the database provides. Such database lock-in discourages upgrading or changing the database because doing so often requires re-writing the application.

ScalarDB provides a unified interface for diverse databases. Thus, once an application is written using the ScalarDB interface, the application becomes portable, which helps to achieve seamless database migration without needing to re-write the application.
