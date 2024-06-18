# Model your data
Data modeling (i.e., design your database schemas) is the process of conceptualizing and visualizing how data will be stored and used by identifying the patterns used to access data and the types of queries to be performed within business operations. 

This page first explains the ScalarDB data model and then describes how to design your database schemas based on the data model.

## ScalarDB data model
ScalarDB’s data model is an extended key-value model inspired by the Bigtable data model. It is similar to the relational model but differs in several ways, as described below.

The following diagram shows an example of ScalarDB tables, each a collection of records. This section first explains what objects, such as tables and records, ScalarDB defines and then describes how to locate records.

![ScalarDB data model.](images/scalardb_data_model.png)

### Objects in ScalarDB
The ScalarDB data model has several objects.

#### Namespace
A namespace is a collection of tables analogous to an SQL namespace or database.

#### Table
A table is a collection of partitions. A namespace most often contains one or more tables, each identified by a name.

#### Partition
A partition is a collection of records and a unit of (logical) distribution to nodes. Therefore, records within the same partition are placed in the same logical location.
ScalarDB assumes multiple partitions are distributed by hashing.

#### Record / Row
A record or row is a set of columns that is uniquely identifiable among all of the other records.

#### Column
A column is a fundamental data element and does not need to be broken down any further. Each record is composed of one or more columns. Each column has a data type. For details about the data type, refer to [Data-type mapping between ScalarDB and other databases](https://scalardb.scalar-labs.com/docs/latest/schema-loader#data-type-mapping-between-scalardb-and-other-databases).

#### Secondary index
A secondary index is a sorted copy of a column of a single base table. Each entry of an index is linked to a corresponding table partition. ScalarDB currently doesn't support multi-column indexes so that it can create indexes with one column.

### How to locate records
This section discusses how to locate records from a table.

#### Primary key
A primary key uniquely identifies each record; no two records can have the same primary key. Therefore, you can locate a record by specifying a primary key. A primary key comprises a partition key and, optionally, a clustering key.

#### Partition key
A partition key uniquely identifies a partition. A partition key comprises a set of columns, which are called partition key columns. When you specify only a partition key, you can get a set of records that belong to the partition.

#### Clustering key
A clustering key uniquely identifies a record within a partition. It comprises a set of columns called clustering key columns. When you want to specify a clustering key, you should specify a partition key for efficient lookups. When you specify a clustering key without a partition key,
you end up scanning all the partitions if you don’t have proper indexes. Scanning all the partitions is very time-consuming, so please do it only when you know what you are doing.

Records within a partition are assumed to be sorted by clustering key columns, specified as a clustering order. Therefore, you can specify a part of clustering key columns in the defined order to narrow down the results to be returned. 

#### Index key
An index key identifies records by looking up the key in indexes. An index key lookup spans all the partitions, so it is not necessarily very efficient, especially if the selectivity of a lookup is not low.

## How to design your database schemas
You can design your database schemas similarly to the relational model, but there is a basic principle and are a few best practices to follow.

### Query-driven data modeling
In relational databases, data is organized in normalized tables with foreign keys used to reference related data in other tables. The queries that the application will make are structured by the tables, and related data are queried as table joins.

In ScalarDB, although it supports join operations, data modeling should be more query-driven, like NoSQL databases. The data access patterns and application queries should determine the structure and organization of tables.
### Best practices

#### Care about data distribution
It would be best if you tried to balance loads to partitions by properly selecting partition and clustering keys. For example, in a banking application, if you choose an account ID as a partition key, you can perform any account operations for a specific account within the partition to which the account belongs. So, if you operate on different account IDs, you access different partitions. On the other hand, if you choose a branch ID as a partition key and an account ID as a clustering key, all the accesses to a branch’s account IDs go to the same partition, causing an imbalance in loads and data sizes. In addition, you should choose a high cardinality column as a partition key because creating a small number of large partitions also causes an imbalance in loads and data sizes.

#### Try to read a single partition as much as possible
Because of the data model characteristics, single partition lookup is most efficient. If you need to issue a scan or select request that requires multi-partition lookups or scans ([with cross-partition scan enabled](https://scalardb.scalar-labs.com/docs/latest/configurations/#cross-partition-scan-configurations)), make sure you know what you are doing and think about updating the schemas if possible. For example, in a banking application, if you choose email as a partition key and an account ID as a clustering key and issue a query that specifies an account ID, the query will span all the partitions because it cannot identify the corresponding partition efficiently. In such a case, you should always look up the table with an account ID.

:::note

If you read multiple partitions on a relational database with proper indexes, your query might be efficient because the query is pushed down to the database. 

:::

#### Avoid using secondary indexes as much as possible

Similarly to the above, if you need to issue a scan or select request that uses a secondary index, the request will span all the partitions of a table. Therefore, you should avoid using secondary indexes as much as possible. If you need to use it, use it through a low-selectivity query, which looks up a small portion.

As an alternative to secondary indexes, you can create another table that works as a (clustered) index of a base table. For example, assume there is a table with 3 columns, `table1(A, B, C)`, with a primary key `A`. Then, you can create a table like `index-table1(C, A, B)` with `C` as a primary key so that you can look up a single partition by specifying a value for `C`.
This approach could speed up read queries but create more load to write queries because it writes to two tables with ScalarDB transactions.

:::note

We plan to have a table-based secondary index feature in the future.

:::

#### Care about data is (assumed to be) distributed by hashing

In the current ScalarDB data model, data is assumed to be distributed by hashing, so you can't perform range queries efficiently without a partition key.
If you want to issue range queries efficiently, you need to do it within a partition.
However, it's important to note that this approach requires you to specify a partition key. This can pose scalability issues as the range queries always go to the same partition, potentially overloading it.
The limitation is not specific to ScalarDB but to databases where data is distributed by hashing for scalability.

:::note

If you run ScalarDB on a relational database with proper indexes, your query might be efficient because the query is pushed down to the database. 

:::

