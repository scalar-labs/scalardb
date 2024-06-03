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
A namespace is a collection of tables analogous to an SQL namespace or database. Typically, each application creates all its tables in one namespace.

#### Table
A table is a collection of data. A namespace most often contains one or more tables, each identified by a name. Each table includes a set of partitions.

#### Partition
A partition is a collection of records and a unit of (logical) distribution to nodes. Therefore, records within the same partition are placed in the same logical location.

#### Record / Row
A record or row is a set of columns that is uniquely identifiable among all of the other records.

#### Column
A column is a fundamental data element and does not need to be broken down any further. Each record is composed of one or more columns. Each column has a data type. For details about the data type, refer to [Data-type mapping between ScalarDB and other databases](https://scalardb.scalar-labs.com/docs/latest/schema-loader#data-type-mapping-between-scalardb-and-other-databases).

#### Index
An index is a copy of the records in a single table, sorted by a column. ScalarDB queries use indexes to find data more efficiently in a table, given the values of a particular column. 


### How to locate records
This section discusses how to locate records from a table.

#### Primary key
A primary key uniquely identifies each record; no two records can have the same primary key. Therefore, you can locate a record by specifying a primary key. A primary key comprises a set of partition keys and clustering keys.

#### Partition key
A partition key uniquely identifies a partition. A partition key comprises a set of columns, which are called partition key columns. When you specify only a partition key, you can get a set of records that belong to the partition.

#### Clustering key
A clustering key uniquely identifies a record within a partition. It comprises a set of columns called clustering key columns. When you want to specify a clustering key, you should specify a partition key for efficient lookups. When you specify a clustering key without a partition key,
you end up scanning all the partitions if you don’t have proper indexes. Scanning all the partitions is very time-consuming, so please do it only when you know what you are doing.

Records within a partition are assumed to be sorted by clustering key columns. Therefore, you can specify a part of clustering key columns in the defined order to narrow down the results to be returned. 

#### Index key
An index key identifies records by looking up the key in indexes.

## How to design your database schemas
You can design your database schemas similarly to the relational model, but there are a few best practices to follow.

### Care about data distribution
It would be best if you tried to balance loads to partitions by properly selecting partition and clustering keys. For example, in a banking application, if you choose an account ID as a partition key, you can perform any account operations for a specific account within the partition to which the account belongs. So, if you operate on different account IDs, you access different partitions. On the other hand, if you choose a branch ID as a partition key and an account ID as a clustering key, all the accesses to a branch’s account IDs go to the same partition. In addition, you should choose a high cardinality column as a partition key because creating a small number of large partitions causes an imbalance in loads and data sizes.

### Try to read a single partition as much as possible
Because of the data model characteristics, single partition lookup is most efficient. If you need to issue a scan or select request that requires multi-partition lookups or scans ([with cross-partition scan enabled](https://scalardb.scalar-labs.com/docs/latest/configurations/#cross-partition-scan-configurations)), make sure you know what you are doing and think about updating the schemas if possible. For example, in a banking application, if you choose email as a partition key and an account ID as a clustering key and issue a query that specifies an account ID, the query will span to all the partitions because it cannot identify the corresponding partition efficiently. In such a case, you should always look up the table with an account ID.

:::note

If you read multiple partitions on a relational database with proper indexes, your query might be efficient because the query is pushed down to the database. 

:::