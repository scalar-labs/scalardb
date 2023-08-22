{% include end-of-support.html %}

# Database schema in Scalar DB

Scalar DB has its own data model and schema, that maps to the implementation specific data model and schema.
Also, it stores internal metadata for managing transaction logs and statuses.
This document briefly explains the Scalar DB data model, how data types are mapped between Scalar DB and the other database implementations, internal metadata, and how Scalar DB database schema can be defined.

## Data model

The data model of Scalar DB is a multi-dimensional map based on the key-value data model. A logical record is composed of partition-key, clustering-key and a set of values. The value is uniquely mapped by a primary key composed of partition-key, clustering-key and value-name as described in the following scheme.

(partition-key, clustering-key, value-name) -> value-content

For each database implementation, there is an adapter that converts the database specific data model into the Scalar DB data model, thus; users usually don't need to care about how it is converted and can design database schema on the basis of the Scalar DB data model. 

## Data type mapping between Scalar DB and the other databases

Here are the supported data types in Scalar DB and their mapping to the data types of other databases.

| Scalar DB | Cassandra | Cosmos DB      | DynamoDB | MySQL    | PostgreSQL       | Oracle         | SQL Server      |
| --------- | --------- | -------------- | ---------| -------- | ---------------- | -------------- | --------------- |
| BOOLEAN   | boolean   | boolean (JSON) | BOOL     | boolean  | boolean          | number(1)      | bit             |
| INT       | int       | number (JSON)  | N        | int      | int              | int            | int             |
| BIGINT    | bigint    | number (JSON)  | N        | bigint   | bigint           | number(19)     | bigint          |
| FLOAT     | float     | number (JSON)  | N        | double   | float            | binary_float   | float(24)       |
| DOUBLE    | double    | number (JSON)  | N        | double   | double precision | binary_double  | float           |
| TEXT      | text      | string (JSON)  | S        | longtext | text             | varchar2(4000) | varchar(8000)   |
| BLOB      | blob      | string (JSON)  | B        | longblob | bytea            | blob           | varbinary(8000) |

## Internal metadata

Scalar DB manages metadata (e.g., transaction ID, record version, transaction status) stored along with the actual records to handle transactions properly.
Thus, along with any required values by the application, additional values for the metadata need to be defined in the schema.

## Schema creation

It is a little hard for application developers to care for the schema mapping and metadata for transactions,
so we offer a tool called [Schema Loader](https://github.com/scalar-labs/scalardb/tree/master/schema-loader/README.md) for creating schema without much knowledge about those.
