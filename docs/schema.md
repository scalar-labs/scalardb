## Database schema in Scalar DB

Scalar DB has its own data model and schema and is mapped to a implementation-specific data model and schema.
Also, it stores internal metadata for managing transaction logs and statuses.
This document briefly explains how the data model and schema is mapped between Scalar DB and other implementations, and what are the internal metadata.

## Scalar DB and Cassandra

The data model in Scalar DB is quite similar to the data model in Cassandra, except that in Scalar DB it is more like a simple key-value data model and it does not support secondary indexes.
The primary key in Scalar DB is composed of one or more partition keys and zero or more clustering keys. Similarly, the primary key in Cassandra is composed of one or more partition keys and zero or more clustering columns.


Data types supported in Scalar DB are a little more restricted than in Cassandra.
Here are the supported data types and their mapping to Cassandra data types.

|Scalar DB  |Cassandra  |
|---|---|
|BOOLEAN  |boolean  |
|INT  |int  |
|BIGINT  |bigint  |
|FLOAT  |float  |
|DOUBLE  |double  |
|TEXT  |text  |
|BLOB  |blob  |

## Internal metadata in Scalar DB

Scalar DB executes transactions in a client-coordinated manner by storing and retrieving metadata stored along with the actual records.
Thus, along with any required values by the application, additional values for the metadata need to be defined in the schema.

Internal metadata schema creation managed by [scalar-schema](../tools/scalar-schema) tools.
 
## Schema generator and loader

It is a little hard for application developers to care for the schema mapping and metadata for transactions,
so we offer tools for generating and loading schema without much knowledge about imlementation schema and Scalar DB internal metadata.
Please see [this](schema-loader.md) for more details.
