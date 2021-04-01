## Database schema in Scalar DB

Scalar DB has its own data model and schema, that maps to the implementation specific data model and schema.
Also, it stores internal metadata for managing transaction logs and statuses.
This document briefly describes how the data model and schema are mapped between Scalar DB and other implementations.

## Scalar DB and Cassandra

The data model in Scalar DB is quite similar to the data model in Cassandra, except that in Scalar DB it is more like a simple key-value data model and it does not support secondary indexes.
The primary key in Scalar DB is composed of one or more partition keys and zero or more clustering keys. Similarly, the primary key in Cassandra is composed of one or more partition keys and zero or more clustering columns.


Data types supported in Scalar DB are a little more restricted than in Cassandra.
Here are the supported data types and their mapping to Cassandra data types.

|Scalar DB  |Cassandra  |Cosmos DB  |DynamoDB  |MySQL  |PostgreSQL |Oracle |SQL Server |
|---|---|---|---|---|---|---|---|
|BOOLEAN  |boolean  |boolean   |boolean     |boolean    |boolean    |number(1) |bit  |
|INT  |int  |int    |ScalarAttributeType/N    |int    |int    |int    |int    |int    |
|BIGINT  |bigint    |bigint |ScalarAttributeType/N |bigint |bigint |number(19) |bigint | 
|FLOAT  |float  |float  |ScalarAttributeType/N  |float  |float  |binary_float   |float(24)  |
|DOUBLE  |double    |double |ScalarAttributeType/N |double   |double precision   |binary_double  |float  |
|TEXT  |text    |text   |ScalarAttributeType/S   |longtext |text   |varchar(4000)  |varchar(8000)  |
|BLOB  |blob    |blob   |ScalarAttributeType/B   |longblob  |bytea  |blob   |varbinary(8000)    |

## Internal metadata in Scalar DB

Scalar DB executes transactions in a client-coordinated manner by storing and retrieving metadata stored along with the actual records.
Thus, along with any required values by the application, additional values for the metadata need to be defined in the schema.

Internal metadata schema creation managed by [scalar-schema](../tools/scalar-schema/README.md) tools.
 
## Schema generator and loader

It is a little hard for application developers to care for the schema mapping and metadata for transactions,
so we offer tools for generating and loading schema without much knowledge about imlementation schema and Scalar DB internal metadata.
Please see [this](../tools/scalar-schema/README.md) for more details.
