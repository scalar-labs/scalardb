# ScalarDB Schema Loader

ScalarDB has its own data model and schema, that maps to the implementation specific data model and schema.
Also, it stores internal metadata (e.g., transaction ID, record version, transaction status) for managing transaction logs and statuses when you use the Consensus Commit transaction manager.
It is a little hard for application developers to manage the schema mapping and metadata for transactions, so we offer a tool called ScalarDB Schema Loader for creating schema without requiring much knowledge about those.

There are two ways to specify general CLI options in Schema Loader:
  - Pass a ScalarDB configuration file and database/storage-specific options additionally.
  - Pass the options without a ScalarDB configuration (Deprecated).

Note that this tool supports only basic options to create/delete/repair/alter a table. If you want
to use the advanced features of a database, please alter your tables with a database specific tool after creating them with this tool.

# Usage

## Install

The release versions of `schema-loader` can be downloaded from [releases](https://github.com/scalar-labs/scalardb/releases) page of ScalarDB.

## Build

In case you want to build `schema-loader` from the source:
```console
$ ./gradlew schema-loader:shadowJar
```
- The built fat jar file is `schema-loader/build/libs/scalardb-schema-loader-<version>.jar`

## Docker

You can pull the docker image from [Scalar's container registry](https://github.com/orgs/scalar-labs/packages/container/package/scalardb-schema-loader).
```console
docker run --rm -v <your_local_schema_file_path>:<schema_file_path_in_docker> [-v <your_local_config_file_path>:<config_file_path_in_docker>] ghcr.io/scalar-labs/scalardb-schema-loader:<version> <command_arguments>
```
- Note that you can specify the same command arguments even if you use the fat jar or the container. The example commands in the next section are shown with a jar, but you can run the commands with the container in the same way by replacing `java -jar scalardb-schema-loader-<version>.jar` with `docker run --rm -v <your_local_schema_file_path>:<schema_file_path_in_docker> [-v <your_local_config_file_path>:<config_file_path_in_docker>] ghcr.io/scalar-labs/scalardb-schema-loader:<version>`.

You can also build the docker image as follows.
```console
$ ./gradlew schema-loader:docker
```

## Run

### Available commands

For using a config file:
```console
Usage: java -jar scalardb-schema-loader-<version>.jar [-D] [--coordinator]
       [--no-backup] [--no-scaling] -c=<configPath>
       [--compaction-strategy=<compactionStrategy>] [-f=<schemaFile>]
       [--replication-factor=<replicaFactor>]
       [--replication-strategy=<replicationStrategy>] [--ru=<ru>]
Create/Delete schemas in the storage defined in the config file
  -A, --alter         Alter tables : it will add new columns and create/delete
                        secondary index for existing tables. It compares the
                        provided table schema to the existing schema to decide
                        which columns need to be added and which indexes need
                        to be created or deleted
  -c, --config=<configPath>
                      Path to the config file of ScalarDB
      --compaction-strategy=<compactionStrategy>
                      The compaction strategy, must be LCS, STCS or TWCS
                        (supported in Cassandra)
      --coordinator   Create/delete/repair coordinator tables
  -D, --delete-all    Delete tables
  -f, --schema-file=<schemaFile>
                      Path to the schema json file
      --no-backup     Disable continuous backup (supported in DynamoDB)
      --no-scaling    Disable auto-scaling (supported in DynamoDB, Cosmos DB)
      --repair-all    Repair tables : it repairs the table metadata of
                               existing tables. When using Cosmos DB, it
                               additionally repairs stored procedure attached
                               to each table
      --replication-factor=<replicaFactor>
                      The replication factor (supported in Cassandra)
      --replication-strategy=<replicationStrategy>
                      The replication strategy, must be SimpleStrategy or
                        NetworkTopologyStrategy (supported in Cassandra)
      --ru=<ru>       Base resource unit (supported in DynamoDB, Cosmos DB)
```

For Cosmos DB for NoSQL (Deprecated. Please use the command using a config file instead):
```console
Usage: java -jar scalardb-schema-loader-<version>.jar --cosmos [-D]
       [--no-scaling] -f=<schemaFile> -h=<uri> -p=<key> [-r=<ru>]
Create/Delete Cosmos DB schemas
  -A, --alter         Alter tables : it will add new columns and create/delete
                        secondary index for existing tables. It compares the
                        provided table schema to the existing schema to decide
                        which columns need to be added and which indexes need
                        to be created or deleted
  -D, --delete-all       Delete tables
  -f, --schema-file=<schemaFile>
                         Path to the schema json file
  -h, --host=<uri>       Cosmos DB account URI
      --no-scaling       Disable auto-scaling for Cosmos DB
  -p, --password=<key>   Cosmos DB key
  -r, --ru=<ru>          Base resource unit
      --repair-all       Repair tables : it repairs the table metadata of
                           existing tables and repairs stored procedure
                           attached to each table
```

For DynamoDB (Deprecated. Please use the command using a config file instead):
```console
Usage: java -jar scalardb-schema-loader-<version>.jar --dynamo [-D]
       [--no-backup] [--no-scaling] [--endpoint-override=<endpointOverride>]
       -f=<schemaFile> -p=<awsSecKey> [-r=<ru>] --region=<awsRegion>
       -u=<awsKeyId>
Create/Delete DynamoDB schemas
  -A, --alter         Alter tables : it will add new columns and create/delete
                        secondary index for existing tables. It compares the
                        provided table schema to the existing schema to decide
                        which columns need to be added and which indexes need
                        to be created or deleted
  -D, --delete-all           Delete tables
      --endpoint-override=<endpointOverride>
                             Endpoint with which the DynamoDB SDK should
                               communicate
  -f, --schema-file=<schemaFile>
                             Path to the schema json file
      --no-backup            Disable continuous backup for DynamoDB
      --no-scaling           Disable auto-scaling for DynamoDB
  -p, --password=<awsSecKey> AWS access secret key
  -r, --ru=<ru>              Base resource unit
      --region=<awsRegion>   AWS region
      --repair-all           Repair tables : it repairs the table metadata of
                               existing tables
  -u, --user=<awsKeyId>      AWS access key ID
```

For Cassandra (Deprecated. Please use the command using a config file instead):
```console
Usage: java -jar scalardb-schema-loader-<version>.jar --cassandra [-D]
       [-c=<compactionStrategy>] -f=<schemaFile> -h=<hostIp>
       [-n=<replicationStrategy>] [-p=<password>] [-P=<port>]
       [-R=<replicationFactor>] [-u=<user>]
Create/Delete Cassandra schemas
  -A, --alter         Alter tables : it will add new columns and create/delete
                        secondary index for existing tables. It compares the
                        provided table schema to the existing schema to decide
                        which columns need to be added and which indexes need
                        to be created or deleted
  -c, --compaction-strategy=<compactionStrategy>
                        Cassandra compaction strategy, must be LCS, STCS or TWCS
  -D, --delete-all      Delete tables
  -f, --schema-file=<schemaFile>
                        Path to the schema json file
  -h, --host=<hostIp>   Cassandra host IP
  -n, --network-strategy=<replicationStrategy>
                        Cassandra network strategy, must be SimpleStrategy or
                          NetworkTopologyStrategy
  -p, --password=<password>
                        Cassandra password
  -P, --port=<port>     Cassandra Port
  -R, --replication-factor=<replicationFactor>
                        Cassandra replication factor
      --repair-all     Repair tables : it repairs the table metadata of
                         existing tables
  -u, --user=<user>     Cassandra user
```

For a JDBC database (Deprecated. Please use the command using a config file instead):
```console
Usage: java -jar scalardb-schema-loader-<version>.jar --jdbc [-D]
       -f=<schemaFile> -j=<url> -p=<password> -u=<user>
Create/Delete JDBC schemas
  -A, --alter         Alter tables : it will add new columns and create/delete
                        secondary index for existing tables. It compares the
                        provided table schema to the existing schema to decide
                        which columns need to be added and which indexes need
                        to be created or deleted
  -D, --delete-all       Delete tables
  -f, --schema-file=<schemaFile>
                         Path to the schema json file
  -j, --jdbc-url=<url>   JDBC URL
  -p, --password=<password>
                         JDBC password
      --repair-all       Repair tables : it repairs the table metadata of
                           existing tables
  -u, --user=<user>      JDBC user
```

### Create namespaces and tables

For using a config file (Sample config file can be found [here](https://github.com/scalar-labs/scalardb/blob/master/conf/database.properties)):
```console
$ java -jar scalardb-schema-loader-<version>.jar --config <PATH_TO_CONFIG_FILE> -f schema.json [--coordinator]
```
  - if `--coordinator` is specified, the coordinator tables will be created.

For using CLI arguments fully for configuration (Deprecated. Please use the command using a config file instead):
```console
# For Cosmos DB for NoSQL
$ java -jar scalardb-schema-loader-<version>.jar --cosmos -h <COSMOS_DB_FOR_NOSQL_ACCOUNT_URI> -p <COSMOS_DB_FOR_NOSQL_KEY> -f schema.json [-r BASE_RESOURCE_UNIT]
```
  - `<COSMOS_DB_FOR_NOSQL_KEY>` you can use a primary key or a secondary key.
  - `-r BASE_RESOURCE_UNIT` is an option. You can specify the RU of each database. The maximum RU in tables in the database will be set. If you don't specify RU of tables, the database RU will be set with this option. By default, it's 400.

```console
# For DynamoDB
$ java -jar scalardb-schema-loader-<version>.jar --dynamo -u <AWS_ACCESS_KEY_ID> -p <AWS_ACCESS_SECRET_KEY> --region <REGION> -f schema.json [-r BASE_RESOURCE_UNIT]
```
  - `<REGION>` should be a string to specify an AWS region like `ap-northeast-1`.
  - `-r` option is almost the same as Cosmos DB for NoSQL option. However, the unit means DynamoDB capacity unit. The read and write capacity units are set the same value.

```console
# For Cassandra
$ java -jar scalardb-schema-loader-<version>.jar --cassandra -h <CASSANDRA_IP> [-P <CASSANDRA_PORT>] [-u <CASSANDRA_USER>] [-p <CASSANDRA_PASSWORD>] -f schema.json [-n <NETWORK_STRATEGY>] [-R <REPLICATION_FACTOR>]
```
  - If `-P <CASSANDRA_PORT>` is not supplied, it defaults to `9042`.
  - If `-u <CASSANDRA_USER>` is not supplied, it defaults to `cassandra`.
  - If `-p <CASSANDRA_PASSWORD>` is not supplied, it defaults to `cassandra`.
  - `<NETWORK_STRATEGY>` should be `SimpleStrategy` or `NetworkTopologyStrategy`

```console
# For a JDBC database
$ java -jar scalardb-schema-loader-<version>.jar --jdbc -j <JDBC URL> -u <USER> -p <PASSWORD> -f schema.json
```

### Alter tables

This command will add new columns and create/delete secondary index for existing tables. It compares
the provided table schema to the existing schema to decide which columns need to be added and which
indexes need to be created or deleted.

For using config file (Sample config file can be found [here](https://github.com/scalar-labs/scalardb/blob/master/conf/database.properties)):

```console
$ java -jar scalardb-schema-loader-<version>.jar --config <PATH_TO_CONFIG_FILE> -f schema.json --alter
```

For using CLI arguments fully for configuration (Deprecated. Please use the command using a config
file instead):

```console
# For Cosmos DB for NoSQL
$ java -jar scalardb-schema-loader-<version>.jar --cosmos -h <COSMOS_DB_FOR_NOSQL_ACCOUNT_URI> -p <COSMOS_DB_FOR_NOSQL_KEY> -f schema.json --alter
```

```console
# For DynamoDB
$ java -jar scalardb-schema-loader-<version>.jar --dynamo -u <AWS_ACCESS_KEY_ID> -p <AWS_ACCESS_SECRET_KEY> --region <REGION> -f schema.json --alter
```

```console
# For Cassandra
$ java -jar scalardb-schema-loader-<version>.jar --cassandra -h <CASSANDRA_IP> [-P <CASSANDRA_PORT>] [-u <CASSANDRA_USER>] [-p <CASSANDRA_PASSWORD>] -f schema.json --alter
```

```console
# For a JDBC database
$ java -jar scalardb-schema-loader-<version>.jar --jdbc -j <JDBC URL> -u <USER> -p <PASSWORD> -f schema.json --alter
```

### Delete tables

For using config file (Sample config file can be found [here](https://github.com/scalar-labs/scalardb/blob/master/conf/database.properties)):
```console
$ java -jar scalardb-schema-loader-<version>.jar --config <PATH_TO_CONFIG_FILE> -f schema.json [--coordinator] -D 
```
  - if `--coordinator` is specified, the coordinator tables will be deleted.
  
For using CLI arguments fully for configuration (Deprecated. Please use the command using a config file instead):
```console
# For Cosmos DB for NoSQL
$ java -jar scalardb-schema-loader-<version>.jar --cosmos -h <COSMOS_DB_FOR_NOSQL_ACCOUNT_URI> -p <COSMOS_DB_FOR_NOSQL_KEY> -f schema.json -D
```

```console
# For DynamoDB
$ java -jar scalardb-schema-loader-<version>.jar --dynamo -u <AWS_ACCESS_KEY_ID> -p <AWS_ACCESS_SECRET_KEY> --region <REGION> -f schema.json -D
```

```console
# For Cassandra
$ java -jar scalardb-schema-loader-<version>.jar --cassandra -h <CASSANDRA_IP> [-P <CASSANDRA_PORT>] [-u <CASSANDRA_USER>] [-p <CASSANDRA_PASSWORD>] -f schema.json -D
```

```console
# For a JDBC database
$ java -jar scalardb-schema-loader-<version>.jar --jdbc -j <JDBC URL> -u <USER> -p <PASSWORD> -f schema.json -D
```

### Repair tables

This command will repair the table metadata of existing tables. When using Cosmos DB for NoSQL, it additionally repairs stored procedure attached to each table.

For using config file (Sample config file can be found [here](https://github.com/scalar-labs/scalardb/blob/master/conf/database.properties)):
```console
$ java -jar scalardb-schema-loader-<version>.jar --config <PATH_TO_CONFIG_FILE> -f schema.json [--coordinator] --repair-all 
```
- if `--coordinator` is specified, the coordinator tables will be repaired as well.

For using CLI arguments fully for configuration (Deprecated. Please use the command using a config file instead):
```console
# For Cosmos DB for NoSQL
$ java -jar scalardb-schema-loader-<version>.jar --cosmos -h <COSMOS_DB_FOR_NOSQL_ACCOUNT_URI> -p <COSMOS_DB_FOR_NOSQL_KEY> -f schema.json --repair-all
```

```console
# For DynamoDB
$ java -jar scalardb-schema-loader-<version>.jar --dynamo -u <AWS_ACCESS_KEY_ID> -p <AWS_ACCESS_SECRET_KEY> --region <REGION> [--no-backup] -f schema.json --repair-all
```

```console
# For Cassandra
$ java -jar scalardb-schema-loader-<version>.jar --cassandra -h <CASSANDRA_IP> [-P <CASSANDRA_PORT>] [-u <CASSANDRA_USER>] [-p <CASSANDRA_PASSWORD>] -f schema.json --repair-all
```

```console
# For a JDBC database
$ java -jar scalardb-schema-loader-<version>.jar --jdbc -j <JDBC URL> -u <USER> -p <PASSWORD> -f schema.json --repair-all
```

### Sample schema file

The sample schema is as follows (Sample schema file can be found [here](https://github.com/scalar-labs/scalardb/blob/master/schema-loader/sample/schema_sample.json)):

```json
{
  "sample_db.sample_table": {
    "transaction": false,
    "partition-key": [
      "c1"
    ],
    "clustering-key": [
      "c4 ASC",
      "c6 DESC"
    ],
    "columns": {
      "c1": "INT",
      "c2": "TEXT",
      "c3": "BLOB",
      "c4": "INT",
      "c5": "BOOLEAN",
      "c6": "INT"
    },
    "secondary-index": [
      "c2",
      "c4"
    ]
  },

  "sample_db.sample_table1": {
    "transaction": true,
    "partition-key": [
      "c1"
    ],
    "clustering-key": [
      "c4"
    ],
    "columns": {
      "c1": "INT",
      "c2": "TEXT",
      "c3": "INT",
      "c4": "INT",
      "c5": "BOOLEAN"
    }
  },

  "sample_db.sample_table2": {
    "transaction": false,
    "partition-key": [
      "c1"
    ],
    "clustering-key": [
      "c4",
      "c3"
    ],
    "columns": {
      "c1": "INT",
      "c2": "TEXT",
      "c3": "INT",
      "c4": "INT",
      "c5": "BOOLEAN"
    }
  }
}
```

The schema has table definitions that include `columns`, `partition-key`, `clustering-key`, `secondary-index`,  and `transaction` fields.
The `columns` field defines columns of the table and their data types.
The `partition-key` field defines which columns the partition key is composed of, and `clustering-key` defines which columns the clustering key is composed of.
The `secondary-index` field defines which columns are indexed.
The `transaction` field indicates whether the table is for transactions or not.
If you set the `transaction` field to `true` or don't specify the `transaction` field, this tool creates a table with transaction metadata if needed.
If not, it creates a table without any transaction metadata (that is, for a table with [Storage API](storage-abstraction.md)).

You can also specify database/storage-specific options in the table definition as follows:
```json
{
  "sample_db.sample_table3": {
    "partition-key": [
      "c1"
    ],
    "columns": {
      "c1": "INT",
      "c2": "TEXT",
      "c3": "BLOB"
    },
    "compaction-strategy": "LCS",
    "ru": 5000
  }
}
```

The database/storage-specific options you can specify are as follows:

For Cassandra:
- `compaction-strategy`, a compaction strategy. It should be `STCS` (SizeTieredCompaction), `LCS` (LeveledCompactionStrategy) or `TWCS` (TimeWindowCompactionStrategy).

For DynamoDB and Cosmos DB for NoSQL:
- `ru`, a request unit. Please see [RU](#ru) for the details.

## Scaling Performance

### RU

You can scale the throughput of Cosmos DB for NoSQL and DynamoDB by specifying `--ru` option (which applies to all the tables) or `ru` parameter for each table. The default values are `400` for Cosmos DB for NoSQL and `10` for DynamoDB respectively, which are set without `--ru` option.

Note that the schema loader abstracts [Request Unit](https://docs.microsoft.com/azure/cosmos-db/request-units) of Cosmos DB for NoSQL and [Capacity Unit](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadWriteCapacityMode.html#HowItWorks.ProvisionedThroughput.Manual) of DynamoDB with `RU`.
So, please set an appropriate value depending on the database implementations. Please also note that the schema loader sets the same value to both Read Capacity Unit and Write Capacity Unit for DynamoDB.

### Auto-scaling

By default, the schema loader enables auto-scaling of RU for all tables: RU is scaled in or out between 10% and 100% of a specified RU depending on a workload. For example, if you specify `-r 10000`, RU of each table is scaled in or out between 1000 and 10000. Note that auto-scaling of Cosmos DB for NoSQL is enabled only when you set more than or equal to 4000 RU.

## Data type mapping between ScalarDB and the other databases

Here are the supported data types in ScalarDB and their mapping to the data types of other databases.

| ScalarDB  | Cassandra | Cosmos DB for NoSQL | DynamoDB | MySQL    | PostgreSQL       | Oracle         | SQL Server      | SQLite  |
|-----------|-----------|---------------------|----------|----------|------------------|----------------|-----------------|---------|
| BOOLEAN   | boolean   | boolean (JSON)      | BOOL     | boolean  | boolean          | number(1)      | bit             | boolean |
| INT       | int       | number (JSON)       | N        | int      | int              | int            | int             | int     |
| BIGINT    | bigint    | number (JSON)       | N        | bigint   | bigint           | number(19)     | bigint          | bigint  |
| FLOAT     | float     | number (JSON)       | N        | double   | float            | binary_float   | float(24)       | float   |
| DOUBLE    | double    | number (JSON)       | N        | double   | double precision | binary_double  | float           | double  |
| TEXT      | text      | string (JSON)       | S        | longtext | text             | varchar2(4000) | varchar(8000)   | text    |
| BLOB      | blob      | string (JSON)       | B        | longblob | bytea            | RAW(2000)      | varbinary(8000) | blob    |

However, the following types in JDBC databases are converted differently when they are used as a primary key or a secondary index key due to the limitations of RDB data types.

| ScalarDB | MySQL         | PostgreSQL        | Oracle       |
|----------|---------------|-------------------|--------------|
| TEXT     | VARCHAR(64)   | VARCHAR(10485760) | VARCHAR2(64) |
| BLOB     | VARBINARY(64) |                   | RAW(64)      |

The value range of `BIGINT` in ScalarDB is from -2^53 to 2^53 regardless of the underlying database.

If this data type mapping doesn't match your application, please alter the tables to change the data types after creating them with this tool.

## Internal metadata for Consensus Commit

The Consensus Commit transaction manager manages metadata (e.g., transaction ID, record version, transaction status) stored along with the actual records to handle transactions properly.
Thus, along with any required columns by the application, additional columns for the metadata need to be defined in the schema.
Additionaly, this tool creates a table with the metadata when you use the Consensus Commit transaction manager.

## Using Schema Loader in your program
You can check the version of `schema-loader` from [maven central repository](https://mvnrepository.com/artifact/com.scalar-labs/scalardb-schema-loader).
For example in Gradle, you can add the following dependency to your build.gradle. Please replace the `<version>` with the version you want to use.
```gradle
dependencies {
    implementation 'com.scalar-labs:scalardb-schema-loader:<version>'
}
```

### Create, alter, repair and delete

You can create, alter, delete and repair tables that are defined in the schema using SchemaLoader by
simply passing ScalarDB configuration file, schema, and additional options if needed as shown
below.

```java
public class SchemaLoaderSample {
  public static int main(String... args) throws SchemaLoaderException {
    Path configFilePath = Paths.get("database.properties");
    // "sample_schema.json" and "altered_sample_schema.json" can be found in the "/sample" directory
    Path schemaFilePath = Paths.get("sample_schema.json");
    Path alteredSchemaFilePath = Paths.get("altered_sample_schema.json");
    boolean createCoordinatorTables = true; // whether to create the coordinator tables or not
    boolean deleteCoordinatorTables = true; // whether to delete the coordinator tables or not
    boolean repairCoordinatorTables = true; // whether to repair the coordinator tables or not

    Map<String, String> tableCreationOptions = new HashMap<>();

    tableCreationOptions.put(
        CassandraAdmin.REPLICATION_STRATEGY, ReplicationStrategy.SIMPLE_STRATEGY.toString());
    tableCreationOptions.put(CassandraAdmin.COMPACTION_STRATEGY, CompactionStrategy.LCS.toString());
    tableCreationOptions.put(CassandraAdmin.REPLICATION_FACTOR, "1");

    tableCreationOptions.put(DynamoAdmin.REQUEST_UNIT, "1");
    tableCreationOptions.put(DynamoAdmin.NO_SCALING, "true");
    tableCreationOptions.put(DynamoAdmin.NO_BACKUP, "true");

    Map<String, String> indexCreationOptions = new HashMap<>();
    indexCreationOptions.put(DynamoAdmin.NO_SCALING, "true");

    Map<String, String> tableReparationOptions = new HashMap<>();
    indexCreationOptions.put(DynamoAdmin.NO_BACKUP, "true");

    // Create tables
    SchemaLoader.load(configFilePath, schemaFilePath, tableCreationOptions, createCoordinatorTables);

    // Alter tables 
    SchemaLoader.alterTables(configFilePath, alteredSchemaFilePath, indexCreationOptions);

    // Repair tables
    SchemaLoader.repairTables(configFilePath, schemaFilePath, tableReparationOptions, repairCoordinatorTables);

    // Delete tables
    SchemaLoader.unload(configFilePath, schemaFilePath, deleteCoordinatorTables);

    return 0;
  }
}
```

You can also create, delete or repair a schema by passing a serialized schema JSON string (the raw text of a schema file).
```java
// Create tables
SchemaLoader.load(configFilePath, serializedSchemaJson, tableCreationOptions, createCoordinatorTables);

// Alter tables 
SchemaLoader.alterTables(configFilePath, serializedAlteredSchemaFilePath, indexCreationOptions);

// Repair tables
SchemaLoader.repairTables(configFilePath, serializedSchemaJson, tableReparationOptions, repairCoordinatorTables);

// Delete tables
SchemaLoader.unload(configFilePath, serializedSchemaJson, deleteCoordinatorTables);
```

For ScalarDB configuration, a `Properties` object can be used as well.

```java
// Create tables
SchemaLoader.load(properties, serializedSchemaJson, tableCreationOptions, createCoordinatorTables);

// Alter tables
SchemaLoader.alterTables(properties, serializedAlteredSchemaFilePath, indexCreationOptions);

// Repair tables
SchemaLoader.repairTables(properties, serializedSchemaJson, tableReparationOptions, repairCoordinatorTables);

// Delete tables
SchemaLoader.unload(properties, serializedSchemaJson, deleteCoordinatorTables);
```
