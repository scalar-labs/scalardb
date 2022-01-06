# Scalar DB Schema Loader

Scalar DB Schema Loader creates and deletes Scalar DB schemas (namespaces and tables) on the basis of a provided schema file.
Also, it automatically adds the Scalar DB transaction metadata (used in the Consensus Commit protocol) to the tables when you set the `transaction` parameter to `true` in the schema file.

There are two ways to specify general CLI options in Schema Loader:
  - Pass a Scalar DB configuration file and database/storage-specific options additionally.
  - Pass the options without a Scalar DB configuration (Deprecated).

Note that this tool supports only basic options to create/delete a table.
If you want to use advanced features of the database, please alter your tables after creating them with this tool.

# Usage

## Install

The release versions of `schema-loader` can be downloaded from [releases](https://github.com/scalar-labs/scalardb/releases) page of Scalar DB.

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
  -c, --config=<configPath>
                      Path to the config file of Scalar DB
      --compaction-strategy=<compactionStrategy>
                      The compaction strategy, must be LCS, STCS or TWCS
                        (supported in Cassandra)
      --coordinator   Create/delete coordinator table
  -D, --delete-all    Delete tables
  -f, --schema-file=<schemaFile>
                      Path to the schema json file
      --no-backup     Disable continuous backup (supported in DynamoDB)
      --no-scaling    Disable auto-scaling (supported in DynamoDB, Cosmos DB)
      --replication-factor=<replicaFactor>
                      The replication factor (supported in Cassandra)
      --replication-strategy=<replicationStrategy>
                      The replication strategy, must be SimpleStrategy or
                        NetworkTopologyStrategy (supported in Cassandra)
      --ru=<ru>       Base resource unit (supported in DynamoDB, Cosmos DB)
```

For Cosmos DB (Deprecated. Please use the command using a config file instead):
```console
Usage: java -jar scalardb-schema-loader-<version>.jar --cosmos [-D]
       [--no-scaling] -f=<schemaFile> -h=<uri> -p=<key> [-r=<ru>]
Create/Delete Cosmos DB schemas
  -D, --delete-all       Delete tables
  -f, --schema-file=<schemaFile>
                         Path to the schema json file
  -h, --host=<uri>       Cosmos DB account URI
      --no-scaling       Disable auto-scaling for Cosmos DB
  -p, --password=<key>   Cosmos DB key
  -r, --ru=<ru>          Base resource unit
```

For DynamoDB (Deprecated. Please use the command using a config file instead):
```console
Usage: java -jar scalardb-schema-loader-<version>.jar --dynamo [-D]
       [--no-backup] [--no-scaling] [--endpoint-override=<endpointOverride>]
       -f=<schemaFile> -p=<awsSecKey> [-r=<ru>] --region=<awsRegion>
       -u=<awsKeyId>
Create/Delete DynamoDB schemas
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
  -u, --user=<awsKeyId>      AWS access key ID
```

For Cassandra (Deprecated. Please use the command using a config file instead):
```console
Usage: java -jar scalardb-schema-loader-<version>.jar --cassandra [-D]
       [-c=<compactionStrategy>] -f=<schemaFile> -h=<hostIp>
       [-n=<replicationStrategy>] [-p=<password>] [-P=<port>]
       [-R=<replicationFactor>] [-u=<user>]
Create/Delete Cassandra schemas
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
  -u, --user=<user>     Cassandra user
```

For a JDBC database (Deprecated. Please use the command using a config file instead):
```console
Usage: java -jar scalardb-schema-loader-<version>.jar --jdbc [-D]
       -f=<schemaFile> -j=<url> -p=<password> -u=<user>
Create/Delete JDBC schemas
  -D, --delete-all       Delete tables
  -f, --schema-file=<schemaFile>
                         Path to the schema json file
  -j, --jdbc-url=<url>   JDBC URL
  -p, --password=<password>
                         JDBC password
  -u, --user=<user>      JDBC user
```

### Create namespaces and tables

For using a config file (Sample config file can be found [here](../conf/database.properties)):
```console
$ java -jar scalardb-schema-loader-<version>.jar --config <PATH_TO_CONFIG_FILE> -f schema.json [--coordinator]
```
  - if `--coordinator` is specified, the coordinator table will be created.

For using CLI arguments fully for configuration (Deprecated. Please use the command using a config file instead):
```console
# For Cosmos DB
$ java -jar scalardb-schema-loader-<version>.jar --cosmos -h <COSMOS_DB_ACCOUNT_URI> -p <COSMOS_DB_KEY> -f schema.json [-r BASE_RESOURCE_UNIT]
```
  - `<COSMOS_DB_KEY>` you can use a primary key or a secondary key.
  - `-r BASE_RESOURCE_UNIT` is an option. You can specify the RU of each database. The maximum RU in tables in the database will be set. If you don't specify RU of tables, the database RU will be set with this option. When you use transaction function, the RU of the coordinator table of Scalar DB is specified by this option. By default, it's 400.

```console
# For DynamoDB
$ java -jar scalardb-schema-loader-<version>.jar --dynamo -u <AWS_ACCESS_KEY_ID> -p <AWS_ACCESS_SECRET_KEY> --region <REGION> -f schema.json [-r BASE_RESOURCE_UNIT]
```
  - `<REGION>` should be a string to specify an AWS region like `ap-northeast-1`.
  - `-r` option is almost the same as Cosmos DB option. However, the unit means DynamoDB capacity unit. The read and write capacity units are set the same value.

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

### Delete tables

For using config file (Sample config file can be found [here](../conf/database.properties)):
```console
$ java -jar scalardb-schema-loader-<version>.jar --config <PATH_TO_CONFIG_FILE> -f schema.json [--coordinator] -D 
```
  - if `--coordinator` is specified, the coordinator table will be deleted.
  
For using CLI arguments fully for configuration (Deprecated. Please use the command using a config file instead):
```console
# For Cosmos DB
$ java -jar scalardb-schema-loader-<version>.jar --cosmos -h <COSMOS_DB_ACCOUNT_URI> -p <COSMOS_DB_KEY> -f schema.json -D
```

```console
# For DynamoDB
$ java -jar scalardb-schema-loader-<version>.jar --dynamo -u <AWS_ACCESS_KEY_ID> -p <AWS_ACCESS_SECRET_KEY> --region <REGION> -f schema.json -D
```

```console
# For Cassandra
$ java -jar scalardb-schema-loader-<version>.jar --cassandra -h <CASSANDRA_IP> [-P <CASSANDRA_PORT>] [-u <CASSNDRA_USER>] [-p <CASSANDRA_PASSWORD>] -f schema.json -D
```

```console
# For a JDBC database
$ java -jar scalardb-schema-loader-<version>.jar --jdbc -j <JDBC URL> -u <USER> -p <PASSWORD> -f schema.json -D
```

### Sample schema file
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

For DynamoDB and Cosmos DB:
- `ru`, a request unit. Please see [RU](#RU) for the details.

## Scaling Performance

### RU

You can scale the throughput of Cosmos DB and DynamoDB by specifying `--ru` option (which applies to all the tables) or `ru` parameter for each table. The default values are `400` for Cosmos DB and `10` for DynamoDB respectively, which are set without `--ru` option.

Note that the schema loader abstracts [Request Unit](https://docs.microsoft.com/azure/cosmos-db/request-units) of Cosmos DB and [Capacity Unit](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadWriteCapacityMode.html#HowItWorks.ProvisionedThroughput.Manual) of DynamoDB with `RU`.
So, please set an appropriate value depending on the database implementations. Please also note that the schema loader sets the same value to both Read Capacity Unit and Write Capacity Unit for DynamoDB.

### Auto-scaling

By default, the schema loader enables auto-scaling of RU for all tables: RU is scaled in or out between 10% and 100% of a specified RU depending on a workload. For example, if you specify `-r 10000`, RU of each table is scaled in or out between 1000 and 10000. Note that auto-scaling of Cosmos DB is enabled only when you set more than or equal to 4000 RU.

## Data type mapping for JDBC databases

When creating tables for a JDBC database with this tool, Scalar DB data types are converted to RDB-specific data types as shown below.

| ScalarDB | MySQL | PostgreSQL | Oracle | SQL Server |
| ---- | ---- |  ---- |  ---- |  ---- | 
| INT | INT | INT | INT | INT |
| BIGINT | BIGINT | BIGINT | NUMBER(19) | BIGINT |
| TEXT | LONGTEXT | TEXT | VARCHAR2(4000) | VARCHAR(8000) |
| FLOAT | DOUBLE | FLOAT | BINARY_FLOAT | FLOAT(24) |
| DOUBLE | DOUBLE | DOUBLE PRECISION | BINARY_DOUBLE | FLOAT |
| BOOLEAN | BOOLEAN | BOOLEAN | NUMBER(1) | BIT |
| BLOB | LONGBLOB | BYTEA | BLOB | VARBINARY(8000) |

However, the following types are converted differently when they are used as a primary key or a secondary index key due to the limitations of RDB data types.

| ScalarDB | MySQL | PostgreSQL | Oracle | SQL Server |
| ---- | ---- |  ---- |  ---- |  ---- |
| TEXT | VARCHAR(64) | VARCHAR(10485760) | VARCHAR2(64) | |
| BLOB | VARBINARY(64) | | RAW(64) | |

If this data type mapping doesn't match your application, please alter the tables to change the data types after creating them with this tool.

## Using Schema Loader in your program
You can check the version of `schema-loader` from [maven central repository](https://mvnrepository.com/artifact/com.scalar-labs/scalardb-schema-loader).
For example in Gradle, you can add the following dependency to your build.gradle. Please replace the `<version>` with the version you want to use.
```gradle
dependencies {
    implementation group: 'com.scalar-labs', name: 'scalardb-schema-loader', version: '<version>'
}
```

### Create and delete tables
You can create and delete tables that are defined in the schema using SchemaLoader by simply passing Scalar DB configuration file, schema, and additional options if needed as shown below.

```java
public class SchemaLoaderSample {
  public static int main(String... args) throws SchemaLoaderException {
    Path configFilePath = Paths.get("database.properties");
    Path schemaFilePath = Paths.get("sample_schema.json");
    boolean createCoordinatorTable = true; // whether creating the coordinator table or not
    boolean deleteCoordinatorTable = true; // whether deleting the coordinator table or not

    Map<String, String> options = new HashMap<>();

    options.put(
        CassandraAdmin.REPLICATION_STRATEGY, ReplicationStrategy.SIMPLE_STRATEGY.toString());
    options.put(CassandraAdmin.COMPACTION_STRATEGY, CompactionStrategy.LCS.toString());
    options.put(CassandraAdmin.REPLICATION_FACTOR, "1");

    options.put(DynamoAdmin.REQUEST_UNIT, "1");
    options.put(DynamoAdmin.NO_SCALING, "true");
    options.put(DynamoAdmin.NO_BACKUP, "true");

    // Create tables
    SchemaLoader.load(configFilePath, schemaFilePath, options, createCoordinatorTable);

    // Delete tables
    SchemaLoader.unload(configFilePath, schemaFilePath, deleteCoordinatorTable);

    return 0;
  }
}
```

You can also create and delete a schema by passing a serialized schema JSON string (the raw text of a schema file).
```java
// Create tables
SchemaLoader.load(configFilePath, serializedSchemaJson, options, createCoordinatorTable);

// Delete tables
SchemaLoader.unload(configFilePath, serializedSchemaJson, deleteCoordinatorTable);
```

For Scalar DB configuration, a `Properties` object can be used as well.
```java
// Create tables
SchemaLoader.load(properties, serializedSchemaJson, options, createCoordinatorTable);

// Delete tables
SchemaLoader.unload(properties, serializedSchemaJson, deleteCoordinatorTable);
```
