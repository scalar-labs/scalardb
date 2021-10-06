# Scalar DB Schema Loader
This tool used for creating, deleting schemas along with necessary metadata of Scalar DB on Cosmos DB, DynamoDB, Cassandra and a JDBC database.
  - For Cosmos DB, this tool works on databases(collections) and tables(containers), also inserts metadata which is required by Scalar DB. You can specify the resource base unit, scaling option as well.
  - For DynamoDB, this tool works on tables named with the database names, also inserts metadata which is required by Scalar DB. You can specify the resource base unit, backup and scaling option as well.
  - For Cassandra, this tool works on databases(keyspaces) and tables, also inserts metadata which is required by Scalar DB. You can specify the compaction strategy, the network topology strategy, and the replication factor as well.
  - For a JDBC database, this tool works on databases(schemas) and tables, also inserts metadata which is required by Scalar DB.
  - Scalar DB metadata for transactions are automatically added when you set the `transaction` parameter `true` in the schema file.

There are two ways to specify general cli options in schema-loader.
  - Pass a Scalar DB configuration file, and additional options of database-specific.
  - Pass all the options separately.

# Usage
## Build & Run
### Build application
```console
$ ./gradlew installDist
```
The built cli application is `./build/install/schema-loader/bin/schema-loader`

### Available commands
For using config file
```console
Usage: schema-loader --config [-D] [--coordinator] [--no-backup] [--no-scaling]
                              [-c=<compactionStrategy>] [-f=<schemaFile>]
                              [-n=<replicationStrategy>] [-r=<ru>]
                              [-R=<replicaFactor>] <configPath>
Using config file for Scalar DB
      <configPath>    Path to config file of Scalar DB
  -c, --compaction-strategy=<compactionStrategy>
                      Cassandra compaction strategy, should be LCS, STCS or TWCS
      --coordinator   Create coordinator table
  -D, --delete-all    Delete tables
  -f, --schema-file=<schemaFile>
                      Path to schema json file
  -n, --replication-strategy=<replicationStrategy>
                      Cassandra network strategy, should be SimpleStrategy or
                        NetworkTopologyStrategy
      --no-backup     Disable continuous backup for Dynamo DB
      --no-scaling    Disable auto-scaling (supported in Dynamo DB, Cosmos DB)
  -r, --ru=<ru>       Base resource unit (supported in Dynamo DB, Cosmos DB)
  -R, --replication-factor=<replicaFactor>
                      Cassandra replication factor
```
For Cosmos DB
```console
Usage: schema-loader --cosmos [-D] [--no-scaling] -f=<schemaFile> -h=<uri>
                              -p=<key> [-r=<ru>]
Using Cosmos DB
  -D, --delete-all       Delete tables
  -f, --schema-file=<schemaFile>
                         Path to schema json file
  -h, --host=<uri>       Cosmos DB account URI
      --no-scaling       Disable auto-scaling for Cosmos DB
  -p, --password=<key>   Cosmos DB key
  -r, --ru=<ru>          Base resource unit
```
For Dynamo DB
```console
Usage: schema-loader --dynamo [-D] [--no-backup] [--no-scaling]
                              [--endpoint-override=<endpointOverride>]
                              -f=<schemaFile> -p=<awsSecKey> [-r=<ru>]
                              --region=<awsRegion> -u=<awsKeyId>
Using Dynamo DB
  -D, --delete-all           Delete tables
      --endpoint-override=<endpointOverride>
                             Endpoint with which the Dynamo DB SDK should
                               communicate
  -f, --schema-file=<schemaFile>
                             Path to schema json file
      --no-backup            Disable continuous backup for Dynamo DB
      --no-scaling           Disable auto-scaling for Dynamo DB
  -p, --password=<awsSecKey> AWS access secret key
  -r, --ru=<ru>              Base resource unit
      --region=<awsRegion>   AWS region
  -u, --user=<awsKeyId>      AWS access key ID
```
For Cassandra DB
```console
Usage: schema-loader --cassandra [-D] [-c=<compactionStrategy>] -f=<schemaFile>
                                 -h=<hostIP> [-n=<replicationStrategy>]
                                 [-p=<password>] [-P=<port>]
                                 [-R=<replicaFactor>] [-u=<user>]
Using Cassandra DB
  -c, --compaction-strategy=<compactionStrategy>
                        Cassandra compaction strategy, should be LCS, STCS or
                          TWCS
  -D, --delete-all      Delete tables
  -f, --schema-file=<schemaFile>
                        Path to schema json file
  -h, --host=<hostIP>   Cassandra host IP
  -n, --network-strategy=<replicationStrategy>
                        Cassandra network strategy, should be SimpleStrategy or
                          NetworkTopologyStrategy
  -p, --password=<password>
                        Cassandra password
  -P, --port=<port>     Cassandra Port
  -R, --replication-factor=<replicaFactor>
                        Cassandra replication factor
  -u, --user=<user>     Cassandra user
```
For JDBC type database
```console
Usage: schema-loader --jdbc [-D] -f=<schemaFile> -j=<url> -p=<password>
                            -u=<user>
Using JDBC type DB
  -D, --delete-all       Delete tables
  -f, --schema-file=<schemaFile>
                         Path to schema json file
  -j, --jdbc-url=<url>   JDBC URL
  -p, --password=<password>
                         JDBC password
  -u, --user=<user>      JDBC user
```
### Create databases/keyspaces and tables
Using config file based from Scalar DB. Sample config file can be found [here](../conf/database.properties)
```console
$ ./build/install/schema-loader/bin/schema-loader --config <PATH_TO_CONFIG_FILE> -f schema.json
```

Using cli arguments fully for configuration
```console
# For Cosmos DB
$ ./build/install/schema-loader/bin/schema-loader --cosmos -h <COSMOS_DB_ACCOUNT_URI> -p <COSMOS_DB_KEY> -f schema.json [-r BASE_RESOURCE_UNIT]
```
  - `<COSMOS_DB_KEY>` you can use a primary key or a secondary key.
  - `-r BASE_RESOURCE_UNIT` is an option. You can specify the RU of each database. The maximum RU in tables in the database will be set. If you don't specify RU of tables, the database RU will be set with this option. When you use transaction function, the RU of the coordinator table of Scalar DB is specified by this option. By default, it's 400.

```console
# For DynamoDB
$ ./build/install/schema-loader/bin/schema-loader --dynamo -u <AWS_ACCESS_KEY_ID> -p <AWS_ACCESS_SECRET_KEY> --region <REGION> -f schema.json [-r BASE_RESOURCE_UNIT]
```
  - `<REGION>` should be a string to specify an AWS region like `ap-northeast-1`.
  - `-r` option is almost the same as Cosmos DB option. However, the unit means DynamoDB capacity unit. The read and write capacity units are set the same value.

```console
# For Cassandra
$ ./build/install/schema-loader/bin/schema-loader --cassandra -h <CASSANDRA_IP> [-P <CASSANDRA_PORT>] [-u <CASSANDRA_USER>] [-p <CASSANDRA_PASSWORD>] -f schema.json [-n <NETWORK_STRATEGY>] [-R <REPLICATION_FACTOR>]
```

  - If `-P <CASSANDRA_PORT>` is not supplied, it defaults to `9042`.
  - If `-u <CASSANDRA_USER>` is not supplied, it defaults to `cassandra`.
  - If `-p <CASSANDRA_PASSWORD>` is not supplied, it defaults to `cassandra`.
  - `<NETWORK_STRATEGY>` should be `SimpleStrategy` or `NetworkTopologyStrategy`

```console
# For a JDBC database
$ ./build/install/schema-loader/bin/schema-loader --jdbc -j <JDBC URL> -u <USER> -p <PASSWORD> -f schema.json
```

### Delete tables
Using config file
```console
$ ./build/install/schema-loader/bin/schema-loader --config <PATH_TO_CONFIG_FILE> -f schema.json -D
```

Using cli arguments

```console
# For Cosmos DB
$ ./build/install/schema-loader/bin/schema-loader --cosmos -h <COSMOS_DB_ACCOUNT_URI> -p <COSMOS_DB_KEY> -f schema.json -D
```

```console
# For DynamoDB
$ ./build/install/schema-loader/bin/schema-loader --dynamo -u <AWS_ACCESS_KEY_ID> -p <AWS_ACCESS_SECRET_KEY> --region <REGION> -f schema.json -D
```

```console
# For Cassandra
$ ./build/install/schema-loader/bin/schema-loader --cassandra -h <CASSANDRA_IP> [-P <CASSANDRA_PORT>] [-u <CASSNDRA_USER>] [-p <CASSANDRA_PASSWORD>] -f schema.json -D
```

```console
# For a JDBC database
$ ./build/install/schema-loader/bin/schema-loader --jdbc -j <JDBC URL> -u <USER> -p <PASSWORD> -f schema.json -D
```

### Show help
```console
$ ./build/install/schema-loader/bin/schema-loader --help
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
    "ru": 5000,
    "compaction-strategy": "LCS",
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
- `compaction-strategy` should be `STCS`, `LCS` or `TWCS`. This is ignored in Cosmos DB and DynamoDB.
- This `ru` value is set for all tables on this database even if `-r BASE_RESOURCE_UNIT` is set when Cosmos DB and DynamoDB. `ru` is ignored in Cassandra.

## Scaling Performance

### RU
You can scale the throughput of Cosmos DB and DynamoDB by specifying `-r` option (which applies to all the tables) or `ru` parameter for each table. Those configurations are ignored in Cassandra. The default values are `400` for Cosmos DB and `10` for DynamoDB respectively, which are set without `-r` option.

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
| FLOAT | FLOAT | FLOAT | BINARY_FLOAT | FLOAT(24) |
| DOUBLE | DOUBLE | DOUBLE PRECISION | BINARY_DOUBLE | FLOAT |
| BOOLEAN | BOOLEAN | BOOLEAN | NUMBER(1) | BIT |
| BLOB | LONGBLOB | BYTEA | BLOB | VARBINARY(8000) |

However, the following types are converted differently when they are used as a primary key or a secondary index key due to the limitations of RDB data types.

| ScalarDB | MySQL | PostgreSQL | Oracle | SQL Server |
| ---- | ---- |  ---- |  ---- |  ---- |
| TEXT | VARCHAR(64) | VARCHAR(10485760) | VARCHAR2(64) | |
| BLOB | VARBINARY(64) | | RAW(64) | |

If this data type mapping doesn't match your application, please alter the tables to change the data types after creating them with this tool.
