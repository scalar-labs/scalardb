# Schema Tool for Scalar DB
This tool creates Scalar DB schemas on Cosmos DB, DynamoDB and Cassandra.
  - For Cosmos DB, this tool creates databases(collections) and tables(containers), also inserts metadata which is required by Scalar DB.
  - For DynamoDB, this tool creates tables named with the database names and table names, also inserts metadata which is required by Scalar DB.
  - For Cassandra, this tool creates databases(keyspaces) and tables. You can specify the compaction strategy, the network topology strategy, and the replication factor as well.
  - You don't have to add Scalar DB metadata for transactions. This tool automatically adds them when you set the `transaction` parameter `true` in your schema file.

# Usage

## Build & Run
### Build a standalone jar
```console
$ cd /path/to/scalardb/tools/scalar-schema
$ lein uberjar
```

### Create tables
```console
# For Cosmos DB
$ java -jar target/scalar-schema-standalone-<version>.jar --cosmos -h <COSMOS_DB_ACCOUNT_URI> -p <COSMOS_DB_KEY> -f schema.json [-r BASE_RESOURCE_UNIT]
```
  - `<COSMOS_DB_KEY>` you can use a primary key or a secondary key.
  - `-r BASE_RESOURCE_UNIT` is an option. You can specify the RU of each database. The maximum RU in tables in the database will be set. If you don't specify RU of tables, the database RU will be set with this option. When you use transaction function, the RU of the coordinator table of Scalar DB is specified by this option. By default, it's 400.

```console
# For DynamoDB
$ java -jar target/scalar-schema-standalone-<version>.jar --dynamo -u <AWS_ACCESS_KEY_ID> -p <AWS_ACCESS_SECRET_KEY> --region <REGION> -f schema.json [-r BASE_RESOURCE_UNIT]
```
  - `<REGION>` should be a string to specify an AWS region like `ap-northeast-1`.
  - `-r` option is almost the same as Cosmos DB option. However, the unit means DynamoDB capacity unit. The read and write capacity units are set the same value.

```console
# For Cassandra
$ java -jar target/scalar-schema-standalone-<version>.jar --cassandra -h <CASSANDRA_IP> [-P <CASSANDRA_PORT>] [-u <CASSNDRA_USER>] [-p <CASSANDRA_PASSWORD>] -f schema.json [-n <NETWORK_STRATEGY>] [-R <REPLICATION_FACTOR>]
```

  - If `-P <CASSANDRA_PORT>` is not supplied, it defaults to `9042`.
  - If `-u <CASSNDRA_USER>` is not supplied, it defaults to `cassandra`.
  - If `-p <CASSANDRA_PASSWORD>` is not supplied, it defaults to `cassandra`.
  - `<NETWORK_STRATEGY>` should be `SimpleStrategy` or `NetworkTopologyStrategy`

### Delete tables
```console
# For Cosmos DB
$ java -jar target/scalar-schema-standalone-<version>.jar --cosmos -h <COSMOS_DB_ACCOUNT_URI> -p <COSMOS_DB_KEY> -f schema.json -D
```

```console
# For DynamoDB
$ java -jar target/scalar-schema-standalone-<version>.jar --dynamo -u <AWS_ACCESS_KEY_ID> -p <AWS_ACCESS_SECRET_KEY> --region <REGION> -f schema.json -D
```

```console
# For Cassandra
$ java -jar target/scalar-schema-standalone-<version>.jar --cassandra -h <CASSANDRA_IP> [-P <CASSANDRA_PORT>] [-u <CASSNDRA_USER>] [-p <CASSANDRA_PASSWORD>] -f schema.json -D
```

### Show help
```console
$ java -jar target/scalar-schema-standalone-<version>.jar --help
```

### Sample schema file
```json
{
  "sample-db.sample-table1": {
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
    },
    "compaction-strategy": "LCS"
  },

  "sample-db.sample-table2": {
    "transaction": false,
    "partition-key": [
      "c1"
    ],
    "clustering-key": [
      "c3",
      "c4"
    ],
    "columns": {
      "c1": "INT",
      "c2": "TEXT",
      "c3": "INT",
      "c4": "INT",
      "c5": "BOOLEAN"
    },
    "ru": 800
  }
}
```
- `compaction-strategy` should be `STCS`, `LCS` or `TWCS`. This is ignored in Cosmos DB and DynamoDB.
- This `ru` value is set for all tables on this database even if `-r BASE_RESOURCE_UNIT` is set when Cosmos DB and DynamoDB. `ru` is ignored in Cassandra.

## Scaling Performance

### RU
You can scale the throughput of Cosmos DB and DynamoDB by specifying `-r` option (which applies to all the tables) or `ru` parameter for each table. Those configurations are ignored in Cassandra. The default values are `400` for Cosmos DB and `10` for DynamoDB respectively, which are set without `-r` option.

Note that the schema tool abstracts [Request Unit](https://docs.microsoft.com/azure/cosmos-db/request-units) of Cosmos DB and [Capacity Unit](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadWriteCapacityMode.html#HowItWorks.ProvisionedThroughput.Manual) of DynamoDB with `RU`.
So, please set an appropriate value depending on the database implementations. Please also note that the schema tool sets the same value to both Read Capacity Unit and Write Capacity Unit for DynamoDB.

### Auto-scaling
By default, the schema tool enables auto-scaling of RU for all tables: RU is scaled in or out between 10% and 100% of a specified RU depending on a workload. For example, if you specify `-r 10000`, RU of each table is scaled in or out between 1000 and 10000. Note that auto-scaling of Cosmos DB is enabled only when you set more than or equal to 4000 RU.
