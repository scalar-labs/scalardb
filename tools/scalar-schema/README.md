# Schema Tool for Scalar DB
This tool makes schemas on Cassandra or Cosmos DB for [Scalar DB](https://github.com/scalar-labs/scalardb).
  - This tool creates databases(collections) and tables(containers), also inserts metadata which is required by Scalar DB for Cosmos DB

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
$ java -jar target/scalar-schema.jar --cosmos -h <YOUR_ACCOUNT_URI> -p <YOUR_ACCOUNT_PASSWORD> -f schema.json [-r BASE_RESOURCE_UNIT]
```
  - `-r BASE_RESOURCE_UNIT` is an option. You can specify the RU of each database. The maximum RU in tables in the database will be set. If you don't specify RU of tables, the database RU will be set with this option. When you use transaction function, the RU of the coordinator table of Scalar DB is specified by this option. By default, it's 400.

```console
# For Cassandra
$ java -jar target/scalar-schema.jar --cassandra -h <CASSANDRA_IP> -u <CASSNDRA_USER> -p <CASSANDRA_PASSWORD> -f schema.json [--network-strategy <NETWORK_STRATEGY> --replication-factor <REPLICATION_FACTOR>]
```

### Delete all tables
```console
# For Cosmos DB
$ java -jar target/scalar-schema.jar --cosmos -h <ACCOUNT_URI> -p <KEY> -D

# For Cassandra
$ java -jar target/scalar-schema.jar --cassandra -h <CASSANDRA_IP> -u <CASSNDRA_USER> -p <CASSANDRA_PASSWORD> -D
```

### Show help
```console
$ java -jar target/scalar-schema.jar --help
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
