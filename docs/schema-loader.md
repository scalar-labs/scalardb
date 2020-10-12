## Scalar DB Schema Tools

This document briefly explains tools used to generate and load database schemas for Scalar DB.

## Scalar DB schema generator and loader

Scalar DB schema generator (tools/scalar-schema) generates storage implementation specific schema definition and metadata definition for Scalar DB transaction
as described [here](schema.md) for a given Scalar DB database schema.
Scalar DB schema loader (tools/scalar-schema/target/scalar-schema.jar) uses the generator to get an implementation specific schema file and load it with a storage implementation specific loader for a give Scalar DB database schema.

With the above tools, you don't need to think about implementation specific schemas when modeling data for your applications.

## Scalar DB schema definition

Scalar DB schema is an abstract schema to define applications data with Scalar DB.
Here is a spec of the definition.

```json
schema_file            ::=  {
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

column_definition      ::=  column_name: data_type [example: "c1": "INT"]
data_type              ::=  BIGINT | BLOB | BOOLEAN | DOUBLE | FLOAT | INT | TEXT 
```
- `compaction-strategy` should be `STCS`, `LCS` or `TWCS`. This is ignored in Cosmos DB.
- This `ru` value is set for all tables on this database even if `-r BASE_RESOURCE_UNIT` is set when Cosmos DB. `ru` is ignored in Cassandra.
- If `transaction` value as `true`, the table is treated as transaction capable, and addtional metadata and coordinator namespace are added.

please see [this](schema.md) for more information about the metadata and the coordinator.

## Install & Build

Before installing, please install and setup [leiningen](https://leiningen.org/).
Then, `lein uberjar` will download the required packages and build generator and loader.

```
$ cd /path/to/scalardb/tools/scalar-schema
$ lein uberjar
```

## Use

After defining a schema, please run the command as follows. You need to use `--cassandra` or `--cosmos` option because scalar db supports cassandra and cosmos db.

```console
# For Cosmos DB
$ java -jar $SCALARDB_HOME/tools/scalar-schema/target/scalar-schema.jar --cosmos -h <YOUR_ACCOUNT_URI> -p <YOUR_ACCOUNT_PASSWORD> -f schema.json [-r BASE_RESOURCE_UNIT]
```
  - `-r BASE_RESOURCE_UNIT` is an option. You can specify the RU of each database. The maximum RU in tables in the database will be set. If you don't specify RU of tables, the database RU will be set with this option. When you use transaction function, the RU of the coordinator table of Scalar DB is specified by this option. By default, it's 400.

```console
# For Cassandra
$ java -jar $SCALARDB_HOME/tools/scalar-schema/target/scalar-schema.jar --cassandra -h <CASSANDRA_IP> -u <CASSNDRA_USER> -p <CASSANDRA_PASSWORD> -f schema.json [-n <NETWORK_STRATEGY> -R <REPLICATION_FACTOR>]
```
  - `<NETWORK_STRATEGY>` should be `SimpleStrategy` or `NetworkTopologyStrategy`
  - `<REPLICATION_FACTOR>` All the namespaces defined in the same file will have the specified replication factor.

Show help

```console
$ java -jar target/scalar-schema.jar --help
```