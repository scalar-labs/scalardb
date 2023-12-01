# ScalarDB Schema Loader

ScalarDB has its own data model and schema that maps to the implementation-specific data model and schema. In addition, ScalarDB stores internal metadata, such as transaction IDs, record versions, and transaction statuses, to manage transaction logs and statuses when you use the Consensus Commit transaction manager.

Since managing the schema mapping and metadata for transactions can be difficult, you can use ScalarDB Schema Loader, which is a tool to create schemas that doesn't require you to need in-depth knowledge about schema mapping or metadata.

You have two options to specify general CLI options in Schema Loader:

- Pass the ScalarDB properties file and database-specific or storage-specific options.
- Pass database-specific or storage-specific options without the ScalarDB properties file. (Deprecated)

{% capture notice--info %}
**Note**

This tool supports only basic options to create, delete, repair, or alter a table. If you want to use the advanced features of a database, you must alter your tables with a database-specific tool after creating the tables with this tool.
{% endcapture %}

<div class="notice--info">{{ notice--info | markdownify }}</div>

## Set up Schema Loader

Select your preferred method to set up Schema Loader, and follow the instructions.

<div id="tabset-1">
<div class="tab">
  <button class="tablinks" onclick="openTab(event, 'Fat_JAR', 'tabset-1')" id="defaultOpen-1">Fat JAR</button>
  <button class="tablinks" onclick="openTab(event, 'Docker_container', 'tabset-1')">Docker container</button>
</div>

<div id="Fat_JAR" class="tabcontent" markdown="1">

You can download the release versions of Schema Loader from the [ScalarDB Releases](https://github.com/scalar-labs/scalardb/releases) page.
</div>
<div id="Docker_container" class="tabcontent" markdown="1">

You can pull the Docker image from the [Scalar container registry](https://github.com/orgs/scalar-labs/packages/container/package/scalardb-schema-loader) by running the following command, replacing the contents in the angle brackets as described:

```console
$ docker run --rm -v <PATH_TO_YOUR_LOCAL_SCHEMA_FILE>:<PATH_TO_SCHEMA_FILE_DOCKER> [-v <PATH_TO_LOCAL_SCALARDB_PROPERTIES_FILE>:<PATH_TO_SCALARDB_PROPERTIES_FILE_IN_DOCKER>] ghcr.io/scalar-labs/scalardb-schema-loader:<VERSION> <COMMAND_ARGUMENTS>
```

{% capture notice--info %}
**Note**

You can specify the same command arguments even if you use the fat JAR or the container. In the [Available commands](#available-commands) section, the JAR is used, but you can run the commands by using the container in the same way by replacing `java -jar scalardb-schema-loader-<VERSION>.jar` with `docker run --rm -v <PATH_TO_YOUR_LOCAL_SCHEMA_FILE>:<PATH_TO_SCHEMA_FILE_DOCKER> [-v <PATH_TO_LOCAL_SCALARDB_PROPERTIES_FILE>:<PATH_TO_SCALARDB_PROPERTIES_FILE_IN_DOCKER>] ghcr.io/scalar-labs/scalardb-schema-loader:<VERSION>`.
{% endcapture %}

<div class="notice--info">{{ notice--info | markdownify }}</div>
</div>
</div>

## Run Schema Loader

This section explains how to run Schema Loader.

### Available commands

Select how you would like to configure Schema Loader for your database. The preferred method is to use the properties file since other, database-specific methods are deprecated.

The following commands are available when using the properties file:

```console
Usage: java -jar scalardb-schema-loader-<VERSION>.jar [-D] [--coordinator]
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
      --coordinator   Create/delete/repair Coordinator tables
  -D, --delete-all    Delete tables
  -f, --schema-file=<schemaFile>
  -I, --import        Import tables : it will import existing non-ScalarDB
                        tables to ScalarDB.
                      Path to the schema json file
      --no-backup     Disable continuous backup (supported in DynamoDB)
      --no-scaling    Disable auto-scaling (supported in DynamoDB, Cosmos DB)
      --repair-all    Repair namespaces and tables that are in an unknown
                        state: it re-creates namespaces, tables, secondary
                        indexes, and their metadata if necessary.          
      --replication-factor=<replicaFactor>
                      The replication factor (supported in Cassandra)
      --replication-strategy=<replicationStrategy>
                      The replication strategy, must be SimpleStrategy or
                        NetworkTopologyStrategy (supported in Cassandra)
      --ru=<ru>       Base resource unit (supported in DynamoDB, Cosmos DB)
```

For a sample properties file, see [`database.properties`](https://github.com/scalar-labs/scalardb/blob/master/conf/database.properties).

{% capture notice--info %}
**Note**

The following database-specific methods have been deprecated. Please use the [commands for configuring the properties file](#available-commands) instead.

<div id="tabset-2">
<div class="tab">
  <button class="tablinks" onclick="openTab(event, 'Cassandra-2', 'tabset-2')" id="defaultOpen-2">Cassandra</button>
  <button class="tablinks" onclick="openTab(event, 'Cosmos_DB_for_NoSQL-2', 'tabset-2')">Cosmos DB for NoSQL</button>
  <button class="tablinks" onclick="openTab(event, 'DynamoDB-2', 'tabset-2')">DynamoDB</button>
  <button class="tablinks" onclick="openTab(event, 'JDBC_databases-2', 'tabset-2')">JDBC databases</button>
</div>

<div id="Cassandra-2" class="tabcontent" markdown="1">

```console
Usage: java -jar scalardb-schema-loader-<VERSION>.jar --cassandra [-D]
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
</div>
<div id="Cosmos_DB_for_NoSQL-2" class="tabcontent" markdown="1">

```console
Usage: java -jar scalardb-schema-loader-<VERSION>.jar --cosmos [-D]
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
</div>
<div id="DynamoDB-2" class="tabcontent" markdown="1">

```console
Usage: java -jar scalardb-schema-loader-<VERSION>.jar --dynamo [-D]
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
</div>
<div id="JDBC_databases-2" class="tabcontent" markdown="1">

```console
Usage: java -jar scalardb-schema-loader-<VERSION>.jar --jdbc [-D]
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
</div>
</div>
{% endcapture %}

<div class="notice--info">{{ notice--info | markdownify }}</div>

### Create namespaces and tables

To create namespaces and tables by using a properties file, run the following command, replacing the contents in the angle brackets as described:

```console
$ java -jar scalardb-schema-loader-<VERSION>.jar --config <PATH_TO_SCALARDB_PROPERTIES_FILE> -f <PATH_TO_SCHEMA_FILE> [--coordinator]
```

If `--coordinator` is specified, a [Coordinator table](api-guide.md#specify-operations-for-the-coordinator-table) will be created.

{% capture notice--info %}
**Note**

The following database-specific CLI arguments have been deprecated. Please use the CLI arguments for configuring the properties file instead.

<div id="tabset-3">
<div class="tab">
  <button class="tablinks" onclick="openTab(event, 'Cassandra-3', 'tabset-3')" id="defaultOpen-3">Cassandra</button>
  <button class="tablinks" onclick="openTab(event, 'Cosmos_DB_for_NoSQL-3', 'tabset-3')">Cosmos DB for NoSQL</button>
  <button class="tablinks" onclick="openTab(event, 'DynamoDB-3', 'tabset-3')">DynamoDB</button>
  <button class="tablinks" onclick="openTab(event, 'JDBC_databases-3', 'tabset-3')">JDBC databases</button>
</div>

<div id="Cassandra-3" class="tabcontent" markdown="1">

```console
$ java -jar scalardb-schema-loader-<VERSION>.jar --cassandra -h <CASSANDRA_IP> [-P <CASSANDRA_PORT>] [-u <CASSANDRA_USER>] [-p <CASSANDRA_PASSWORD>] -f <PATH_TO_SCHEMA_FILE> [-n <NETWORK_STRATEGY>] [-R <REPLICATION_FACTOR>]
```

- If `-P <CASSANDRA_PORT>` is not supplied, it defaults to `9042`.
- If `-u <CASSANDRA_USER>` is not supplied, it defaults to `cassandra`.
- If `-p <CASSANDRA_PASSWORD>` is not supplied, it defaults to `cassandra`.
- `<NETWORK_STRATEGY>` should be `SimpleStrategy` or `NetworkTopologyStrategy`
</div>
<div id="Cosmos_DB_for_NoSQL-3" class="tabcontent" markdown="1">

```console
$ java -jar scalardb-schema-loader-<VERSION>.jar --cosmos -h <COSMOS_DB_FOR_NOSQL_ACCOUNT_URI> -p <COSMOS_DB_FOR_NOSQL_KEY> -f <PATH_TO_SCHEMA_FILE> [-r BASE_RESOURCE_UNIT]
```

- `<COSMOS_DB_FOR_NOSQL_KEY>` you can use a primary key or a secondary key.
- `-r BASE_RESOURCE_UNIT` is an option. You can specify the RU of each database. The maximum RU in tables in the database will be set. If you don't specify RU of tables, the database RU will be set with this option. By default, it's 400.
</div>
<div id="DynamoDB-3" class="tabcontent" markdown="1">

```console
$ java -jar scalardb-schema-loader-<VERSION>.jar --dynamo -u <AWS_ACCESS_KEY_ID> -p <AWS_ACCESS_SECRET_KEY> --region <REGION> -f <PATH_TO_SCHEMA_FILE> [-r BASE_RESOURCE_UNIT]
```

- `<REGION>` should be a string to specify an AWS region like `ap-northeast-1`.
- `-r` option is almost the same as Cosmos DB for NoSQL option. However, the unit means DynamoDB capacity unit. The read and write capacity units are set the same value.
</div>
<div id="JDBC_databases-3" class="tabcontent" markdown="1">

```console
$ java -jar scalardb-schema-loader-<VERSION>.jar --jdbc -j <JDBC_URL> -u <USER> -p <PASSWORD> -f <PATH_TO_SCHEMA_FILE>
```
</div>
</div>
{% endcapture %}

<div class="notice--info">{{ notice--info | markdownify }}</div>

### Alter tables

You can use a command to add new columns to and create or delete a secondary index for existing tables. This command compares the provided table schema to the existing schema to decide which columns need to be added and which indexes need to be created or deleted.

To add new colums to and create or delete a secondary index for existing tables, run the following command, replacing the contents in the angle brackets as described:

```console
$ java -jar scalardb-schema-loader-<VERSION>.jar --config <PATH_TO_SCALARDB_PROPERTIES_FILE> -f <PATH_TO_SCHEMA_FILE> --alter
```

{% capture notice--info %}
**Note**

The following database-specific CLI arguments have been deprecated. Please use the CLI arguments for configuring the properties file instead.

<div id="tabset-4">
<div class="tab">
  <button class="tablinks" onclick="openTab(event, 'Cassandra-4', 'tabset-4')" id="defaultOpen-4">Cassandra</button>
  <button class="tablinks" onclick="openTab(event, 'Cosmos_DB_for_NoSQL-4', 'tabset-4')">Cosmos DB for NoSQL</button>
  <button class="tablinks" onclick="openTab(event, 'DynamoDB-4', 'tabset-4')">DynamoDB</button>
  <button class="tablinks" onclick="openTab(event, 'JDBC_databases-4', 'tabset-4')">JDBC databases</button>
</div>

<div id="Cassandra-4" class="tabcontent" markdown="1">

```console
$ java -jar scalardb-schema-loader-<VERSION>.jar --cassandra -h <CASSANDRA_IP> [-P <CASSANDRA_PORT>] [-u <CASSANDRA_USER>] [-p <CASSANDRA_PASSWORD>] -f <PATH_TO_SCHEMA_FILE> --alter
```
</div>
<div id="Cosmos_DB_for_NoSQL-4" class="tabcontent" markdown="1">

```console
$ java -jar scalardb-schema-loader-<VERSION>.jar --cosmos -h <COSMOS_DB_FOR_NOSQL_ACCOUNT_URI> -p <COSMOS_DB_FOR_NOSQL_KEY> -f <PATH_TO_SCHEMA_FILE> --alter
```
</div>
<div id="DynamoDB-4" class="tabcontent" markdown="1">

```console
$ java -jar scalardb-schema-loader-<VERSION>.jar --dynamo -u <AWS_ACCESS_KEY_ID> -p <AWS_ACCESS_SECRET_KEY> --region <REGION> -f <PATH_TO_SCHEMA_FILE> --alter
```
</div>
<div id="JDBC_databases-4" class="tabcontent" markdown="1">

```console
$ java -jar scalardb-schema-loader-<VERSION>.jar --jdbc -j <JDBC_URL> -u <USER> -p <PASSWORD> -f <PATH_TO_SCHEMA_FILE> --alter
```
</div>
</div>
{% endcapture %}

<div class="notice--info">{{ notice--info | markdownify }}</div>

### Delete tables

You can delete tables by using the properties file. To delete tables, run the following command, replacing the contents in the angle brackets as described:

```console
$ java -jar scalardb-schema-loader-<VERSION>.jar --config <PATH_TO_SCALARDB_PROPERTIES_FILE> -f <PATH_TO_SCHEMA_FILE> [--coordinator] -D 
```

If `--coordinator` is specified, the Coordinator table will be deleted as well.

{% capture notice--info %}
**Note**

The following database-specific CLI arguments have been deprecated. Please use the CLI arguments for configuring the properties file instead.

<div id="tabset-5">
<div class="tab">
  <button class="tablinks" onclick="openTab(event, 'Cassandra-5', 'tabset-5')" id="defaultOpen-5">Cassandra</button>
  <button class="tablinks" onclick="openTab(event, 'Cosmos_DB_for_NoSQL-5', 'tabset-5')">Cosmos DB for NoSQL</button>
  <button class="tablinks" onclick="openTab(event, 'DynamoDB-5', 'tabset-5')">DynamoDB</button>
  <button class="tablinks" onclick="openTab(event, 'JDBC_databases-5', 'tabset-4')">JDBC databases</button>
</div>

<div id="Cassandra-5" class="tabcontent" markdown="1">

```console
$ java -jar scalardb-schema-loader-<VERSION>.jar --cassandra -h <CASSANDRA_IP> [-P <CASSANDRA_PORT>] [-u <CASSANDRA_USER>] [-p <CASSANDRA_PASSWORD>] -f <PATH_TO_SCHEMA_FILE> -D
```
</div>
<div id="Cosmos_DB_for_NoSQL-5" class="tabcontent" markdown="1">

```console
$ java -jar scalardb-schema-loader-<VERSION>.jar --cosmos -h <COSMOS_DB_FOR_NOSQL_ACCOUNT_URI> -p <COSMOS_DB_FOR_NOSQL_KEY> -f <PATH_TO_SCHEMA_FILE> -D
```
</div>
<div id="DynamoDB-5" class="tabcontent" markdown="1">

```console
$ java -jar scalardb-schema-loader-<VERSION>.jar --dynamo -u <AWS_ACCESS_KEY_ID> -p <AWS_ACCESS_SECRET_KEY> --region <REGION> -f <PATH_TO_SCHEMA_FILE> -D
```
</div>
<div id="JDBC_databases-5" class="tabcontent" markdown="1">

```console
$ java -jar scalardb-schema-loader-<VERSION>.jar --jdbc -j <JDBC_URL> -u <USER> -p <PASSWORD> -f <PATH_TO_SCHEMA_FILE> -D
```
</div>
</div>
{% endcapture %}

<div class="notice--info">{{ notice--info | markdownify }}</div>

### Repair namespaces and tables

You can repair namespaces and tables by using the properties file. The reason for repairing namespaces and tables is because they can be in an unknown state, such as a namespace or table exists in the underlying storage but not its ScalarDB metadata or vice versa. Repairing the namespaces, the tables, the secondary indexes, and their metadata requires re-creating them if necessary. To repair them, run the following command, replacing the contents in the angle brackets as described:

```console
$ java -jar scalardb-schema-loader-<VERSION>.jar --config <PATH_TO_SCALARDB_PROPERTIES_FILE> -f <PATH_TO_SCHEMA_FILE> [--coordinator] --repair-all 
```

If `--coordinator` is specified, the Coordinator table will be repaired as well. In addition, if you're using Cosmos DB for NoSQL, running this command will also repair stored procedures attached to each table.

{% capture notice--info %}
**Note**

The following database-specific CLI arguments have been deprecated. Please use the CLI arguments for configuring the properties file instead.

<div id="tabset-6">
<div class="tab">
  <button class="tablinks" onclick="openTab(event, 'Cassandra-6', 'tabset-6')" id="defaultOpen-6">Cassandra</button>
  <button class="tablinks" onclick="openTab(event, 'Cosmos_DB_for_NoSQL-6', 'tabset-6')">Cosmos DB for NoSQL</button>
  <button class="tablinks" onclick="openTab(event, 'DynamoDB-6', 'tabset-6')">DynamoDB</button>
  <button class="tablinks" onclick="openTab(event, 'JDBC_databases-6', 'tabset-6')">JDBC databases</button>
</div>

<div id="Cassandra-6" class="tabcontent" markdown="1">

```console
$ java -jar scalardb-schema-loader-<VERSION>.jar --cassandra -h <CASSANDRA_IP> [-P <CASSANDRA_PORT>] [-u <CASSANDRA_USER>] [-p <CASSANDRA_PASSWORD>] -f <PATH_TO_SCHEMA_FILE> --repair-all
```
</div>
<div id="Cosmos_DB_for_NoSQL-6" class="tabcontent" markdown="1">

```console
$ java -jar scalardb-schema-loader-<VERSION>.jar --cosmos -h <COSMOS_DB_FOR_NOSQL_ACCOUNT_URI> -p <COSMOS_DB_FOR_NOSQL_KEY> -f <PATH_TO_SCHEMA_FILE> --repair-all
```
</div>
<div id="DynamoDB-6" class="tabcontent" markdown="1">

```console
$ java -jar scalardb-schema-loader-<VERSION>.jar --dynamo -u <AWS_ACCESS_KEY_ID> -p <AWS_ACCESS_SECRET_KEY> --region <REGION> [--no-backup] -f <PATH_TO_SCHEMA_FILE> --repair-all
```
</div>
<div id="JDBC_databases-6" class="tabcontent" markdown="1">

```console
$ java -jar scalardb-schema-loader-<VERSION>.jar --jdbc -j <JDBC_URL> -u <USER> -p <PASSWORD> -f <PATH_TO_SCHEMA_FILE> --repair-all
```
</div>
</div>
{% endcapture %}

<div class="notice--info">{{ notice--info | markdownify }}</div>

### Import tables

You can import an existing table in JDBC databases to ScalarDB by using the `--import` option and an import-specific schema file. For details, see [Importing Existing Tables to ScalarDB by Using ScalarDB Schema Loader](./schema-loader-import.md).

### Sample schema file

The following is a sample schema. For a sample schema file, see [`schema_sample.json`](https://github.com/scalar-labs/scalardb/blob/master/schema-loader/sample/schema_sample.json).

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

- The `columns` field defines columns of the table and their data types.
- The `partition-key` field defines which columns the partition key is composed of.
- The `clustering-key` field defines which columns the clustering key is composed of.
- The `secondary-index` field defines which columns are indexed.
- The `transaction` field indicates whether the table is for transactions or not.
  - If you set the `transaction` field to `true` or don't specify the `transaction` field, this tool creates a table with transaction metadata if needed.
  - If you set the `transaction` field to `false`, this tool creates a table without any transaction metadata (that is, for a table with [Storage API](storage-abstraction.md)).

You can also specify database or storage-specific options in the table definition as follows:

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

The database or storage-specific options you can specify are as follows:

<div id="tabset-7">
<div class="tab">
  <button class="tablinks" onclick="openTab(event, 'Cassandra-7', 'tabset-7')" id="defaultOpen-7">Cassandra</button>
  <button class="tablinks" onclick="openTab(event, 'Cosmos_DB_for_NoSQL-7', 'tabset-7')">Cosmos DB for NoSQL</button>
  <button class="tablinks" onclick="openTab(event, 'DynamoDB-7', 'tabset-7')">DynamoDB</button>
  <button class="tablinks" onclick="openTab(event, 'JDBC_databases-7', 'tabset-7')">JDBC databases</button>
</div>

<div id="Cassandra-7" class="tabcontent" markdown="1">

The `compaction-strategy` option is the compaction strategy used. This option should be `STCS` (SizeTieredCompaction), `LCS` (LeveledCompactionStrategy), or `TWCS` (TimeWindowCompactionStrategy).
</div>
<div id="Cosmos_DB_for_NoSQL-7" class="tabcontent" markdown="1">

The `ru` option stands for Request Units. For details, see [RUs](#rus).
</div>
<div id="DynamoDB-7" class="tabcontent" markdown="1">

The `ru` option stands for Request Units. For details, see [RUs](#rus).
</div>
<div id="JDBC_databases-7" class="tabcontent" markdown="1">

No options are available for JDBC databases.
</div>
</div>

## Scale for performance when using Cosmos DB for NoSQL or DynamoDB

When using Cosmos DB for NoSQL or DynamoDB, you can scale by using Request Units (RUs) or auto-scaling.

### RUs

You can scale the throughput of Cosmos DB for NoSQL and DynamoDB by specifying the `--ru` option. When specifying this option, scaling applies to all tables or the `ru` parameter for each table.

If the `--ru` option is not set, the default values will be `400` for Cosmos DB for NoSQL and `10` for DynamoDB.

{% capture notice--info %}
**Note**

- Schema Loader abstracts [Request Units](https://docs.microsoft.com/azure/cosmos-db/request-units) for Cosmos DB for NoSQL and [Capacity Units](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadWriteCapacityMode.html#HowItWorks.ProvisionedThroughput.Manual) for DynamoDB with `RU`. Therefore, be sure to set an appropriate value depending on the database implementation.
- Be aware that Schema Loader sets the same value to both read capacity unit and write capacity unit for DynamoDB.
{% endcapture %}

<div class="notice--info">{{ notice--info | markdownify }}</div>

### Auto-scaling

By default, Schema Loader enables auto-scaling of RUs for all tables: RUs scale between 10 percent and 100 percent of a specified RU depending on the workload. For example, if you specify `-r 10000`, the RUs of each table auto-scales between `1000` and `10000`.

{% capture notice--info %}
**Note**

Auto-scaling for Cosmos DB for NoSQL is enabled only when this option is set to `4000` or more.
{% endcapture %}

<div class="notice--info">{{ notice--info | markdownify }}</div>

## Data-type mapping between ScalarDB and other databases

The following table shows the supported data types in ScalarDB and their mapping to the data types of other databases.

| ScalarDB  | Cassandra | Cosmos DB for NoSQL | DynamoDB | MySQL    | PostgreSQL       | Oracle         | SQL Server      | SQLite  |
|-----------|-----------|---------------------|----------|----------|------------------|----------------|-----------------|---------|
| BOOLEAN   | boolean   | boolean (JSON)      | BOOL     | boolean  | boolean          | number(1)      | bit             | boolean |
| INT       | int       | number (JSON)       | N        | int      | int              | int            | int             | int     |
| BIGINT    | bigint    | number (JSON)       | N        | bigint   | bigint           | number(19)     | bigint          | bigint  |
| FLOAT     | float     | number (JSON)       | N        | double   | float            | binary_float   | float(24)       | float   |
| DOUBLE    | double    | number (JSON)       | N        | double   | double precision | binary_double  | float           | double  |
| TEXT      | text      | string (JSON)       | S        | longtext | text             | varchar2(4000) | varchar(8000)   | text    |
| BLOB      | blob      | string (JSON)       | B        | longblob | bytea            | RAW(2000)      | varbinary(8000) | blob    |

However, the following data types in JDBC databases are converted differently when they are used as a primary key or a secondary index key. This is due to the limitations of RDB data types.

| ScalarDB | MySQL         | PostgreSQL        | Oracle       |
|----------|---------------|-------------------|--------------|
| TEXT     | VARCHAR(64)   | VARCHAR(10485760) | VARCHAR2(64) |
| BLOB     | VARBINARY(64) |                   | RAW(64)      |

The value range of `BIGINT` in ScalarDB is from -2^53 to 2^53, regardless of the underlying database.

If this data-type mapping doesn't match your application, please alter the tables to change the data types after creating them by using this tool.

## Internal metadata for Consensus Commit

The Consensus Commit transaction manager manages metadata (for example, transaction ID, record version, and transaction status) stored along with the actual records to handle transactions properly.

Thus, along with any columns that the application requires, additional columns for the metadata need to be defined in the schema. Additionally, this tool creates a table with the metadata if you use the Consensus Commit transaction manager.

## Use Schema Loader in your application

You can check the version of Schema Loader from the [Maven Central Repository](https://mvnrepository.com/artifact/com.scalar-labs/scalardb-schema-loader). For example in Gradle, you can add the following dependency to your `build.gradle` file, replacing `<VERSION>` with the version of Schema Loader that you want to use:

```gradle
dependencies {
    implementation 'com.scalar-labs:scalardb-schema-loader:<VERSION>'
}
```

### Create, alter, repair, or delete tables

You can create, alter, delete, or repair tables that are defined in the schema by using Schema Loader. To do this, you can pass a ScalarDB properties file, schema, and additional options, if needed, as shown below:

```java
public class SchemaLoaderSample {
  public static int main(String... args) throws SchemaLoaderException {
    Path configFilePath = Paths.get("database.properties");
    // "sample_schema.json" and "altered_sample_schema.json" can be found in the "/sample" directory.
    Path schemaFilePath = Paths.get("sample_schema.json");
    Path alteredSchemaFilePath = Paths.get("altered_sample_schema.json");
    boolean createCoordinatorTables = true; // whether to create the Coordinator table or not
    boolean deleteCoordinatorTables = true; // whether to delete the Coordinator table or not
    boolean repairCoordinatorTables = true; // whether to repair the Coordinator table or not

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

    Map<String, String> reparationOptions = new HashMap<>();
    reparationOptions.put(DynamoAdmin.NO_BACKUP, "true");

    // Create tables.
    SchemaLoader.load(configFilePath, schemaFilePath, tableCreationOptions, createCoordinatorTables);

    // Alter tables.
    SchemaLoader.alterTables(configFilePath, alteredSchemaFilePath, indexCreationOptions);

    // Repair namespaces and tables.
    SchemaLoader.repairAll(configFilePath, schemaFilePath, reparationOptions, repairCoordinatorTables);

    // Delete tables.
    SchemaLoader.unload(configFilePath, schemaFilePath, deleteCoordinatorTables);

    return 0;
  }
}
```

You can also create, delete, or repair a schema by passing a serialized-schema JSON string (the raw text of a schema file) as shown below:

```java
// Create tables.
SchemaLoader.load(configFilePath, serializedSchemaJson, tableCreationOptions, createCoordinatorTables);

// Alter tables.
SchemaLoader.alterTables(configFilePath, serializedAlteredSchemaFilePath, indexCreationOptions);

// Repair namespaces and tables.
SchemaLoader.repairAll(configFilePath, serializedSchemaJson, reparationOptions, repairCoordinatorTables);

// Delete tables.
SchemaLoader.unload(configFilePath, serializedSchemaJson, deleteCoordinatorTables);
```

When configuring ScalarDB, you can use a `Properties` object as well, as shown below:

```java
// Create tables.
SchemaLoader.load(properties, serializedSchemaJson, tableCreationOptions, createCoordinatorTables);

// Alter tables.
SchemaLoader.alterTables(properties, serializedAlteredSchemaFilePath, indexCreationOptions);

// Repair namespaces and tables.
SchemaLoader.repairAll(properties, serializedSchemaJson, reparationOptions, repairCoordinatorTables);

// Delete tables.
SchemaLoader.unload(properties, serializedSchemaJson, deleteCoordinatorTables);
```

### Import tables

You can import an existing JDBC database table to ScalarDB by using the `--import` option and an import-specific schema file, in a similar manner as shown in [Sample schema file](#sample-schema-file). For details, see [Importing Existing Tables to ScalarDB by Using ScalarDB Schema Loader](./schema-loader-import.md).

{% capture notice--warning %}
**Attention**

You should carefully plan to import a table to ScalarDB in production because it will add transaction metadata columns to your database tables and the ScalarDB metadata tables. In this case, there would also be several differences between your database and ScalarDB, as well as some limitations.
{% endcapture %}

<div class="notice--warning">{{ notice--warning | markdownify }}</div>

The following is an import sample:

```java
public class SchemaLoaderImportSample {
  public static int main(String... args) throws SchemaLoaderException {
    Path configFilePath = Paths.get("database.properties");
    // "import_sample_schema.json" can be found in the "/sample" directory.
    Path schemaFilePath = Paths.get("import_sample_schema.json");
    Map<String, String> tableImportOptions = new HashMap<>();

    // Import tables.
    // You can also use a Properties object instead of configFilePath and a serialized-schema JSON
    // string instead of schemaFilePath.
    SchemaLoader.importTables(configFilePath, schemaFilePath, tableImportOptions);

    return 0;
  }
}
```
