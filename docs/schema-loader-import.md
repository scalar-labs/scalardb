# Importing Existing Tables to ScalarDB by Using ScalarDB Schema Loader

You might want to use ScalarDB (e.g., for database-spanning transactions) with your existing databases. In that case, you can import those databases under the ScalarDB control using ScalarDB Schema Loader. ScalarDB Schema Loader automatically adds ScalarDB-internal metadata columns in each existing table and metadata tables to enable various ScalarDB functionalities including transaction management across multiple databases.

## Before you begin

{% capture notice--warning %}
**Attention**

You should carefully plan to import a table to ScalarDB in production because it will add transaction metadata columns to your database tables and the ScalarDB metadata tables. In this case, there would also be several differences between your database and ScalarDB, as well as some limitations.
{% endcapture %}

<div class="notice--warning">{{ notice--warning | markdownify }}</div>

### What will be added to your databases

- **ScalarDB metadata tables:** ScalarDB manages namespace names and table metadata in a namespace (schema or database in underlying databases) called 'scalardb'.
- **Transaction metadata columns:** The Consensus Commit transaction manager requires metadata (for example, transaction ID, record version, and transaction status) stored along with the actual records to handle transactions properly. Thus, this tool adds the metadata columns if you use the Consensus Commit transaction manager.

{% capture notice--info %}
**Note**

This tool only changes database metadata. Thus, the processing time does not increase in proportion to the database size and usually takes only several seconds.
{% endcapture %}

<div class="notice--info">{{ notice--info | markdownify }}</div>

### Requirements

- [JDBC databases](./scalardb-supported-databases.md#jdbc-databases), except for SQLite, can be imported.
- Each table must have primary key columns. (Composite primary keys can be available.)
- Target tables must only have columns with supported data types. For details, see [Data-type mapping from JDBC databases to ScalarDB](#data-type-mapping-from-jdbc-databases-to-scalardb)).

### Set up Schema Loader

To set up Schema Loader for importing existing tables, see [Set up Schema Loader](./schema-loader.md#set-up-schema-loader). 

## Run Schema Loader for importing existing tables

You can import an existing table in JDBC databases to ScalarDB by using the `--import` option and an import-specific schema file. To import tables, run the following command, replacing the contents in the angle brackets as described:

```console
$ java -jar scalardb-schema-loader-<VERSION>.jar --config <PATH_TO_SCALARDB_PROPERTIES_FILE> -f <PATH_TO_SCHEMA_FILE> --import
```

- `<VERSION>`: Version of ScalarDB Schema Loader that you set up.
- `<PATH_TO_SCALARDB_PROPERTIES_FILE>`: Path to a properties file for ScalarDB. For a sample properties file, see [`database.properties`](https://github.com/scalar-labs/scalardb/blob/master/conf/database.properties).
- `<PATH_TO_SCHEMA_FILE>`: Path to an import schema file. For a sample, see [Sample import schema file](#sample-import-schema-file).

If you use the Consensus Commit transaction manager after importing existing tables, run the following command separately, replacing the contents in the angle brackets as described:

```console
$ java -jar scalardb-schema-loader-<VERSION>.jar --config <PATH_TO_SCALARDB_PROPERTIES_FILE> --coordinator
```

## Sample import schema file

The following is a sample schema for importing tables. For the sample schema file, see [`import_schema_sample.json`](https://github.com/scalar-labs/scalardb/blob/master/schema-loader/sample/import_schema_sample.json).

```json
{
  "sample_namespace1.sample_table1": {
    "transaction": true
  },
  "sample_namespace1.sample_table2": {
    "transaction": true
  },
  "sample_namespace2.sample_table3": {
    "transaction": false
  }
}
```

The import table schema consists of a namespace name, a table name, and a `transaction` field. The `transaction` field indicates whether the table will be imported for transactions or not. If you set the `transaction` field to `true` or don't specify the `transaction` field, this tool creates a table with transaction metadata if needed. If you set the `transaction` field to `false`, this tool imports a table without adding transaction metadata (that is, for a table using the [Storage API](storage-abstraction.md)).

## Data-type mapping from JDBC databases to ScalarDB

The following table shows the supported data types in each JDBC database and their mapping to the ScalarDB data types. Select your database and check if your existing tables can be imported.

<div id="tabset-1">
<div class="tab">
  <button class="tablinks" onclick="openTab(event, 'MySQL', 'tabset-1')" id="defaultOpen-1">MySQL</button>
  <button class="tablinks" onclick="openTab(event, 'PostgreSQL_YugabyteDB', 'tabset-1')">PostgreSQL and YugabyteDB</button>
  <button class="tablinks" onclick="openTab(event, 'Oracle', 'tabset-1')">Oracle</button>
  <button class="tablinks" onclick="openTab(event, 'SQLServer', 'tabset-1')">SQL Server</button>
</div>

<div id="MySQL" class="tabcontent" markdown="1">

| MySQL        | ScalarDB | Notes                 |
|--------------|----------|-----------------------|
| bigint       | BIGINT   | See warning (1) below |
| binary       | BLOB     |                       |
| bit          | BOOLEAN  |                       |
| blob         | BLOB     | See warning (2) below |
| char         | TEXT     | See warning (2) below |
| double       | DOUBLE   |                       |
| float        | FLOAT    |                       |
| int          | INT      |                       |
| int unsigned | BIGINT   | See warning (2) below |
| integer      | INT      |                       |
| longblob     | BLOB     |                       |
| longtext     | TEXT     |                       |
| mediumblob   | BLOB     | See warning (2) below |
| mediumint    | INT      | See warning (2) below |
| mediumtext   | TEXT     | See warning (2) below |
| smallint     | INT      | See warning (2) below |
| text         | TEXT     | See warning (2) below |
| tinyblob     | BLOB     | See warning (2) below |
| tinyint      | INT      | See warning (2) below |
| tinyint(1)   | BOOLEAN  |                       |
| tinytext     | TEXT     | See warning (2) below |
| varbinary    | BLOB     | See warning (2) below |
| varchar      | TEXT     | See warning (2) below |

Data types not listed in the above are not supported. The following are some common data types that are not supported:

- bigint unsigned
- bit(n) (n > 1)
- date
- datetime
- decimal
- enum
- geometry
- json
- numeric
- set
- time
- timestamp
- year

</div>

<div id="PostgreSQL_YugabyteDB" class="tabcontent" markdown="1">

| PostgreSQL/YugabyteDB | ScalarDB | Notes                 |
|-----------------------|----------|-----------------------|
| bigint                | BIGINT   | See warning (1) below |
| boolean               | BOOLEAN  |                       |
| bytea                 | BLOB     |                       |
| character             | TEXT     | See warning (2) below |
| character varying     | TEXT     | See warning (2) below |
| double precision      | DOUBLE   |                       |
| integer               | INT      |                       |
| real                  | FLOAT    |                       |
| smallint              | INT      | See warning (2) below |
| text                  | TEXT     |                       |

Data types not listed in the above are not supported. The following are some common data types that are not supported:

- bigserial
- bit
- box
- cidr
- circle
- date
- inet
- interval
- json
- jsonb
- line
- lseg
- macaddr
- macaddr8
- money
- numeric
- path
- pg_lsn
- pg_snapshot
- point
- polygon
- smallserial
- serial
- time
- timestamp
- tsquery
- tsvector
- txid_snapshot
- uuid
- xml

</div>

<div id="Oracle" class="tabcontent" markdown="1">

| Oracle        | ScalarDB        | Notes                 |
|---------------|-----------------|-----------------------|
| binary_double | DOUBLE          |                       |
| binary_float  | FLOAT           |                       |
| blob          | BLOB            | See warning (3) below |
| char          | TEXT            | See warning (2) below |
| clob          | TEXT            |                       |
| float         | DOUBLE          | See warning (4) below |
| long          | TEXT            |                       |
| long raw      | BLOB            |                       |
| nchar         | TEXT            | See warning (2) below |
| nclob         | TEXT            |                       |
| number        | BIGINT / DOUBLE | See warning (5) below |
| nvarchar2     | TEXT            | See warning (2) below |
| raw           | BLOB            | See warning (2) below |
| varchar2      | TEXT            | See warning (2) below |

Data types not listed in the above are not supported. The following are some common data types that are not supported:

- date
- timestamp
- interval
- rowid
- urowid
- bfile
- json

</div>

<div id="SQLServer" class="tabcontent" markdown="1">

| SQL Server | ScalarDB | Notes                 |
|------------|----------|-----------------------|
| bigint     | BIGINT   | See warning (1) below |
| binary     | BLOB     | See warning (2) below |
| bit        | BOOLEAN  |                       |
| char       | TEXT     | See warning (2) below |
| float      | DOUBLE   |                       |
| image      | BLOB     |                       |
| int        | INT      |                       |
| nchar      | TEXT     | See warning (2) below |
| ntext      | TEXT     |                       |
| nvarchar   | TEXT     | See warning (2) below |
| real       | FLOAT    |                       |
| smallint   | INT      | See warning (2) below |
| text       | TEXT     |                       |
| tinyint    | INT      | See warning (2) below |
| varbinary  | BLOB     | See warning (2) below |
| varchar    | TEXT     | See warning (2) below |

Data types not listed in the above are not supported. The following are some common data types that are not supported:

- cursor
- date
- datetime
- datetime2
- datetimeoffset
- decimal
- geography
- geometry
- hierarchyid
- money
- numeric
- rowversion
- smalldatetime
- smallmoney
- sql_variant
- time
- uniqueidentifier
- xml

</div>

</div>

{% capture notice--warning %}
**Attention**

1. The value range of `BIGINT` in ScalarDB is from -2^53 to 2^53, regardless of the size of `bigint` in the underlying database. Thus, if the data out of this range exists in the imported table, ScalarDB cannot read it.
2. For certain data types noted above, ScalarDB may map a data type larger than that of the underlying database. In that case, You will see errors when putting a value with a size larger than the size specified in the underlying database.
3. The maximum size of `BLOB` in ScalarDB is about 2GB (precisely 2^31-1 bytes). In contrast, Oracle `blob` can have (4GB-1)*(number of blocks). Thus, if data larger than 2GB exists in the imported table, ScalarDB cannot read it.
4. ScalarDB does not support Oracle `float` columns that have a higher precision than `DOUBLE` in ScalarDB.
5. ScalarDB does not support Oracle `numeric(p, s)` columns (`p` is precision and `s` is scale) when `p` is larger than 15 due to the maximum size of the data type in ScalarDB. Note that ScalarDB maps the column to `BIGINT` if `s` is zero; otherwise ScalarDB will map the column to `DOUBLE`. For the latter case, be aware that round-up or round-off can happen in the underlying database since the floating-point value will be cast to a fixed-point value.

{% endcapture %}

<div class="notice--warning" id="warn-data-size">{{ notice--warning | markdownify }}</div>

## Use import function in your application

You can use the import function in your application by using the following interfaces:

- [ScalarDB Admin API](./api-guide.md#import-a-table)
- [ScalarDB Schema Loader API](./schema-loader.md#use-schema-loader-in-your-application)
