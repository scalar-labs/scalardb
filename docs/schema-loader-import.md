# Importing existing tables to ScalarDB using ScalarDB Schema Loader

You might want to use ScalarDB (e.g., for database-spanning transactions) with your existing databases. In that case, you can import those databases under the ScalarDB control using ScalarDB Schema Loader. ScalarDB Schema Loader automatically adds ScalarDB-internal metadata columns in each existing table and metadata tables to enable various ScalarDB functionalities including transaction management across multiple databases.

## Before you begin

{% capture notice--warning %}
**Attention**

You should carefully plan to import a table to ScalarDB in production because it will add transaction metadata columns to your database tables and the ScalarDB metadata tables. There would also be several differences between your database and ScalarDB and limitations.
{% endcapture %}

<div class="notice--warning">{{ notice--warning | markdownify }}</div>

### What will be added to your databases

- ScalarDB metadata tables: ScalarDB manages namespace names and table metadata in an namespace (schema or database in underlying databases) called 'scalardb'.
- Transaction metadata columns: the Consensus Commit transaction manager requires metadata (for example, transaction ID, record version, and transaction status) stored along with the actual records to handle transactions properly. Thus, this tool adds the metadata columns if you use the Consensus Commit transaction manager.

### Requirements

- [JDBC databases](./scalardb-supported-databases.md#jdbc-databases) except for SQLite are importable.
- Each table must have primary key column(s) (composite primary keys can be available) .
- Target tables must only have columns with supported data type (see also [here](#data-type-mapping-from-jdbc-databases-to-scalardb)).

### Set up Schema Loader

See the [ScalarDB Schema Loader](./schema-loader.md#set-up-schema-loader) document to set up Schema Loader for importing existing tables.

## Run Schema Loader for importing existing tables

You can import an existing table in JDBC databases to ScalarDB using `--import` option and an import-specific schema file. To import tables, run the following command, replacing the contents in the angle brackets as described:

```console
$ java -jar scalardb-schema-loader-<VERSION>.jar --config <PATH_TO_SCALARDB_PROPERTIES_FILE> -f <PATH_TO_SCHEMA_FILE> --import
```

- `<VERSION>`: version of ScalarDB Schema Loader that you set up.
- `<PATH_TO_SCALARDB_PROPERTIES_FILE>`: path to a property file of ScalarDB. For a sample properties file, see [`database.properties`](https://github.com/scalar-labs/scalardb/blob/master/conf/database.properties).
- `<PATH_TO_SCHEMA_FILE>`: path to an import schema file. See also a [sample](#sample-import-schema-file) in the next section.

If you use the Consensus Commit transaction manager after importing existing tables, run the following command separately.

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

The import table schema consists of a namespace name, a table name, and a `transaction` field. The `transaction` field indicates whether the table will be imported for transactions or not. If you set the `transaction` field to `true` or don't specify the `transaction` field, this tool creates a table with transaction metadata if needed. If you set the `transaction` field to `false`, this tool imports a table without adding transaction metadata (that is, for a table with [Storage API](storage-abstraction.md)).

## Data-type mapping from JDBC databases to ScalarDB

The following table shows the supported data types in each JDBC database and their mapping to the ScalarDB data types. Select your database and check if your existing tables are importable or not.

<div id="tabset-1">
<div class="tab">
  <button class="tablinks" onclick="openTab(event, 'MySQL', 'tabset-1')" id="defaultOpen-1">MySQL</button>
  <button class="tablinks" onclick="openTab(event, 'PostgreSQL', 'tabset-1')">PostgreSQL</button>
  <button class="tablinks" onclick="openTab(event, 'Oracle', 'tabset-1')">Oracle</button>
  <button class="tablinks" onclick="openTab(event, 'SQLServer', 'tabset-1')">SQL Server</button>
</div>

<div id="MySQL" class="tabcontent" markdown="1">

| MySQL        | ScalarDB | Notes                 |
|--------------|----------|-----------------------|
| bigint       | BIGINT   | [*1](#warn-data-size) |
| binary       | BLOB     |                       |
| bit          | BOOLEAN  |                       |
| blob         | BLOB     | [*2](#warn-data-size) |
| char         | TEXT     | [*2](#warn-data-size) |
| double       | DOUBLE   |                       |
| float        | FLOAT    |                       |
| int          | INT      |                       |
| int unsigned | BIGINT   | [*2](#warn-data-size) |
| integer      | INT      |                       |
| longblob     | BLOB     |                       |
| longtext     | TEXT     |                       |
| mediumblob   | BLOB     | [*2](#warn-data-size) |
| mediumint    | INT      | [*2](#warn-data-size) |
| mediumtext   | TEXT     | [*2](#warn-data-size) |
| smallint     | INT      | [*2](#warn-data-size) |
| text         | TEXT     | [*2](#warn-data-size) |
| tinyblob     | BLOB     | [*2](#warn-data-size) |
| tinyint      | INT      | [*2](#warn-data-size) |
| tinyint(1)   | BOOLEAN  |                       |
| tinytext     | TEXT     | [*2](#warn-data-size) |
| varbinary    | BLOB     | [*2](#warn-data-size) |
| varchar      | TEXT     | [*2](#warn-data-size) |

Data types not listed in the above are not supported. The typical examples are shown below.

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

<div id="PostgreSQL" class="tabcontent" markdown="1">

| PostgreSQL        | ScalarDB | Notes                 |
|-------------------|----------|-----------------------|
| bigint            | BIGINT   | [*1](#warn-data-size) |
| boolean           | BOOLEAN  |                       |
| bytea             | BLOB     |                       |
| character         | TEXT     | [*2](#warn-data-size) |
| character varying | TEXT     | [*2](#warn-data-size) |
| double precision  | DOUBLE   |                       |
| integer           | INT      |                       |
| real              | FLOAT    |                       |
| smallint          | INT      | [*2](#warn-data-size) |
| text              | TEXT     |                       |

Data types not listed in the above are not supported. The typical examples are shown below.

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
| blob          | BLOB            | [*3](#warn-data-size) |
| char          | TEXT            | [*2](#warn-data-size) |
| clob          | TEXT            |                       |
| float         | DOUBLE          | [*4](#warn-data-size) |
| long          | TEXT            |                       |
| long raw      | BLOB            |                       |
| nchar         | TEXT            | [*2](#warn-data-size) |
| nclob         | TEXT            |                       |
| number        | BIGINT / DOUBLE | [*5](#warn-data-size) |
| nvarchar2     | TEXT            | [*2](#warn-data-size) |
| raw           | BLOB            | [*2](#warn-data-size) |
| varchar2      | TEXT            | [*2](#warn-data-size) |

Data types not listed in the above are not supported. The typical examples are shown below.

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
| bigint     | BIGINT   | [*1](#warn-data-size) |
| binary     | BLOB     | [*2](#warn-data-size) |
| bit        | BOOLEAN  |                       |
| char       | TEXT     | [*2](#warn-data-size) |
| float      | DOUBLE   |                       |
| image      | BLOB     |                       |
| int        | INT      |                       |
| nchar      | TEXT     | [*2](#warn-data-size) |
| ntext      | TEXT     |                       |
| nvarchar   | TEXT     | [*2](#warn-data-size) |
| real       | FLOAT    |                       |
| smallint   | INT      | [*2](#warn-data-size) |
| text       | TEXT     |                       |
| tinyint    | INT      | [*2](#warn-data-size) |
| varbinary  | BLOB     | [*2](#warn-data-size) |
| varchar    | TEXT     | [*2](#warn-data-size) |

Data types not listed in the above are not supported. The typical examples are shown below.

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
3. The maximum size of `BLOB` in ScalarDB is about 2GB (precisely 2^31-1 bytes). In contrast, Oracle `blob` can have (4GB-1)*(number of blocks). Thus, if the data larger than 2GB exists in the imported table, ScalarDB cannot read it.
4. ScalarDB does not support Oracle `float` columns that have higher precision than ScalarDB's `DOUBLE`.
5. ScalarDB does not support Oracle `numeric(p, s)` (`p` is precision and `s` is scale) columns, where `p` is larger than 15 due to the maximum size of the data type in ScalarDB. Note that ScalarDB maps the column to `BIGINT` if `s` is zero; else it maps `DOUBLE`. For the latter case, be aware that round-up or round-off can happen in the underlying database since the floating-point value will be cast to a fixed-point one.

{% endcapture %}

<div class="notice--warning" id="warn-data-size">{{ notice--warning | markdownify }}</div>

## Use import function in your application

You can use import function in your application with the following interfaces.

- [ScalarDB Admin API](./api-guide.md#import-a-table)
- [ScalarDB Schema Loader API](./schema-loader.md#use-schema-loader-in-your-application)
