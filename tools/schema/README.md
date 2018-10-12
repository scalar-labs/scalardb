## Scalar DB Schema Tools

This document briefly explains tools used to generate and load database schemas for Scalar DB.

## Scalar DB schema generator and loader

Scalar DB schema generator (tools/schema/generator) generates storage implementation specific schema definition and metadata definition for Scalar DB transaction
as described [here](/docs/schema.md) for a given Scalar DB database schema.
Scalar DB schema loader (tools/schema/loader) uses the generator to get an implementation specific schema file and load it with a storage implemtation specific loader for a give Scalar DB database schema.

With the above tools, you don't need to think about implmentation specific schemas when modeling data for your applications.

## Scalar DB schema definition

Scalar DB schema is an abstract schema to define applications data with Scalar DB.
Here is a spec of the definition.

```sql
REPLICATION number

CREATE NAMESPACE namespace_name

create_table_statement ::=  CREATE [TRANSACTION] TABLE namespace_name'.'table_name  
                            '('   
                                column_definition
                                ( ',' column_definition )*
                            ');'
column_definition      ::=  column_name data_type [key_definition]
data_type              ::=  BIGINT | BLOB | BOOLEAN | DOUBLE | FLOAT | INT | TEXT 
key_definition         ::=  PARTITIONKEY | CLUSTERINGKEY 
```

`REPLICATION` is a global command for replication factor.
All the namespaces defined in the same file will have the specified replication factor.

`CREATE NAMESPACE` is a command to define namespaces.
`CREATE [TRANSACTION] TABLE` is a command to define tables.
The grammer for defining tables is as stated above.
If you add `TRANSACTION` keyword, the table is treated as transaction capable, and addtional metadata and coordinator namespace are added.
please see [this](/docs/schema.md) for more information about the metadata and the coordinator.

## Install & Build

Before installing, please install and setup [golang](https://golang.org/doc/install).
Then, `make` will download the required packages and build generator and loader.

```
$ cd /path/to/scalardb/tools/schema
$ make
```

## Use

After defining a schema, please run the command as follows. You don't need `--database` option as of writing since the only supported database is `cassandra`.

```
$ ./loader your-schema-file
```

You can also generate implementation schema file only.

```
$ ./generator your-schema-file
```

