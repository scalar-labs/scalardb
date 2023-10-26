# ScalarDB

[![CI](https://github.com/scalar-labs/scalardb/actions/workflows/ci.yaml/badge.svg?branch=master)](https://github.com/scalar-labs/scalardb/actions/workflows/ci.yaml)

ScalarDB is a universal transaction manager that achieves:
- database/storage-agnostic ACID transactions in a scalable manner even if an underlying database or storage is not ACID-compliant.
- multi-storage/database/service ACID transactions that can span multiple (possibly different) databases, storages, and services.

## Install
The library is available on [maven central repository](https://mvnrepository.com/artifact/com.scalar-labs/scalardb).
You can install it in your application using your build tool such as Gradle and Maven.

To add a dependency on ScalarDB using Gradle, use the following:
```gradle
dependencies {
    implementation 'com.scalar-labs:scalardb:3.10.1'
}
```

To add a dependency using Maven:
```xml
<dependency>
  <groupId>com.scalar-labs</groupId>
  <artifactId>scalardb</artifactId>
  <version>3.10.1</version>
</dependency>
```

## Docs
* [Getting started](docs/getting-started-with-scalardb.md)
* [Java API Guide](docs/api-guide.md)
* [ScalarDB Samples](https://github.com/scalar-labs/scalardb-samples)
* [ScalarDB Server](docs/scalardb-server.md)
* [Multi-storage Transactions](docs/multi-storage-transactions.md)
* [Two-phase Commit Transactions](docs/two-phase-commit-transactions.md)
* [Design document](docs/design.md)
* [Schema Loader](docs/schema-loader.md)
* [Requirements and Recommendations for the Underlying Databases of ScalarDB](docs/requirements.md)
* [How to Back up and Restore](docs/backup-restore.md)
* [ScalarDB supported databases](docs/scalardb-supported-databases.md)
* [Configurations](docs/configurations.md)
* [Storage abstraction](docs/storage-abstraction.md)
* Slides
    * [Making Cassandra more capable, faster, and more reliable](https://speakerdeck.com/scalar/making-cassandra-more-capable-faster-and-more-reliable-at-apachecon-at-home-2020) at ApacheCon@Home 2020
    * [Scalar DB: A library that makes non-ACID databases ACID-compliant](https://speakerdeck.com/scalar/scalar-db-a-library-that-makes-non-acid-databases-acid-compliant) at Database Lounge Tokyo #6 2020
    * [Transaction Management on Cassandra](https://speakerdeck.com/scalar/transaction-management-on-cassandra) at Next Generation Cassandra Conference / ApacheCon NA 2019
* Javadoc
    * [scalardb](https://javadoc.io/doc/com.scalar-labs/scalardb/latest/index.html) - ScalarDB: A universal transaction manager that achieves database-agnostic transactions and distributed transactions that span multiple databases
    * [scalardb-rpc](https://javadoc.io/doc/com.scalar-labs/scalardb-rpc/latest/index.html) - ScalarDB RPC libraries
    * [scalardb-server](https://javadoc.io/doc/com.scalar-labs/scalardb-server/latest/index.html) - ScalarDB Server: A gRPC interface of ScalarDB
    * [scalardb-schema-loader](https://javadoc.io/doc/com.scalar-labs/scalardb-schema-loader/latest/index.html) - ScalarDB Schema Loader: A tool for schema creation and schema deletion in ScalarDB
* [Jepsen tests](https://github.com/scalar-labs/scalar-jepsen)
* [TLA+](tla+/consensus-commit/README.md)

## Contributing
This library is mainly maintained by the Scalar Engineering Team, but of course we appreciate any help.

* For asking questions, finding answers and helping other users, please go to [stackoverflow](https://stackoverflow.com/) and use [scalardb](https://stackoverflow.com/questions/tagged/scalardb) tag.
* For filing bugs, suggesting improvements, or requesting new features, help us out by opening an issue.

Here are the contributors we are especially thankful for:
- [Toshihiro Suzuki](https://github.com/brfrn169) - created [Phoenix adapter](https://github.com/scalar-labs/scalardb-phoenix) for ScalarDB
- [Yonezawa-T2](https://github.com/Yonezawa-T2) - reported bugs around Serializable and proposed a new Serializable strategy (now named Extra-Read)

## Development

### Pre-commit hook

This project uses [pre-commit](https://pre-commit.com/) to automate code format and so on as much as possible. If you're interested in the development of ScalarDB, please [install pre-commit](https://pre-commit.com/#installation) and the git hook script as follows.

```
$ ls -a .pre-commit-config.yaml
.pre-commit-config.yaml
$ pre-commit install
```

The code formatter is automatically executed when commiting files. A commit will fail and be formatted by the formatter when any invalid code format is detected. Try to commit the change again.

## License
ScalarDB is dual-licensed under both the Apache 2.0 License (found in the LICENSE file in the root directory) and a commercial license.
You may select, at your option, one of the above-listed licenses.
The commercial license includes several enterprise-grade features such as ScalarDB Server, management tools, and declarative query interfaces like GraphQL and SQL interfaces.
Regarding the commercial license, please [contact us](https://scalar-labs.com/contact_us/) for more information.
