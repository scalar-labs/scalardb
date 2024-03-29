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
    implementation 'com.scalar-labs:scalardb:3.8.6'
}
```

To add a dependency using Maven:
```xml
<dependency>
  <groupId>com.scalar-labs</groupId>
  <artifactId>scalardb</artifactId>
  <version>3.8.6</version>
</dependency>
```

## Docs
* [Getting started](getting-started-with-scalardb.md)
* [Java API Guide](api-guide.md)
* [ScalarDB Samples](https://github.com/scalar-labs/scalardb-samples)
* [ScalarDB Server](scalardb-server.md)
* [Multi-storage Transactions](multi-storage-transactions.md)
* [Two-phase Commit Transactions](two-phase-commit-transactions.md)
* [Design document](design.md)
* [Schema Loader](schema-loader.md)
* [Requirements and Recommendations for the Underlying Databases of ScalarDB](requirements.md)
* [How to Back up and Restore](backup-restore.md)
* [ScalarDB supported databases](scalardb-supported-databases.md)
* [Configurations for Consensus Commit](configurations-for-consensus-commit.md)
* [Storage abstraction](storage-abstraction.md)
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
* [TLA+](https://github.com/scalar-labs/scalardb/tree/master/tla+/consensus-commit)

## Contributing 
This library is mainly maintained by the Scalar Engineering Team, but of course we appreciate any help.

* For asking questions, finding answers and helping other users, please go to [stackoverflow](https://stackoverflow.com/) and use [scalardb](https://stackoverflow.com/questions/tagged/scalardb) tag.
* For filing bugs, suggesting improvements, or requesting new features, help us out by opening an issue.

Here are the contributors we are especially thankful for:
- [Toshihiro Suzuki](https://github.com/brfrn169) - created [Phoenix adapter](https://github.com/scalar-labs/scalardb-phoenix) for ScalarDB
- [Yonezawa-T2](https://github.com/Yonezawa-T2) - reported bugs around Serializable and proposed a new Serializable strategy (now named Extra-Read)

## License
ScalarDB is dual-licensed under both the Apache 2.0 License (found in the LICENSE file in the root directory) and a commercial license.
You may select, at your option, one of the above-listed licenses.
The commercial license includes several enterprise-grade features such as management tools and declarative query interfaces like GraphQL and SQL interfaces.
Regarding the commercial license, please [contact us](https://scalar-labs.com/contact_us/) for more information.
