## Scalar DB

[![CI](https://github.com/scalar-labs/scalardb/actions/workflows/ci.yaml/badge.svg?branch=master)](https://github.com/scalar-labs/scalardb/actions/workflows/ci.yaml)

Scalar DB is a universal transaction manager that achieves:
- database/storage-agnostic ACID transactions in a scalable manner even if an underlying database or storage is not ACID-compliant.
- multi-storage/database/service ACID transactions that can span multiple (possibly different) databases, storages, and services.

## Install
The library is available on [Maven Central](https://search.maven.org/search?q=a:scalardb). 
You can install it in your application using your build tool such as Gradle and Maven. 

To add a dependency on Scalar DB using Gradle, use the following:
```gradle
dependencies {
    implementation group: 'com.scalar-labs', name: 'scalardb', version: '3.5.9'
}
```

To add a dependency using Maven:
```xml
<dependency>
  <groupId>com.scalar-labs</groupId>
  <artifactId>scalardb</artifactId>
  <version>3.5.9</version>
</dependency>
```

## Docs
* [Getting started](getting-started-with-scalardb.md)
* [Scalar DB Samples](https://github.com/scalar-labs/scalardb-samples)
* [Scalar DB Server](scalardb-server.md)
* [Multi-storage Transactions](multi-storage-transactions.md)
* [Two-phase Commit Transactions](two-phase-commit-transactions.md)
* [Design document](design.md)
* [Scalar DB supported databases](scalardb-supported-databases.md)
* Slides
    * [Making Cassandra more capable, faster, and more reliable](https://www.slideshare.net/scalar-inc/making-cassandra-more-capable-faster-and-more-reliable-at-apacheconhome-2020) at ApacheCon@Home 2020
    * [Scalar DB: A library that makes non-ACID databases ACID-compliant](https://www.slideshare.net/scalar-inc/scalar-db-a-library-that-makes-nonacid-databases-acidcompliant) at Database Lounge Tokyo #6 2020
    * [Transaction Management on Cassandra](https://www.slideshare.net/scalar-inc/transaction-management-on-cassandra) at Next Generation Cassandra Conference / ApacheCon NA 2019
* Javadoc
    * [scalardb](https://javadoc.io/doc/com.scalar-labs/scalardb/latest/index.html) - A library that makes non-ACID distributed databases/storages ACID-compliant
    * [scalardb-rpc](https://javadoc.io/doc/com.scalar-labs/scalardb-rpc/latest/index.html) - Scalar DB RPC libraries
    * [scalardb-server](https://javadoc.io/doc/com.scalar-labs/scalardb-server/latest/index.html) - Scalar DB Server that is the gRPC interfarce of Scalar DB
    * [scalardb-schema-loader](https://javadoc.io/doc/com.scalar-labs/scalardb-schema-loader/latest/index.html) - Scalar DB Schema Loader that creates and deletes Scalar DB schemas
* [Jepsen tests](https://github.com/scalar-labs/scalar-jepsen)
* [TLA+](https://github.com/scalar-labs/scalardb/tree/master/tla+/consensus-commit)
* Sample applications by contributors/collaborators
  * [Q&A application (From Indetail Engineering team)](https://github.com/indetail-blockchain/getting-started-with-scalardb)

## Contributing 
This library is mainly maintained by the Scalar Engineering Team, but of course we appreciate any help.

* For asking questions, finding answers and helping other users, please go to [stackoverflow](https://stackoverflow.com/) and use [scalardb](https://stackoverflow.com/questions/tagged/scalardb) tag.
* For filing bugs, suggesting improvements, or requesting new features, help us out by opening an issue.

Here are the contributors we are especially thankful for:
- [Toshihiro Suzuki](https://github.com/brfrn169) - created [Phoenix adapter](https://github.com/scalar-labs/scalardb-phoenix) for Scalar DB
- [Yonezawa-T2](https://github.com/Yonezawa-T2) - reported bugs around Serializable and proposed a new Serializable strategy (now named Extra-Read)

## License
Scalar DB is dual-licensed under both the Apache 2.0 License (found in the LICENSE file in the root directory) and a commercial license. You may select, at your option, one of the above-listed licenses. The commercial license includes enterprise-grade tools, such as a multi-table consistent backup/restore tool for Cassandra. Regarding the commercial license, please [contact us](https://scalar-labs.com/contact_us/) for more information.
