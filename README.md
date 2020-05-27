## Scalar DB

[![CircleCI](https://circleci.com/gh/scalar-labs/scalardb/tree/master.svg?style=svg&circle-token=672f70ce7f2c4f8d9e71f7c9db8ae824e2cfaeca)](https://circleci.com/gh/scalar-labs/scalardb/tree/master)

A library that makes non-ACID distributed databases/storages ACID-compliant. It not only supports strongly-consistent ACID transactions, but also scales linearly and achieves high availability when it is deployed with distributed databases/storages such as Cassandra.

## Install
The library is available on [Maven Central](https://search.maven.org/search?q=a:scalardb). You can install it in your application using your build tool such as Gradle. For example in Gradle, you can add the following dependency to your build.gradle. Please replace the `<version>` with the version you want to use.

```
dependencies {
    compile group: 'com.scalar-labs', name: 'scalardb', version: '<version>'
}
```

## Docs
* [Getting started](docs/getting-started.md)
* [Design document](docs/design.md)
* Slides
    * [Transaction Management on Cassandra](https://www.slideshare.net/scalar-inc/transaction-management-on-cassandra) at Next Generation Cassandra Conference / ApacheCon NA 2019
* [Javadoc](https://scalar-labs.github.io/scalardb/javadoc/)
* [Jepsen tests](https://github.com/scalar-labs/scalar-jepsen)
* [TLA+](tla+/consensus-commit/README.md)
* Sample applications by contributors/collaborators
  * [Q&A application (From Indetail Engineering team)](https://github.com/indetail-blockchain/getting-started-with-scalardb)

## Contributing 
This library is mainly maintained by the Scalar Engineering Team, but of course we appreciate any help.

* For asking questions, finding answers and helping other users, please go to [scalardb-user (English only)](https://groups.google.com/forum/#!forum/scalardb-user) or [scalardb-user-ja (Japanese only)](https://groups.google.com/forum/#!forum/scalardb-user-ja).
* For filing bugs, suggesting improvements, or requesting new features, help us out by opening an issue.

Here are the contributors we are especially thankful for:
- [Toshihiro Suzuki](https://github.com/brfrn169) - created [Phoenix adapter](https://github.com/scalar-labs/scalardb-phoenix) for Scalar DB
- [Yonezawa-T2](https://github.com/Yonezawa-T2) - reported bugs around Serializable and proposed a new Serializable strategy (now named Extra-Read)

## License
Scalar DB is dual-licensed under both the Apache 2.0 License (found in the LICENSE file in the root directory) and a commercial license. You may select, at your option, one of the above-listed licenses. The commercial license includes enterprise-grade tools, such as a multi-table consistent backup/restore tool for Cassandra. Regarding the commercial license, please [contact us](https://scalar-labs.com/contact_us/) for more information.
