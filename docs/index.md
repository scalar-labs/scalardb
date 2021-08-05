## Scalar DB

[![CircleCI](https://circleci.com/gh/scalar-labs/scalardb/tree/master.svg?style=svg&circle-token=672f70ce7f2c4f8d9e71f7c9db8ae824e2cfaeca)](https://circleci.com/gh/scalar-labs/scalardb/tree/master)

A library that makes non-ACID distributed databases/storages ACID-compliant. It not only supports strongly-consistent ACID transactions, but also scales linearly and achieves high availability when it is deployed with distributed databases/storages such as Cassandra.

## Docs
* [Getting started](getting-started.md)
* [Design document](design.md)
* Slides
    * [Making Cassandra more capable, faster, and more reliable](https://www.slideshare.net/scalar-inc/making-cassandra-more-capable-faster-and-more-reliable-at-apacheconhome-2020) at ApacheCon@Home 2020
    * [Scalar DB: A library that makes non-ACID databases ACID-compliant](https://www.slideshare.net/scalar-inc/scalar-db-a-library-that-makes-nonacid-databases-acidcompliant) at Database Lounge Tokyo #6 2020
    * [Transaction Management on Cassandra](https://www.slideshare.net/scalar-inc/transaction-management-on-cassandra) at Next Generation Cassandra Conference / ApacheCon NA 2019
* [Javadoc](https://javadoc.io/doc/com.scalar-labs/scalardb/latest/index.html)
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

