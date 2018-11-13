## Scalar DB

[![CircleCI](https://circleci.com/gh/scalar-labs/scalardb/tree/master.svg?style=svg&circle-token=672f70ce7f2c4f8d9e71f7c9db8ae824e2cfaeca)](https://circleci.com/gh/scalar-labs/scalardb/tree/master)

A library that provides a distributed storage abstraction and client-coordinated distributed transaction manager on the storage, and makes non-ACID distributed databases/storages ACID-compliant. It not only supports strongly-consistnt ACID transactions, but also scales linearly and achieves high availability when it is deployed with distributed databases/storages such as Cassandra.

## Docs
* [Getting started](docs/getting-started.md)
* [Design document](docs/design.md)
* [Javadoc](https://scalar-labs.github.io/scalardb/javadoc/)
* [Jepsen tests](jepsen/scalardb)
* Sample applications by 3rd party developers (coming soon)

## Contributing 
This library is mainly maintained by the Scalar Engineering Team, but of course we appreciate any help.

* For asking questions, finding answers and helping other users, please use the public mailing list scalardb-user@googlegroups.com or go to https://groups.google.com/forum/#!forum/scalardb-user.
* For filing bugs, suggesting improvements, or requesting new features, help us out by opening an issue.

## License
Scalar DB is dual-licensed under both the Apache 2.0 License (found in the LICENSE file in the root directory) and a commercial license. You may select, at your option, one of the above-listed licenses. The commercial license includes enterprise-grade tools, such as a multi-table consistent backup/restore tool for Cassandra. Regarding the commercial license, please [contact us](https://scalar-labs.com/contact_us/) for more information.
