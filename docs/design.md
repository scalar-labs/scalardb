# Scalar DB v1 design document

## Introduction

Scalar DB v1 is a distributed storage abstraction and client-coordinated distributed transaction on top of the storage. This design document briefly explains its background, design and implementation.

## Background and Objectives

Distributed storage is widely adopted in real-world applications and recent open-source distributed storages such as Cassandra and HBase accelerates the trend. They are particularly accepted by large and sometimes mission-critical applications because of its high performance, high availability and high scalability. However, they are mostly lack of transaction capability, which are particularly important in mission-critical applications. HBase has the transaction capability by third-party libraries, but they tend to be a single point of failure due to master-slave architecture, thus it sacrifices some availability property of the storage. Also, distributed storages are so diverge per implementation, and some of them are getting too complex to meet various user requirements. It's sometimes a big headache for engineers to use and manage such storage systems. Some engineers or companies ended up with creating yet another distributed transactional database (such as CockroachDB and TiDB) from scratch to overcome such problems.

Scalar DB v1 is a simple and practical solution to solve the above mentioned problems in a little different way. It provides a storage abstraction layer on the existing storage implementations and storage-independent distributed transaction layer on top of the storage abstraction. So, it can fully utilizes not only battle-tested existing implementations, tools and good properties of storages, but also the eco system and the community which have been grown for a long time.

## Design Goals

The primary design goals of Scalar DB v1 are high availability, horizontal scalability and strong consistency for distributed storage and transaction operations. It aims to tolerate disk, machine, rack, and even data-center failures with minimal performance degradation. It achieves the goals with unbundled transaction layer [1] with easy and unified API so the underlining storage implementations can be replaced with others without application code change. The performance of the Scalar DB is highly dependent on the underlining storage performance, and is usually slower than other scratch-built distributed databases since it adds a storage abstraction layer and storage-oblivious transaction layer. 

## High-level Architecture

- TODO : diagram

## Data Model

- TODO : diagram

The data model of Scalar DB v1 is a multi-dimensional map based on key-value data model. A logical record is composed of partition-key, clustering-key and a set of values. The value is uniquely mapped by a primary key composed of partition-key, clustering-key and value-name as described in the following scheme.

(partition-key, clustering-key, value-name) -> value-content

### Physical Data Model

Scalar DB v1 is a multi-dimensional map distributed to multiple nodes by key-based hash partitioning.
Records are assumed to be hash-partitioned by partition-key (even though an underlining implementation supports range partitioning).
Records with the same partition-key, which is called a partition, are sorted by clustering-key.
It is similar to Google BigTable[2] but it differs in clustering-key structure and partitioning scheme.

### Limitation of the data model

Since records in Scalar DB v1 are assumed to be hash-partitioned, global range scan is not supported.
Range scan is only supported for clustering-key access within the same partition.

## Implementation

### Storage

Scalar DB supports only Cassandra storage as a storage implementation in v1.0. More correctly, it supports Cassandra java-driver API. Thus Cassandra java-driver compatible storage system such as ScyllaDB can also be used potentially.
The storage abstraction assumes the following features/properties, which most of recent distributed storages have:
- Atomic CRUD operation (Each single-record operation needs to be atomic)
- Sequential consistency support
- Atomic/Linearlizable conditional mutation (CUD)
- Ability to include user-defined meta-data for each record

Please see the javadoc for more detail and the usage.

### Transaction

It basically follows Cherry Garicia protocol[3] to achieve highly-available and scalable transaction on top of storage, in other words, paxos-commit[4] like transaction management.
More specifically, it achieves scalable distributed transaction by utilizing atomic conditional mutation for managing transaction state and storing WAL (Write-Ahead-Logging) records in distributed fashion in each record by using meta-data ability.

Please see the javadoc for more detail and the usage.

## Future Work

* It might support Hbase for another storage implementation for more performance.
* It might utlize deterministic nature of Scalar DL (ledger middleware used for stroing ledger information with Scalar DB) to avoid heavy-wight global consensus for linearizability and serializablity.

## References

- [1] D. B. Lomet, A. Fekete, G. Weikum, and M. J. Zwilling.  Unbundling Transaction Services in the Cloud. In CIDR, Jan. 2009.
- [2] Fay Chang , Jeffrey Dean , Sanjay Ghemawat , Wilson C. Hsieh , Deborah A. Wallach , Mike Burrows , Tushar Chandra , Andrew Fikes , Robert E. Gruber, Bigtable: A Distributed Storage System for Structured Data, ACM Transactions on Computer Systems (TOCS), v.26 n.2, p.1-26, June 2008.
- [3] A. Dey, A. Fekete, U. Röhm, "Scalable Distributed Transactions across Heterogeneous Stores", IEEE 31th International Conference on Data Engineering (ICDE), 2015.
- [4] Jim Gray , Leslie Lamport, Consensus on transaction commit, ACM Transactions on Database Systems (TODS), v.31 n.1, p.133-160, March 2006.
