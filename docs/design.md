## Scalar DB v1 design document

## Introduction

Scalar DB v1 is a distributed storage abstraction and client-coordinated distributed transaction manager on top of the storage. This design document briefly explains its background, design and implementation.

## Background and Objectives

Distributed storage is widely adopted in real-world applications and recent open-source distributed storages such as Cassandra and HBase have accelerated the trend. They are particularly used by large and sometimes mission-critical applications because of their high performance, high availability and high scalability. However, they often lack transaction capability, which is particularly important in mission-critical applications. Transaction capability can be added to HBase via third-party libraries, but they tend to sacrifice some availability property due to the master-slave architecture. Some companies have ended up creating yet another distributed transactional database from scratch (such as CockroachDB and TiDB) to overcome such problem.

Scalar DB v1 is a simple and practical solution to solve the above mentioned problem in a different way. It provides a simple storage abstraction layer on the existing storage implementations and a storage-independent distributed transaction layer on top of the storage abstraction. So, it can fully utilize, not only battle-tested existing implementations, operational/management tools and good properties of storages, but also the eco-system, the best practices and the community which have grown for a long time.

## Design Goals

The primary design goals of Scalar DB are high availability, horizontal scalability and strong consistency for distributed storage and transaction operations. It aims to tolerate disk, machine, rack, and even data-center failures, with minimal performance degradation. It achieves these goals with an unbundled transaction layer [1] with easy and unified API so that the underlining storage implementations can be replaced with others without application code change. The performance of the Scalar DB is highly dependent on the underlining storage performance, and is usually slower than other scratch-built distributed databases since it adds a storage abstraction layer and storage-oblivious transaction layer.

## High-level Architecture

Scalar DB is composed of distributed storage abstraction layer and client-coordinated distributed transaction manager.
Distributed storage abstraction has storage implementation specific adapters such as Cassandra adapter.
Another storage implementation can be added by creating an adapter which follows the abstraction interface.

<p align="center">
<img src="./images/software_stack.png" width="440" />
</p>

## Data Model

The data model of Scalar DB is a multi-dimensional map based on the key-value data model. A logical record is composed of partition-key, clustering-key and a set of values. The value is uniquely mapped by a primary key composed of partition-key, clustering-key and value-name as described in the following scheme.

(partition-key, clustering-key, value-name) -> value-content


<p align="center">
<img src="./images/data_model.png" width="480" />
</p>

### Physical Data Model

Scalar DB is a multi-dimensional map distributed to multiple nodes by key-based hash partitioning.
Records are assumed to be hash-partitioned by partition-key (even though an underlining implementation may support range partitioning).
Records with the same partition-key define a partition. Partitions are then sorted by the clustering-key.
It is similar to Google BigTable [2] but it differs in clustering-key structure and partitioning scheme.

### Limitation of the data model

Since records in Scalar DB are assumed to be hash-partitioned, global range scan is not supported.
Range scan is only supported for clustering-key access within the same partition.

## Implementation

### Storage

As of writing this, Scalar DB supports only Cassandra storage as a storage implementation. More correctly, it supports Cassandra java-driver API. Thus Cassandra java-driver compatible storage systems, such as ScyllaDB and Azure Cosmos DB, can potentially also be used. The storage abstraction assumes the following features/properties, which most recent distributed storages have:
- Atomic CRUD operations (each single-record operation needs to be atomic)
- Sequential consistency support
- Atomic/Linearlizable conditional mutation (Create/Update/Delete)
- Ability to include user-defined meta-data for each record

Please see the javadoc for more details and usage.

### Transaction

Scalar DB executes transactions in a fully client-coordinated way so that it can do master-less transactions, which achieves almost linear scalability and high availability (especially when it is integrated with master-less Cassandra).
It basically follows the Cherry Garcia protocol propsed in [3]. More specifically, Scalar DB achieves scalable distributed transaction by utilizing atomic conditional mutation for managing transaction state and storing WAL (Write-Ahead-Logging) records in distributed fashion in each record by using meta-data ability.
It also has some similarity to paxos-commit [4].

Please see the javadoc for more details and usage.

## Future Work

* Support Hbase for another storage implementation for more performance.
* Utilize deterministic nature of [Scalar DL](https://github.com/scalar-labs/scalardl) (ledger middleware used for storing ledger information with Scalar DB) to avoid heavy-weight global consensus for linearizability and serializablity.

## References

- [1] D. B. Lomet, A. Fekete, G. Weikum, and M. J. Zwilling.  Unbundling Transaction Services in the Cloud. In CIDR, Jan. 2009.
- [2] Fay Chang , Jeffrey Dean , Sanjay Ghemawat , Wilson C. Hsieh , Deborah A. Wallach , Mike Burrows , Tushar Chandra , Andrew Fikes , Robert E. Gruber, Bigtable: A Distributed Storage System for Structured Data, ACM Transactions on Computer Systems (TOCS), v.26 n.2, p.1-26, June 2008.
- [3] A. Dey, A. Fekete, U. Röhm, "Scalable Distributed Transactions across Heterogeneous Stores", IEEE 31th International Conference on Data Engineering (ICDE), 2015.
- [4] Jim Gray , Leslie Lamport, Consensus on transaction commit, ACM Transactions on Database Systems (TODS), v.31 n.1, p.133-160, March 2006.
