# Requirements in the Underlying Databases of ScalarDB

This document explains the requirements in the underlying databases of ScalarDB to make ScalarDB applications work correctly.

## Cassandra (or Cassandra compatible databases)

Here are the requirements to make ScalarDB on Cassandra (or Cassandra compatible databases) work properly.

1. Durability is provided.
   1. In Cassandra, `commitlog_sync` in cassandra.yaml must be changed to `batch` or `group` from the default `periodic`.
2. (For Cassandra-compatible databases) Lightweight transactions (LWTs) are supported.

The first requirement is necessary because ScalarDB provides only atomicity and isolation properties of ACID and requests the underlying databases to provide durability.
You can still use `periodic`, but we do not recommend using this you know exactly what you are doing.

The second requirement is necessary because ScalarDB does not work on some Cassandra-compatible databases, such as [Amazon Keyspaces](https://aws.amazon.com/keyspaces/), that do not support LWTs. This is because the Consensus Commit transaction manager relies on the linearizable operations of underlying databases to make transactions serializable.

If the above requirements are met, storage operations with `LINEARIZABLE` can provide linearizablity and transaction operations with `SERIALIZABLE` can provide strict serializability.

## JDBC databases

In ScalarDB on JDBC databases, you can't choose a consistency level (`LINEARIZABLE`, `SEQUENTIAL` or `EVENTUAL`) in your code with the `Operation.withConsistency()` method, and the consistency level depends on the setup of your JDBC database.
For example, when you have asynchronous read replicas in your setup and perform read operations against them, the consistency will be eventual because you can read stale data from the read replicas.
On the other hand, when you perform all operations against a single master instance, the consistency will be linearizable.
We recommend performing all operations/transactions against a single master instance (so that you can achieve linearizable) if you want to avoid caring consistency issues in your applications. Note that you can still use a read replica as a backup and standby even if you follow the recommendation.

If you strongly want to perform operations/transactions on read replicas, you can configure your application to perform read-write-mixed transactions against a master instance and read-only transactions against read replicas.
The resulting schedule would not be strict serializable (linearizable and serializable) but would be serializable.
