# Requirements and Recommendations in the Underlying Databases of ScalarDB

This document explains the requirements and recommendations in the underlying databases of ScalarDB to make ScalarDB applications work correctly.

## Cassandra or Cassandra-compatible database requirements

The following are requirements to make ScalarDB on Cassandra or Cassandra-compatible databases work properly and for storage operations with `LINEARIZABLE` to provide linearizablity and for transaction operations with `SERIALIZABLE` to provide strict serializability.

### Ensure durability in Cassandra

In **cassandra.yaml**, you must change `commitlog_sync` from the default `periodic` to `batch` or `group` to ensure durability in Cassandra.

ScalarDB provides only the atomicity and isolation properties of ACID and requests the underlying databases to provide durability. Although you can specify `periodic`, we do not recommend doing so unless you know exactly what you are doing.

### Confirm that the Cassandra-compatible database supports lightweight transactions (LWTs)

You must use a Cassandra-compatible database that supports LWTs.

ScalarDB does not work on some Cassandra-compatible databases that do not support LWTs, such as [Amazon Keyspaces](https://aws.amazon.com/keyspaces/). This is because the Consensus Commit transaction manager relies on the linearizable operations of underlying databases to make transactions serializable.

## JDBC database recommendations

In ScalarDB on JDBC databases, you can't choose a consistency level (`LINEARIZABLE`, `SEQUENTIAL` or `EVENTUAL`) in your code by using the `Operation.withConsistency()` method. In addition, the consistency level depends on the setup of your JDBC database. 

For example, if you have asynchronous read replicas in your setup and perform read operations against them, the consistency will be eventual because you can read stale data from the read replicas. On the other hand, if you perform all operations against a single master instance, the consistency will be linearizable. 

We recommend performing all operations or transactions against a single master instance so that you can achieve linearizability and avoid worrying about consistency issues in your application. Note that you can still use a read replica as a backup and standby even if you follow this recommendation.

If you must perform operations or transactions on read replicas, you can configure your application to perform read-write-mixed transactions against a single master instance and read-only transactions against read replicas. The resulting configuration would not be strict serializable (linearizable and serializable) but would be serializable.
