# A Guide on How to Configure Underlining Databases for Scalar DB

This document sets out some guidelines for configuring the underlining databases of Scalar DB to make Scalar DB applications work correctly.

## Cassandra (or Cassandra compatible databases)

Here are the requirements to make Scalar DB on Cassandra (or Cassandra compatible databases) work properly.

1. Durability is provided
   1. In Cassandra, `commitlog_sync` in cassandra.yaml must be changed to `batch` or `group` from the default `periodic`.
2. (For Cassandra compatible databases) LWT is supported

For the first, it is required since Scalar DB only provides Atomicity and Isolation properties of ACID and requests the underlining databases to provide Durability.
You can stil use `periodic`, but it is not recommended unless you know exactly what you are doing.

For the second, Scalar DB does not work on some Cassandra compatible databases such as [Amazon Keyspaces](https://aws.amazon.com/keyspaces/) that don't support LWT since `ConsensusCommit` transaction manager relies on linearizable operations of underlining databases to make transactions Serializable. 

With the above configurations, storage operations with `LINEARIZABLE` can provide Linearizablity and transaction operations with `SERIALIZABLE` can provide strict Serializability.