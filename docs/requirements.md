# Requirements and Recommendations for the Underlying Databases of ScalarDB

This document explains the requirements and recommendations in the underlying databases of ScalarDB to make ScalarDB applications work correctly.

## Common requirements

### Privileges to access the underlying databases

ScalarDB operates the underlying databases not only for CRUD operations but also for creating/altering schema, tables, indexes, and so on. Thus, ScalarDB requires a fully privileged account to access the underlying databases.

## Cassandra or Cassandra-compatible database requirements

The following are requirements to make ScalarDB on Cassandra or Cassandra-compatible databases work properly and for storage operations with `LINEARIZABLE` to provide linearizability and for transaction operations with `SERIALIZABLE` to provide strict serializability.

### Ensure durability in Cassandra

In **cassandra.yaml**, you must change `commitlog_sync` from the default `periodic` to `batch` or `group` to ensure durability in Cassandra.

ScalarDB provides only the atomicity and isolation properties of ACID and requests the underlying databases to provide durability. Although you can specify `periodic`, we do not recommend doing so unless you know exactly what you are doing.

### Confirm that the Cassandra-compatible database supports lightweight transactions (LWTs)

You must use a Cassandra-compatible database that supports LWTs.

ScalarDB does not work on some Cassandra-compatible databases that do not support LWTs, such as [Amazon Keyspaces](https://aws.amazon.com/keyspaces/). This is because the Consensus Commit transaction manager relies on the linearizable operations of underlying databases to make transactions serializable.

## CosmosDB database requirements

In your Azure CosmosDB account, you must set the **default consistency level** to **Strong**.

Consensus Commit, the ScalarDB transaction protocol, requires linearizable reads. By setting the **default consistency level** to **Strong**, CosmosDB can guarantee linearizability.

For instructions on how to configure this setting, see the official documentation at [Configure the default consistency level](https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/how-to-manage-consistency#configure-the-default-consistency-level).

## JDBC database recommendations

In ScalarDB on JDBC databases, you can't choose a consistency level (`LINEARIZABLE`, `SEQUENTIAL` or `EVENTUAL`) in your code by using the `Operation.withConsistency()` method. In addition, the consistency level depends on the setup of your JDBC database.

For example, if you have asynchronous read replicas in your setup and perform read operations against them, the consistency will be eventual because you can read stale data from the read replicas. On the other hand, if you perform all operations against a single master instance, the consistency will be linearizable.

With this in mind, you must perform all operations or transactions against a single master instance so that you can achieve linearizability and avoid worrying about consistency issues in your application. In other words, ScalarDB does not support read replicas. 

{% capture notice--info %}
**Note**

You can still use a read replica as a backup and standby even when following this guideline.
{% endcapture %}

<div class="notice--info">{{ notice--info | markdownify }}</div>
