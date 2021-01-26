# A Guide on How to Back up and Restore Databases Integrated with Scalar DB

Since Scalar DB provides transaction capability on top of non-transactional (possibly transactional) databases non-invasively, you need to take special care of backing up and restoring the databases in a transactionally-consistent way.
This document sets out some guidelines for backing up and restoring the databases that Scalar DB supports.

## Cassandra

Since Cassandra has a built-in replication mechanism, we don't always need a transactionally-consistent backup to recover a Cassandra cluster.

For example, if replication is properly set to 3 and only the data of one of the nodes in a cluster is lost, we don't need a transactionally-consistent backup because the node can be recovered with a normal (transactionally-inconsistent) snapshot and the repair mechanism.
However, if the quorum of nodes of a cluster loses their data, we need a transactionally-consistent backup to restore the cluster to a certain transactionally-consistent point.

The easiest way to take a transactionally-consistent backup for Scalar DB on Cassandra is to use [Cassy](https://github.com/scalar-labs/cassy).
Cassy takes snapshots of a Cassandra cluster after it pauses the application cluster to make the resulting snapshots transactionally-consistent.
Please see [the doc](https://github.com/scalar-labs/cassy/blob/master/docs/getting-started.md#take-cluster-wide-consistent-backups) for more details.

Note that we can simply use the snapshots when restoring a cluster because they are just snapshots of Cassandra. In any case, it is recommended to use Cassy for restoration as well to minimize operational mistakes.
