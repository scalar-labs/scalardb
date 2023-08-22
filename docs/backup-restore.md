{% include end-of-support.html %}

# A Guide on How to Back up and Restore Databases Integrated with Scalar DB

Since Scalar DB provides transaction capability on top of non-transactional (possibly transactional) databases non-invasively, you need to take special care of backing up and restoring the databases in a transactionally-consistent way.
This document sets out some guidelines for backing up and restoring the databases that Scalar DB supports.

## Cassandra

Since Cassandra has a built-in replication mechanism, we don't always need a transactionally-consistent backup to recover a Cassandra cluster.

For example, if replication is properly set to 3 and only the data of one of the nodes in a cluster is lost, we don't need a transactionally-consistent backup because the node can be recovered with a normal (transactionally-inconsistent) snapshot and the repair mechanism.
However, if the quorum of nodes of a cluster loses their data, we need a transactionally-consistent backup to restore the cluster to a certain transactionally-consistent point.

The easiest way to take a transactionally-consistent backup for Scalar DB on Cassandra is to stop a cluster, take the snapshots of all the nodes of the cluster, and start the cluster. If you implement [scalar-admin](https://github.com/scalar-labs/scalar-admin) interface properly in your application, you can easily pause the application without losing on-going transactions.

To minimize mistakes when doing backup and restore operations, it is recommended to use [Cassy](https://github.com/scalar-labs/cassy).
Cassy is also integrated with `scalar-admin` so it can issue a pause request to the application of a Cassandra cluster.
Please see [the doc](https://github.com/scalar-labs/cassy/blob/master/docs/getting-started.md#take-cluster-wide-consistent-backups) for more details.

## JDBC databases

You can take a backup with your favorite way for JDBC databases.
One requirement for backup in Scalar DB on JDBC databases is that backups for all the Scalar DB managed tables (including the coordinator table) need to be transactionally-consistent or automatically recoverable to a transactionally-consistent state.
That means that you need to create a consistent snapshot by dumping all tables in a single transaction.
For example, you can use `mysqldump` command with `--single-transaction` option in MySQL and `pg_dump` command in PostgreSQL to achieve that.
Or when you use Amazon RDS (Relational Database Service) or Azure Database for MySQL/PostgreSQL, you can restore to any point within the backup retention period with the automated backup feature, which satisfies the requirement.

## Scalar DB server

Since Scalar DB server implements [scalar-admin](https://github.com/scalar-labs/scalar-admin) interface, you can easily pause the server to stop accepting incoming requests and drain in-flight requests.
You can use paused duration and choose one of the following ways depending on the underlining database to make your databases transactionally-consistent even after restored.
* Take backups of your databases during the paused duration with snapshot feature.
* Restore to a point in the paused duration with point-in-time-restore (PITR) feature.
