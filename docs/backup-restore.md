# A Guide on How to Back up and Restore Databases Integrated with Scalar DB

Since Scalar DB provides transaction capability on top of non-transactional (possibly transactional) databases non-invasively, you need to take special care of backing up and restoring the databases in a transactionally-consistent way.
This document sets out some guidelines for backing up and restoring the databases that Scalar DB supports.

## Scalar DB server

Since Scalar DB server implements [scalar-admin](https://github.com/scalar-labs/scalar-admin) interface, you can easily pause the server to stop accepting incoming requests and drain in-flight requests.
You can use paused duration and choose one of the following ways depending on the underlining database to make your databases transactionally-consistent even after restored.
* Take backups of your databases during the paused duration.
* Restore to a point in the paused duration with point-in-time-restore (PITR) feature.

## How To Create Transactionally-Consistent Backup

The easiest way to take a transactionally-consistent backup for Scalar DB on a non-transactional database is to use the [Scalar DB server](https://github.com/scalar-labs/scalardb/tree/master/server) or implement the [scalar-admin](https://github.com/scalar-labs/scalar-admin) interface properly in your application, you can easily pause the application without losing ongoing transactions.

You must pause for 10 seconds to create a backup and use the midtime of the pause as a restore point.

## Scalar DB Supported Databases

### Cassandra

Since Cassandra has a built-in replication mechanism, we don't always need a transactionally-consistent backup to recover a Cassandra cluster.

For example, if replication is properly set to 3 and only the data of one of the nodes in a cluster is lost, we don't need a transactionally-consistent backup because the node can be recovered with a normal (transactionally-inconsistent) snapshot and the repair mechanism.
However, if the quorum of nodes of a cluster loses their data, we need a transactionally-consistent backup to restore the cluster to a certain transactionally-consistent point.

Follow [How to create transactionally-consistent backup](#how-to-create-transactionally-consistent-backup) to create a transactionally-consistent backup.

Another way to take a transactionally-consistent backup for Scalar DB on Cassandra is to stop a cluster, take the snapshots of all the nodes of the cluster, and start the cluster. 

To minimize mistakes when doing backup and restore operations, it is recommended to use [Cassy](https://github.com/scalar-labs/cassy).
Cassy is also integrated with `scalar-admin` so it can issue a pause request to the application of a Cassandra cluster.
Please see [the doc](https://github.com/scalar-labs/cassy/blob/master/docs/getting-started.md#take-cluster-wide-consistent-backups) for more details.

### Cosmos DB

You should create Cosmos DB account using `Continuous` backup policy to create continuous backups.

Follow [How to create transactionally-consistent backup](#how-to-create-transactionally-consistent-backup) to create a transactionally-consistent backup.

You can restore the backup based on the [azure official guide](https://docs.microsoft.com/en-us/azure/cosmos-db/restore-account-continuous-backup#restore-account-portal) and you must change the default consistency to `STRONG` after restoring the data.

### DynamoDB

You should create the schema using the scalardb schema loader, which enables Point-In-Time-Recovery (PITR) for each table in DynamoDB. It will create continuous backups.

Follow [How to create transactionally-consistent backup](#how-to-create-transactionally-consistent-backup) to create a transactionally-consistent backup.

You can restore tables one by one from the Amazon DynamoDB console using the following steps,

* Restore with PITR of table A to another table B (alias table)
* Take a backup of the restored table B (say, backup B)
* Remove table A
* Create a table named A with backup B

You must enable continuous backup and auto-scaling with scalardb schema loader or Amazon DynamoDB console. The scalardb schema loader doesn't remake the existing tables.

### JDBC databases

You can take a backup with your favorite way for JDBC databases.
One requirement for backup in Scalar DB on JDBC databases is that backups for all the Scalar DB managed tables (including the coordinator table) need to be transactionally-consistent or automatically recoverable to a transactionally-consistent state.
That means that you need to create a consistent snapshot by dumping all tables in a single transaction.
For example, you can use `mysqldump` command with `--single-transaction` option in MySQL and `pg_dump` command in PostgreSQL to achieve that.
Or when you use Amazon RDS (Relational Database Service) or Azure Database for MySQL/PostgreSQL, you can restore to any point within the backup retention period with the automated backup feature, which satisfies the requirement.
