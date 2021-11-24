# How to Backup and Restore Databases Integrated with Scalar DB

Since Scalar DB provides transaction capability on top of non-transactional (possibly transactional) databases non-invasively, you need to take special care of backing up and restoring the databases in a transactionally-consistent way. 
This document sets out some guidelines for backing up and restoring the databases that Scalar DB supports.

## Create Backup

### For Transactional Databases

#### JDBC databases

You can take a backup with your favorite way for JDBC databases.
One requirement for backup in Scalar DB on JDBC databases is that backups for all the Scalar DB managed tables (including the coordinator table) need to be transactionally-consistent or automatically recoverable to a transactionally-consistent state.
That means that you need to create a consistent snapshot by dumping all tables in a single transaction.
For example, you can use `mysqldump` command with `--single-transaction` option in MySQL and `pg_dump` command in PostgreSQL to achieve that.
Or when you use Amazon RDS (Relational Database Service) or Azure Database for MySQL/PostgreSQL, you can restore to any point within the backup retention period with the automated backup feature, which satisfies the requirement.

### For Non-transactional Databases

#### General strategy to create a transactionally-consistent backup

The best way to take a transactionally-consistent backup for Scalar DB on a non-transactional database is to use the [Scalar DB server](https://github.com/scalar-labs/scalardb/tree/master/server) or implement the [scalar-admin](https://github.com/scalar-labs/scalar-admin) interface properly in your application, you can easily pause the application without losing ongoing transactions.

You must pause for a long enough time (e.g., 10 seconds) to create a backup and use the midtime of the pause as a restore point.

#### Database specific strategies to create a transactionally-consistent backup

##### Cassandra

Cassandra has a built-in replication mechanism, so you do not always have to create a transactionally-consistent backup.

For example, if replication is properly set to 3 and only the data of one of the nodes in a cluster is lost, you do not need a transactionally-consistent backup because the node can be recovered with a normal (transactionally-inconsistent) snapshot and the repair mechanism. However, if the quorum of nodes of a cluster loses their data, we need a transactionally-consistent backup to restore the cluster to a certain transactionally-consistent point.

If you want to create a transactionally-consistent cluster-wide backup, please follow the [General strategy to create a transactionally-consistent backup](#general-strategy-to-create-a-transactionally-consistent-backup) section, or 
stop the Cassandra cluster and take the snapshots of all the nodes of the cluster, and start the cluster. 

To minimize mistakes when doing backup operations, it is recommended to use [Cassy](https://github.com/scalar-labs/cassy).
Cassy is also integrated with `scalar-admin` so it can issue a pause request to the application of a Cassandra cluster.
Please see [the doc](https://github.com/scalar-labs/cassy/blob/master/docs/getting-started.md#take-cluster-wide-consistent-backups) for more details.

##### Cosmos DB

You must create a Cosmos DB account with a Continuous backup policy to create point-in-time recovery (PITR). Please follow the [General strategy to create a transactionally-consistent backup](#general-strategy-to-create-a-transactionally-consistent-backup) section to create a backup.

##### DynamoDB

You must create tables with point-in-time recovery (PITR) and autoscaling in DynamoDB, scalardb schema Loader enables PITR and autoscaling by default. Please follow the [General strategy to create a transactionally-consistent backup](#general-strategy-to-create-a-transactionally-consistent-backup) section to create a backup.


## Restore Backup

This section shows how to restore transactionally-consistent backup for Scalar DL. You must restore Scalar Ledger and Auditor tables with the same restore point if you use Ledger and Auditor.

### JDBC Databases

You can restore the backup with your favorite way for JDBC databases.
You can use `mysql` command in MySQL and `psql` command in PostgreSQL to achieve that. 
If you use Amazon RDS (Relational Database Service) or Azure Database for MySQL/PostgreSQL, 
you can restore to any point within the backup retention period with the automated backup feature.

### Cassandra

To minimize mistakes when doing restore operations, it is recommended to use [Cassy](https://github.com/scalar-labs/cassy).
You can restore to any point using `CLUSTER-ID` and `SNAPSHOT-ID`.
Please see [the doc](https://github.com/scalar-labs/cassy/blob/master/docs/getting-started.md#take-cluster-wide-consistent-backups) for more details.

### Cosmos DB

To restore a backup, you must follow the [azure official guide](https://docs.microsoft.com/en-us/azure/cosmos-db/restore-account-continuous-backup#restore-account-portal) and change the default consistency to `STRONG` after restoring the backup.
You must use the midtime of the pause as a restore point.

### DynamoDB

You must restore tables one by one from the Amazon DynamoDB console using the following steps and use the midtime of the pause as a restore point

* Restore with PITR of table A to another table B (alias table)
* Take a backup of the restored table B (say, backup B)
* Remove table A
* Create a table named A with backup B

You must enable continuous backup and auto-scaling using the scalardb schema loader or Amazon DynamoDB console. The scalardb schema loader doesn't remake the existing tables.
