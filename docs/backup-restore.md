# How to Backup and Restore Data in Scalar DB

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

#### Database-specific ways to create a transactionally-consistent backup   

**Cassandra**

Cassandra has a built-in replication mechanism, so you do not always have to create a transactionally-consistent backup.

For example, if replication is properly set to 3 and only the data of one of the nodes in a cluster is lost, you do not need a transactionally-consistent backup because the node can be recovered with a normal (transactionally-inconsistent) snapshot and the repair mechanism.
However, if the quorum of nodes of a cluster loses their data, we need a transactionally-consistent backup to restore the cluster to a certain transactionally-consistent point.

If you want to create a transactionally-consistent cluster-wide backup, please follow [the basic strategy](#general-way-to-create-a-transactionally-consistent-backup) section, or 
stop the Cassandra cluster and take the copies of all the nodes of the cluster, and start the cluster. 

To avoid mistakes when doing backup operations, it is recommended to use [Cassy](https://github.com/scalar-labs/cassy).
Cassy is also integrated with `scalar-admin` so it can issue a pause request to the application of a Cassandra cluster.
Please see [the doc](https://github.com/scalar-labs/cassy/blob/master/docs/getting-started.md#take-cluster-wide-consistent-backups) for more details.

**Cosmos DB**

You must create a Cosmos DB account with a Continuous backup policy to create point-in-time restore (PITR).
Please follow [the basic strategy](#general-way-to-create-a-transactionally-consistent-backup) section to create a backup.

**DynamoDB**

You must create tables with point-in-time recovery (PITR) and auto-scaling in DynamoDB. Scalar DB Schema Loader enables PITR and auto-scaling by default.
Please follow [the basic strategy](#general-way-to-create-a-transactionally-consistent-backup) section to create a backup.

#### Basic strategy to create a transactionally-consistent backup

The best way to take a transactionally-consistent backup for Scalar DB on a non-transactional database is to use the [Scalar DB server](https://github.com/scalar-labs/scalardb/tree/master/server) or implement the [scalar-admin](https://github.com/scalar-labs/scalar-admin) interface properly in your application,
you can easily pause the application without losing ongoing transactions.

One way to create a transactionally-consistent backup is to take a backup while Scalar DB cluster does not have outstanding transactions. 
If an underlying database supports a point-in-time snapshot/backup mechanism, you can take a snapshot during the period.
If an underlying database supports a point-in-time restore/recovery mechanism, you can set a restore point to a specific time (preferably the midtime) in the period since the system takes backups for each operation in such a case.

You must pause for a long enough time (e.g., 10 seconds) to create a backup and use the midtime of the pause as a restore point.

## Restore Backup

### JDBC databases

You can restore the backup with your favorite way for JDBC databases.
You can use `mysql` command in MySQL and `psql` command in PostgreSQL to achieve that.
If you use Amazon RDS (Relational Database Service) or Azure Database for MySQL/PostgreSQL,
you can restore to any point within the backup retention period with the automated backup feature.

### Cassandra

To restore the backup, you should stop the Cassandra and clean the `data`, `commitlogs`, and `hints` directories then restores the backup copies of all the nodes of the cluster, and start the cluster.

To avoid mistakes when doing restore operations, it is recommended to use [Cassy](https://github.com/scalar-labs/cassy).
You can restore to any point using `CLUSTER-ID` and `SNAPSHOT-ID`.
Please see [the doc](https://github.com/scalar-labs/cassy/blob/master/docs/getting-started.md#take-cluster-wide-consistent-backups) for more details.

### Cosmos DB

To restore a backup, please follow the [azure official guide](https://docs.microsoft.com/en-us/azure/cosmos-db/restore-account-continuous-backup#restore-account-portal) and change the default consistency to `STRONG` after restoring the backup.
It is highly recommended to use the midtime of the pause as a restore point as we explained earlier.

### DynamoDB

You can restore the table to a point in time using the DynamoDB console or the AWS Command Line Interface (AWS CLI). The point-in-time recovery process restores to a new table.
By default, the table can only be restored one by one.

You can restore tables one by one from the [Amazon DynamoDB console](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/PointInTimeRecovery.Tutorial.html) using the following steps and use the midtime of the pause as a restore point

* Restore with PITR of table A to another table B
* Take a backup of the restored table B (assume the backup is named backup B)
* Remove table A
* Create a table named A with backup B

If you want to restore multiple tables with a single command, you can create a script to restore multiple tables using the AWS CLI commands.

It is recommended to enable point-in-time recovery (PITR) for continuous backup creation and auto-scaling for better performance. Auto-scaling dynamically adjusts provisioned throughput capacity on your behalf in response to actual traffic patterns.
DynamoDB will not enable PITR and auto-scaling by default after restoring.

You can enable continuous backup and auto-scaling using the schema loader or Amazon DynamoDB console. Configuring the continuous backup and auto-scaling is easier if you use the schema loader.
Don't worry the schema loader only sets the missing configurations and doesn't recreate the schemas if the tables exist.
