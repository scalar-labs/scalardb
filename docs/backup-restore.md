# How to Backup and Restore Data in Scalar DB

Since Scalar DB provides transaction capability on top of non-transactional (possibly transactional) databases non-invasively, you need to take special care of backing up and restoring the databases in a transactionally-consistent way. 
This document sets out some guidelines for backing up and restoring the databases that Scalar DB supports.

## Create Backup

### For Transactional Databases

#### JDBC databases

You can take a backup with your favorite way for JDBC databases.
One requirement for backup in Scalar DB on JDBC databases is that backups for all the Scalar DB managed tables (including the coordinator table) need to be transactionally-consistent or automatically recoverable to a transactionally-consistent state.
That means that you need to create a consistent snapshot by dumping all tables in a single transaction.
For example, you can use the `mysqldump` command with `--single-transaction` option in MySQL and the `pg_dump` command in PostgreSQL to achieve that.
Or when you use Amazon RDS (Relational Database Service) or Azure Database for MySQL/PostgreSQL, you can restore to any point within the backup retention period with the automated backup feature, which satisfies the requirement.

### For Non-transactional Databases

#### Basic strategy to create a transactionally-consistent backup

One way to create a transactionally-consistent backup is to take a backup while Scalar DB cluster does not have outstanding transactions.
If an underlying database supports a point-in-time snapshot/backup mechanism, you can take a snapshot during the period.
If an underlying database supports a point-in-time restore/recovery mechanism, you can set a restore point to a time (preferably the midtime) in the period.

To easily make Scalar DB drain outstanding requests and stop accepting new requests, it is recommended to use [Scalar DB server](https://github.com/scalar-labs/scalardb/tree/master/server) (which implements `scalar-admin` interface) or implement the [scalar-admin](https://github.com/scalar-labs/scalar-admin) interface properly in your Scalar DB applications.
With [scalar-admin client tool](https://github.com/scalar-labs/scalar-admin/tree/scalar-admin-dockerfile#client-side-tool), you can pause nodes/servers/applications that implement the scalar-admin interface without losing ongoing transactions.

Note that when you use a point-in-time-restore/recovery mechanism, it is recommended to minimize the clock drifts between clients and servers by using clock synchronization such as NTP.
Otherwise, the time you get as a paused duration might be too different from the time in which the pause was actually conducted, which could restore to a point where ongoing transactions exist.
Also, it is recommended to pause a long enough time (e.g., 10 seconds) and use the midpoint time of the paused duration as a restore point since clock synchronization cannot perfectly synchronize clocks between nodes.

#### Database-specific ways to create a transactionally-consistent backup   

**Cassandra**

Cassandra has a built-in replication mechanism, so you do not always have to create a transactionally-consistent backup.

For example, if replication is properly set to 3 and only the data of one of the nodes in a cluster is lost, you do not need a transactionally-consistent backup because the node can be recovered with a normal (transactionally-inconsistent) snapshot and the repair mechanism.
However, if the quorum of nodes of a cluster loses their data, we need a transactionally-consistent backup to restore the cluster to a certain transactionally-consistent point.

If you want to create a transactionally-consistent cluster-wide backup, pause the application and take the snapshots of nodes as described in [the basic strategy](#basic-strategy-to-create-a-transactionally-consistent-backup), or 
stop the Cassandra cluster and take the copies of all the nodes of the cluster, and start the cluster. 

To avoid mistakes when doing backup operations, it is recommended to use [Cassy](https://github.com/scalar-labs/cassy).
Cassy is also integrated with `scalar-admin` so it can issue a pause request to the Scalar DB application of a Cassandra cluster.
Please see [the doc](https://github.com/scalar-labs/cassy/blob/master/docs/getting-started.md#take-cluster-wide-consistent-backups) for more details.

**Cosmos DB**

You must create a Cosmos DB account with a Continuous backup policy enabled to create point-in-time restore (PITR). Backups are created continuously after enabling this.
To specify a transactionally-consistent restore point, please pause the Scalar DB application of a Cosmos DB as described in [the basic strategy](#basic-strategy-to-create-a-transactionally-consistent-backup).

**DynamoDB**

You must enable the point-in-time recovery (PITR) feature for DynamoDB tables.
To specify a transactionally-consistent restore point, please pause the Scalar DB application of a DynamoDB as described in [the basic strategy](#basic-strategy-to-create-a-transactionally-consistent-backup).
Note that you need to reconfigure restored tables since the configurations like Stream settings, Time To Live settings, PITR settings, tags, AWS Identity and Access Management (IAM) policies,  Amazon CloudWatch metrics and alarms, and auto scaling policies are not copied to the restored table.
Scalar DB Schema Loader enables PITR and auto-scaling by default.

## Restore Backup

### JDBC databases

You can restore the backup with your favorite way for JDBC databases.
Please refer the [documentation of MySQL](https://dev.mysql.com/doc/mysql-backup-excerpt/8.0/en/reloading-sql-format-dumps.html) and [documentation of PostgreSQL](https://www.postgresql.org/docs/8.1/backup.html#BACKUP-DUMP-RESTORE) for restoring backups in MySQL and PostgreSQL respectively.
If you use Amazon RDS (Relational Database Service) or Azure Database for MySQL/PostgreSQL,
you can restore to any point within the backup retention period with the automated backup feature.

### Cassandra

To restore the backup, you should stop the Cassandra and clean the `data`, `commitlogs`, and `hints` directories then restore the backup copies of all the nodes of the cluster, and start the cluster.

To avoid mistakes when doing restore operations, it is recommended to use [Cassy](https://github.com/scalar-labs/cassy).
You can restore to any point using `CLUSTER-ID` and `SNAPSHOT-ID`.
Please see [the doc](https://github.com/scalar-labs/cassy/blob/master/docs/getting-started.md#take-cluster-wide-consistent-backups) for more details.

### Cosmos DB

To restore a backup, please follow the [azure official guide](https://docs.microsoft.com/en-us/azure/cosmos-db/restore-account-continuous-backup#restore-account-portal) and change the default consistency to `STRONG` after restoring the backup.
It is recommended to use the midtime of paused duration as a restore point as we explained earlier.

### DynamoDB

You can restore the table to a point in time using the DynamoDB console or the AWS Command Line Interface (AWS CLI). The point-in-time recovery process restores to a new table.
Tables can only be restored one by one.

You can restore tables from the [Amazon DynamoDB console](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/PointInTimeRecovery.Tutorial.html) using the following steps.

* Select the midtime of paused duration as the restore point.
* Restore with PITR of table A to another table B
* Take a backup of the restored table B (assume the backup is named backup B)
* Remove table A
* Create a table named A with backup B

Note that you need to follow the above steps because it assumes the application requires tables with the same names as before. In DynamoDB, a table can only be restored with an alias, so you can restore the table with an alias and delete the original table and rename the alias to the original name.

If you want to restore multiple tables with a single command, you can create a script to restore multiple tables using the AWS CLI commands.

Configurations like Stream settings, Time To Live settings, PITR settings, tags, AWS Identity and Access Management (IAM) policies,  Amazon CloudWatch metrics and alarms, and auto scaling policies are not copied to the restored table.
You must update these configurations as required after restoring the table; thus, please enable point-in-time recovery (PITR) for continuous backup creation.

You can re-apply schemas with the schema loader because it enables features like  PITR and auto-scaling.
_Don't worry the schema loader only sets the missing configurations and doesn't recreate the schemas if the tables exist._