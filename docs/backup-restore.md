# A Guide on How to Backup and Restore Databases Used Through Scalar DB

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
If an underlying database supports a point-in-time restore/recovery mechanism, you can set a restore point to a time (preferably the mid-time) in the period.

To easily make Scalar DB drain outstanding requests and stop accepting new requests to create a pause duration, it is recommended to use [Scalar DB server](https://github.com/scalar-labs/scalardb/tree/master/server) (which implements `scalar-admin` interface) or implement the [scalar-admin](https://github.com/scalar-labs/scalar-admin) interface properly in your Scalar DB applications.
With [scalar-admin client tool](https://github.com/scalar-labs/scalar-admin/tree/scalar-admin-dockerfile#client-side-tool), you can pause nodes/servers/applications that implement the scalar-admin interface without losing ongoing transactions.

Note that when you use a point-in-time-restore/recovery mechanism, it is recommended to minimize the clock drifts between clients and servers by using clock synchronization such as NTP.
Otherwise, the time you get as a paused duration might be too different from the time in which the pause was actually conducted, which could restore to a point where ongoing transactions exist.
Also, it is recommended to pause a long enough time (e.g., 10 seconds) and use the mid-time of the paused duration as a restore point since clock synchronization cannot perfectly synchronize clocks between nodes.

#### Database-specific ways to create a transactionally-consistent backup   

**Cassandra**

Cassandra has a built-in replication mechanism, so you do not always have to create a transactionally-consistent backup.
For example, if the replication factor is set to 3 and only the data of one of the nodes in a cluster is lost, you do not need a transactionally-consistent backup because the node can be recovered with a normal (transactionally-inconsistent) snapshot and the repair mechanism.
However, if the quorum of nodes of a cluster loses their data, we need a transactionally-consistent backup to restore the cluster to a certain transactionally-consistent point.

If you want to create a transactionally-consistent cluster-wide backup, pause the Scalar DB application (or Scalar DB server) and take the snapshots of nodes as described in [the basic strategy](#basic-strategy-to-create-a-transactionally-consistent-backup), or stop the Cassandra cluster and take the copies of all the nodes' data, and start the cluster.

To avoid mistakes, it is recommended to use [Cassy](https://github.com/scalar-labs/cassy).
Cassy is also integrated with `scalar-admin` so it can issue a pause request to the Scalar DB application (or Scalar DB server) of a Cassandra cluster.
Please see [the doc](https://github.com/scalar-labs/cassy/blob/master/docs/getting-started.md#take-cluster-wide-consistent-backups) for more details.

**Cosmos DB**

You must create a Cosmos DB account with a continuous backup policy enabled to use point-in-time restore (PITR) feature. Backups are created continuously after it is enabled.
To specify a transactionally-consistent restore point, please pause the Scalar DB application of a Cosmos DB as described in [the basic strategy](#basic-strategy-to-create-a-transactionally-consistent-backup).

**DynamoDB**

You must enable the point-in-time recovery (PITR) feature for DynamoDB tables. If you use [Scalar DB Schema Loader](https://github.com/scalar-labs/scalardb/tree/master/schema-loader) for creating schema, it enables PITR feature for tables by default.
To specify a transactionally-consistent restore point, please pause the Scalar DB application of a DynamoDB as described in [the basic strategy](#basic-strategy-to-create-a-transactionally-consistent-backup).

## Restore Backup

### JDBC databases

You need to restore backups appropriately depending on how the backups are created.
For example, if you use MySQL and `mysqldump` to create a backup file, use `mysql` command to restore the file as specified [in the MySQL doc](https://dev.mysql.com/doc/mysql-backup-excerpt/8.0/en/reloading-sql-format-dumps.html). If you use PostgreSQL and `pg_dump` to create a backup file, use `psql` command to restore the file as specified in [the PostgreSQL doc](https://www.postgresql.org/docs/current/backup-dump.html#BACKUP-DUMP-RESTORE).
If you use Amazon RDS (Relational Database Service) or Azure Database for MySQL/PostgreSQL,
you can restore to any point within the backup retention period with the automated backup feature.

### Cassandra

You first need to stop all the nodes of a Cassandra cluster. Clean the directories (`data`, `commitlogs`, and `hints`) and place backups (snapshots) in each node. Then, start all the nodes.

To avoid mistakes, it is recommended to use [Cassy](https://github.com/scalar-labs/cassy).
Please see [the doc](https://github.com/scalar-labs/cassy/blob/master/docs/getting-started.md#take-cluster-wide-consistent-backups) for more details.

### Cosmos DB

You can follow the [azure official guide](https://docs.microsoft.com/en-us/azure/cosmos-db/restore-account-continuous-backup#restore-account-portal). After restoring backups. change the default consistencies of the restored databases to `STRONG`
It is recommended to use the mid-time of paused duration as a restore point as we explained earlier.

### DynamoDB

You can basically follow [the official doc](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/PointInTimeRecovery.Tutorial.html). However, a table can only be restored with an alias, so you need to restore a table with an alias and delete the original table and rename the alias to the original name to restore the same named tables.

Specifically, please follow the steps below.

1. Create a backup
   * Select the mid-time of paused duration as the restore point.
   * Restore with PITR of table A to another table B
   * Take a backup of the restored table B (assume the backup is named backup B)
   * Remove table B 
2. Restore from the backup 
   * Remove table A
   * Create a table named A with backup B

Note that tables can only be restored one by one so you need to do the above steps for each table.

Note also that some of the configurations such as PITR and auto scaling policies are reset to the default values for restored tables, so please manually configure required settings. See [the doc](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/CreateBackup.html#CreateBackup_HowItWorks-restore) for more detail.
If you used the schema loader and didn't change any other configurations, executing the schema loader with the same schema file is one of the ways to restore the original configuration. It only sets missing configurations if a specified table exists.
