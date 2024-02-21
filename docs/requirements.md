# Requirements and Recommendations for the Underlying Databases of ScalarDB

This document explains the requirements and recommendations in the underlying databases of ScalarDB to make ScalarDB applications work correctly and efficiently.

## Requirements

ScalarDB requires each underlying database to provide certain capabilities for running transactions and analytics on the databases. This document first explains the general requirements and then explains how to configure each database to achieve the requirements.

### General requirements

#### Transactions
{:.no_toc}
ScalarDB requires each underlying database to provide at least the following capabilities for running transactions on the databases:
- Linearizable read and conditional mutations (write and delete) on a single database record.
- Durability of written database records.
- Ability to store arbitrary data besides application data in each database record.

{% capture notice--info %}
**Note**

You need to have database accounts that have enough privileges to access the databases thorough ScalarDB since ScalarDB runs on the underlying databases not only for CRUD operations but also for performing operations like creating or altering schemas, tables, or indexes. ScalarDB basically requires a fully privileged account to access the underlying databases.
{% endcapture %}

<div class="notice--info">{{ notice--info | markdownify }}</div>

#### Analytics
{:.no_toc}
ScalarDB requires each underlying database to provide the following capability for running analytics on the databases:
- Ability to return only committed records.

### How to configure databases to achieve the general requirements

<div id="tabset-1">
<div class="tab">
  <button class="tablinks" onclick="openTab(event, 'JDBC_databases', 'tabset-1')" id="defaultOpen-1">JDBC databases</button>
  <button class="tablinks" onclick="openTab(event, 'DynamoDB', 'tabset-1')">DynamoDB</button>
  <button class="tablinks" onclick="openTab(event, 'Cosmos_DB_for_NoSQL', 'tabset-1')">Cosmos DB for NoSQL</button>
  <button class="tablinks" onclick="openTab(event, 'Cassandra', 'tabset-1')">Cassandra</button>
</div>

<div id="JDBC_databases" class="tabcontent" markdown="1">

#### Transactions
{:.no_toc}
- Use a single primary server or synchronized multi-primary servers for all operations. (No reads on read replicas that are asynchronously replicated from a primary database.)
- Use read committed or stricter isolation levels.

#### Analytics
{:.no_toc}
- Use read committed or stricter isolation levels.

</div>

<div id="DynamoDB" class="tabcontent" markdown="1">

#### Transactions
{:.no_toc}
- Use a single primary region for all operations. (No reads and writes on global tables in non-primary regions.)
  - There is no concept for primary regions in DynamoDB, so you must designate a primary region by yourself.

#### Analytics
{:.no_toc}
- N/A (Thee are no DynamoDB-specific requirements since DynamoDB always returns committed records.)

</div>

<div id="Cosmos_DB_for_NoSQL" class="tabcontent" markdown="1">

#### Transactions
{:.no_toc}
- Use a single primary region for all operations with `Strong` or `Bounded Staleness` consistency.

#### Analytics
{:.no_toc}
- N/A (Thee are no Cosmos DB-specific requirements since Cosmos DB always returns committed records.)

</div>

<div id="Cassandra" class="tabcontent" markdown="1">

#### Transactions
{:.no_toc}
- Use a single primary cluster for all operations. (No reads and writes in non-primary clusters.)
- Use `batch` or `group` for `commitlog_sync`.
- (In case you use Cassandra-compatible databases) Use Cassandra-compatible databases that properly support Lightweight Transactions (LWT).

#### Analytics
{:.no_toc}
- N/A (Thee are no Cassandra-specific requirements since Cassandra always returns committed records.)

</div>
</div>

## Recommendations

ScalarDB recommends each underlying database to be configured properly for high performance and high availability. The following items are some knobs and configurations to update.

{% capture notice--info %}
**Note**

ScalarDB can be seen as an application of underlying databases, so you may want to try updating other knobs and configurations that are commonly used to improve efficiency.
{% endcapture %}
<div class="notice--info">{{ notice--info | markdownify }}</div>

<div id="tabset-2">
<div class="tab">
  <button class="tablinks" onclick="openTab(event, 'JDBC_databases2', 'tabset-2')" id="defaultOpen-2">JDBC databases</button>
  <button class="tablinks" onclick="openTab(event, 'DynamoDB2', 'tabset-2')">DynamoDB</button>
  <button class="tablinks" onclick="openTab(event, 'Cosmos_DB_for_NoSQL2', 'tabset-2')">Cosmos DB for NoSQL</button>
  <button class="tablinks" onclick="openTab(event, 'Cassandra2', 'tabset-2')">Cassandra</button>
</div>

<div id="JDBC_databases2" class="tabcontent" markdown="1">
- Use read committed isolation for better performance.
- Follow each database’s best practices for performance optimization. For example, increasing buffer size (e.g., shared_buffers in PostgreSQL) and increasing the number of connections (e.g., max_connections in PostgreSQL) are usually recommended for better performance.
</div>

<div id="DynamoDB2" class="tabcontent" markdown="1">
- Increase RCU and WCU for high throughput.
- Enable Point-in-time Recovery (PITR).

{% capture notice--info %}
**Note**

Since DynamoDB stores data in multiple availability zones by default, so you don’t need to do anything for better availability.)
{% endcapture %}
<div class="notice--info">{{ notice--info | markdownify }}</div>
</div>

<div id="Cosmos_DB_for_NoSQL2" class="tabcontent" markdown="1">
- Increase RU for high throughput.
- Enable Point-In-Time-Restore (PITR).
- Enable Availability Zones.
</div>

<div id="Cassandra2" class="tabcontent" markdown="1">
- Increase concurrent_reads and concurrent_writes for high throughput. See [the official doc](https://cassandra.apache.org/doc/stable/cassandra/configuration/cass_yaml_file.html#concurrent_writes) for more details.
</div>

</div>
