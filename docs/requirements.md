# Requirements and Recommendations for the Underlying Databases of ScalarDB

This document explains the requirements and recommendations in the underlying databases of ScalarDB to make ScalarDB applications work correctly and efficiently.

## Requirements

ScalarDB requires each underlying database to provide certain capabilities to run transactions and analytics on the databases. This document explains the general requirements and how to configure each database to achieve the requirements.

### General requirements

#### Transactions
{:.no_toc}
ScalarDB requires each underlying database to provide at least the following capabilities to run transactions on the databases:

- Linearizable read and conditional mutations (write and delete) on a single database record.
- Durability of written database records.
- Ability to store arbitrary data besides application data in each database record.

{% capture notice--info %}
**Note**

You need to have database accounts that have enough privileges to access the databases through ScalarDB since ScalarDB runs on the underlying databases not only for CRUD operations but also for performing operations like creating or altering schemas, tables, or indexes. ScalarDB basically requires a fully privileged account to access the underlying databases.
{% endcapture %}

<div class="notice--info">{{ notice--info | markdownify }}</div>

#### Analytics
{:.no_toc}
ScalarDB requires each underlying database to provide the following capability to run analytics on the databases:

- Ability to return only committed records.

### How to configure databases to achieve the general requirements

Select your database for details on how to configure it to achieve the general requirements.

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
- Use a single primary server or synchronized multi-primary servers for all operations (no read operations on read replicas that are asynchronously replicated from a primary database).
- Use read-committed or stricter isolation levels.

#### Analytics
{:.no_toc}
- Use read-committed or stricter isolation levels.

</div>

<div id="DynamoDB" class="tabcontent" markdown="1">

#### Transactions
{:.no_toc}
- Use a single primary region for all operations. (No read and write operations on global tables in non-primary regions.)
  - There is no concept for primary regions in DynamoDB, so you must designate a primary region by yourself.

#### Analytics
{:.no_toc}
- Not applicable. DynamoDB always returns committed records, so there are no DynamoDB-specific requirements.

</div>

<div id="Cosmos_DB_for_NoSQL" class="tabcontent" markdown="1">

#### Transactions
{:.no_toc}
- Use a single primary region for all operations with `Strong` or `Bounded Staleness` consistency.

#### Analytics
{:.no_toc}
- Not applicable. Cosmos DB always returns committed records, so there are no Cosmos DB–specific requirements.

</div>

<div id="Cassandra" class="tabcontent" markdown="1">

#### Transactions
{:.no_toc}
- Use a single primary cluster for all operations (no read or write operations in non-primary clusters).
- Use `batch` or `group` for `commitlog_sync`.
- If you're using Cassandra-compatible databases, those databases must properly support lightweight transactions (LWT).

#### Analytics
{:.no_toc}
- Not applicable. Cassandra always returns committed records, so there are no Cassandra-specific requirements.

</div>
</div>

## Recommendations

Properly configuring each underlying database of ScalarDB for high performance and high availability is recommended. The following recommendations include some knobs and configurations to update.

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
- Use read-committed isolation for better performance.
- Follow the performance optimization best practices for each database. For example, increasing the buffer size (for example, `shared_buffers` in PostgreSQL) and increasing the number of connections (for example, `max_connections` in PostgreSQL) are usually recommended for better performance.
</div>

<div id="DynamoDB2" class="tabcontent" markdown="1">
- Increase the number of read capacity units (RCUs) and write capacity units (WCUs) for high throughput.
- Enable point-in-time recovery (PITR).

{% capture notice--info %}
**Note**

Since DynamoDB stores data in multiple availability zones by default, you don’t need to adjust any configurations to improve availability.
{% endcapture %}
<div class="notice--info">{{ notice--info | markdownify }}</div>
</div>

<div id="Cosmos_DB_for_NoSQL2" class="tabcontent" markdown="1">
- Increase the number of Request Units (RUs) for high throughput.
- Enable point-in-time restore (PITR).
- Enable availability zones.
</div>

<div id="Cassandra2" class="tabcontent" markdown="1">
- Increase `concurrent_reads` and `concurrent_writes` for high throughput. For details, see the official Cassandra documentation about [`concurrent_writes`](https://cassandra.apache.org/doc/stable/cassandra/configuration/cass_yaml_file.html#concurrent_writes).
</div>
</div>
