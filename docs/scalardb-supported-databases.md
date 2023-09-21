# Supported Databases

ScalarDB supports the following databases and their versions.

## Amazon DynamoDB

| Version           | DynamoDB  |
|:------------------|:----------|
| **ScalarDB 3.10** | ✅        |
| **ScalarDB 3.9**  | ✅        |
| **ScalarDB 3.8**  | ✅        |
| **ScalarDB 3.7**  | ✅        |
| **ScalarDB 3.6**  | ✅        |
| **ScalarDB 3.5**  | ✅        |
| **ScalarDB 3.4**  | ✅        |

## Apache Cassandra

{% capture notice--info %}
**Note**

For requirements when using Cassandra or Cassandra-compatible databases, see [Cassandra or Cassandra-compatible database requirements](requirements/#cassandra-or-cassandra-compatible-database-requirements).
{% endcapture %}

<div class="notice--info">{{ notice--info | markdownify }}</div>

| Version           | Cassandra 4.1  | Cassandra 4.0  | Cassandra 3.11  | Cassandra 3.0  |
|:------------------|:---------------|:---------------|:----------------|:---------------|
| **ScalarDB 3.10** | ❌             | ❌             | ✅              | ✅             |
| **ScalarDB 3.9**  | ❌             | ❌             | ✅              | ✅             |
| **ScalarDB 3.8**  | ❌             | ❌             | ✅              | ✅             |
| **ScalarDB 3.7**  | ❌             | ❌             | ✅              | ✅             |
| **ScalarDB 3.6**  | ❌             | ❌             | ✅              | ✅             |
| **ScalarDB 3.5**  | ❌             | ❌             | ✅              | ✅             |
| **ScalarDB 3.4**  | ❌             | ❌             | ✅              | ✅             |

## Azure Cosmos DB for NoSQL

| Version           | Cosmos DB for NoSQL  |
|:------------------|:---------------------|
| **ScalarDB 3.10** | ✅                   |
| **ScalarDB 3.9**  | ✅                   |
| **ScalarDB 3.8**  | ✅                   |
| **ScalarDB 3.7**  | ✅                   |
| **ScalarDB 3.6**  | ✅                   |
| **ScalarDB 3.5**  | ✅                   |
| **ScalarDB 3.4**  | ✅                   |

## JDBC databases

{% capture notice--info %}
**Note**

For recommendations when using JDBC databases, see [JDBC database recommendations](requirements/#jdbc-database-requirements).
{% endcapture %}

<div class="notice--info">{{ notice--info | markdownify }}</div>

### Amazon Aurora MySQL

| Version           | Aurora MySQL 3  | Aurora MySQL 2  |
|:------------------|:----------------|:----------------|
| **ScalarDB 3.10** | ✅              | ✅              |
| **ScalarDB 3.9**  | ✅              | ✅              |
| **ScalarDB 3.8**  | ✅              | ✅              |
| **ScalarDB 3.7**  | ✅              | ✅              |
| **ScalarDB 3.6**  | ✅              | ✅              |
| **ScalarDB 3.5**  | ✅              | ✅              |
| **ScalarDB 3.4**  | ✅              | ✅              |

### Amazon Aurora PostgreSQL

| Version           | Aurora PostgreSQL 15  | Aurora PostgreSQL 14  | Aurora PostgreSQL 13  | Aurora PostgreSQL 12  |
|:------------------|:----------------------|:----------------------|:----------------------|:----------------------|
| **ScalarDB 3.10** | ✅                    | ✅                    | ✅                    | ✅                    |
| **ScalarDB 3.9**  | ✅                    | ✅                    | ✅                    | ✅                    |
| **ScalarDB 3.8**  | ✅                    | ✅                    | ✅                    | ✅                    |
| **ScalarDB 3.7**  | ✅                    | ✅                    | ✅                    | ✅                    |
| **ScalarDB 3.6**  | ✅                    | ✅                    | ✅                    | ✅                    |
| **ScalarDB 3.5**  | ✅                    | ✅                    | ✅                    | ✅                    |
| **ScalarDB 3.4**  | ✅                    | ✅                    | ✅                    | ✅                    |

### Microsoft SQL Server

| Version           | SQL Server 2022  | SQL Server 2019  | SQL Server 2017  |
|:------------------|:-----------------|:-----------------|:-----------------|
| **ScalarDB 3.10** | ✅               | ✅               | ✅               |
| **ScalarDB 3.9**  | ✅               | ✅               | ✅               |
| **ScalarDB 3.8**  | ✅               | ✅               | ✅               |
| **ScalarDB 3.7**  | ✅               | ✅               | ✅               |
| **ScalarDB 3.6**  | ✅               | ✅               | ✅               |
| **ScalarDB 3.5**  | ✅               | ✅               | ✅               |
| **ScalarDB 3.4**  | ✅               | ✅               | ✅               |

### MySQL

| Version           | MySQL 8.1  | MySQL 8.0  | MySQL 5.7  |
|:------------------|:-----------|:-----------|:-----------|
| **ScalarDB 3.10** | ✅         | ✅         | ✅         |
| **ScalarDB 3.9**  | ✅         | ✅         | ✅         |
| **ScalarDB 3.8**  | ✅         | ✅         | ✅         |
| **ScalarDB 3.7**  | ✅         | ✅         | ✅         |
| **ScalarDB 3.6**  | ✅         | ✅         | ✅         |
| **ScalarDB 3.5**  | ✅         | ✅         | ✅         |
| **ScalarDB 3.4**  | ✅         | ✅         | ✅         |

### Oracle

| Version           | Oracle 23.2.0-free  | Oracle 21.3.0-xe  | Oracle 18.4.0-xe  |
|:------------------|:--------------------|:------------------|:------------------|
| **ScalarDB 3.10** | ✅                  | ✅                | ✅                |
| **ScalarDB 3.9**  | ✅                  | ✅                | ✅                |
| **ScalarDB 3.8**  | ✅                  | ✅                | ✅                |
| **ScalarDB 3.7**  | ✅                  | ✅                | ✅                |
| **ScalarDB 3.6**  | ✅                  | ✅                | ✅                |
| **ScalarDB 3.5**  | ✅                  | ✅                | ✅                |
| **ScalarDB 3.4**  | ✅                  | ✅                | ✅                |

### PostgreSQL

| Version           | PostgreSQL 15  | PostgreSQL 14  | PostgreSQL 13  | PostgreSQL 12  |
|:------------------|:---------------|:---------------|:---------------|:---------------|
| **ScalarDB 3.10** | ✅             | ✅             | ✅             | ✅             |
| **ScalarDB 3.9**  | ✅             | ✅             | ✅             | ✅             |
| **ScalarDB 3.8**  | ✅             | ✅             | ✅             | ✅             |
| **ScalarDB 3.7**  | ✅             | ✅             | ✅             | ✅             |
| **ScalarDB 3.6**  | ✅             | ✅             | ✅             | ✅             |
| **ScalarDB 3.5**  | ✅             | ✅             | ✅             | ✅             |
| **ScalarDB 3.4**  | ✅             | ✅             | ✅             | ✅             |

### SQLite

| Version           | SQLite 3  |
|:------------------|:----------|
| **ScalarDB 3.10** | ✅        |
| **ScalarDB 3.9**  | ✅        |
| **ScalarDB 3.8**  | ❌        |
| **ScalarDB 3.7**  | ❌        |
| **ScalarDB 3.6**  | ❌        |
| **ScalarDB 3.5**  | ❌        |
| **ScalarDB 3.4**  | ❌        |
