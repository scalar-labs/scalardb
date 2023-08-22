# Getting Started with ScalarDB

This getting started tutorial explains how to configure your preferred database in ScalarDB and set up a basic electronic money application.

{% capture notice--warning %}
**Warning**

The electronic money application is simplified for this tutorial and isn't suitable for a production environment.
{% endcapture %}

<div class="notice--warning">{{ notice--warning | markdownify }}</div>

## Install a JDK

Because ScalarDB is written in Java, you must have one of the following Java Development Kits (JDKs) installed in your environment.

* [Oracle JDK 8](https://www.oracle.com/java/technologies/downloads/#java8)
* [Oracle JDK 11](https://www.oracle.com/java/technologies/downloads/#java11)
* [OpenJDK 8](https://openjdk.org/install/)

## Clone the `scalardb` repository
 
Open a terminal window, and go to your working directory. Then, clone the [scalardb](https://github.com/scalar-labs/scalardb) repository by running the following command:

```shell
$ git clone https://github.com/scalar-labs/scalardb
```

Then, go to the `scalardb/docs/getting-started` directory in the cloned repository by running the following command: 

```shell
$ cd scalardb/docs/getting-started
```

## Set up your database for ScalarDB

Select your database, and follow the instructions to configure it for ScalarDB.

For a list of databases that ScalarDB supports, see [Supported Databases](scalardb-supported-databases.md).

<div id="tabset-1">
<div class="tab">
  <button class="tablinks" onclick="openTab(event, 'Cassandra', 'tabset-1')" id="defaultOpen-1">Cassandra</button>
  <button class="tablinks" onclick="openTab(event, 'Cosmos_DB_for_NoSQL', 'tabset-1')">Cosmos DB for NoSQL</button>
  <button class="tablinks" onclick="openTab(event, 'DynamoDB', 'tabset-1')">DynamoDB</button>
  <button class="tablinks" onclick="openTab(event, 'JDBC_databases', 'tabset-1')">JDBC databases</button>
</div>

<div id="Cassandra" class="tabcontent" markdown="1">

Confirm that you have Cassandra installed. If Cassandra isn't installed, visit [Downloading Cassandra](https://cassandra.apache.org/_/download.html).

### Configure Cassandra
{:.no_toc}

Open **cassandra.yaml** in your preferred IDE. Then, change `commitlog_sync` from `periodic` to `batch` so that you don't lose data if a quorum of replica nodes goes down.

### Configure ScalarDB
{:.no_toc}

The following instructions assume that you have properly installed and configured the JDK and Cassandra in your local environment, and Cassandra is running on your localhost.

The **scalardb.properties** file in the `docs/getting-started` directory holds database configurations for ScalarDB. The following is a basic configuration for Cassandra. Be sure to change the values for `scalar.db.username` and `scalar.db.password` as described.

```properties
# The Cassandra storage implementation is used for Consensus Commit.
scalar.db.storage=cassandra

# Comma-separated contact points.
scalar.db.contact_points=localhost

# The port number for all the contact points.
scalar.db.contact_port=9042

# The username and password to access the database.
scalar.db.username=<USER_NAME>
scalar.db.password=<USER_PASSWORD>
```
</div>
<div id="Cosmos_DB_for_NoSQL" class="tabcontent" markdown="1">

To use Azure Cosmos DB for NoSQL, you must have an Azure account. If you don't have an Azure account, visit [Create an Azure Cosmos DB account](https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/quickstart-portal#create-account).

### Configure Cosmos DB for NoSQL
{:.no_toc}

Set the **default consistency level** to **Strong** according to the official document at [Configure the default consistency level](https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/how-to-manage-consistency#configure-the-default-consistency-level).

### Configure ScalarDB
{:.no_toc}

The following instructions assume that you have properly installed and configured the JDK in your local environment and properly configured your Cosmos DB for NoSQL account in Azure. 

The **scalardb.properties** file in the `docs/getting-started` directory holds database configurations for ScalarDB. Be sure to change the values for `scalar.db.contact_points` and `scalar.db.password` as described.
    
```properties
# The Cosmos DB for NoSQL storage implementation is used for Consensus Commit.
scalar.db.storage=cosmos

# The Cosmos DB for NoSQL URI.
scalar.db.contact_points=<COSMOS_DB_FOR_NOSQL_URI>

# The Cosmos DB for NoSQL key to access the database.
scalar.db.password=<COSMOS_DB_FOR_NOSQL_KEY>
```

{% capture notice--info %}
**Note**

You can use a primary key or a secondary key as the value for `scalar.db.password`.
{% endcapture %}
<div class="notice--info">{{ notice--info | markdownify }}</div>
</div>
<div id="DynamoDB" class="tabcontent" markdown="1">

To use Amazon DynamoDB, you must have an AWS account. If you don't have an AWS account, visit [Getting started: Are you a first-time AWS user?](https://docs.aws.amazon.com/accounts/latest/reference/welcome-first-time-user.html).

### Configure ScalarDB
{:.no_toc}

The following instructions assume that you have properly installed and configured the JDK in your local environment.

The **scalardb.properties** file in the `docs/getting-started` directory holds database configurations for ScalarDB. Be sure to change the values for `scalar.db.contact_points`, `scalar.db.username`, and  `scalar.db.password` as described.

```properties
# The DynamoDB storage implementation is used for Consensus Commit.
scalar.db.storage=dynamo

# The AWS region.
scalar.db.contact_points=<REGION>

# The AWS access key ID and secret access key to access the database.
scalar.db.username=<ACCESS_KEY_ID>
scalar.db.password=<SECRET_ACCESS_KEY>
```
</div>
<div id="JDBC_databases" class="tabcontent" markdown="1">

Confirm that you have a JDBC database installed. For a list of supported JDBC databases, see [Supported Databases](scalardb-supported-databases.md).

### Configure ScalarDB
{:.no_toc}

The following instructions assume that you have properly installed and configured the JDK and JDBC database in your local environment, and the JDBC database is running on your localhost.

The **scalardb.properties** file in the `docs/getting-started` directory holds database configurations for ScalarDB. The following is a basic configuration for JDBC databases. 

{% capture notice--info %}
**Note**

Be sure to uncomment the `scalar.db.contact_points` variable and change the value of the JDBC database you are using, and change the values for `scalar.db.username` and `scalar.db.password` as described.
{% endcapture %}

<div class="notice--info">{{ notice--info | markdownify }}</div>

```properties
# The JDBC database storage implementation is used for Consensus Commit.
scalar.db.storage=jdbc

# The JDBC database URL for the type of database you are using.
# scalar.db.contact_points=jdbc:mysql://localhost:3306/
# scalar.db.contact_points=jdbc:oracle:thin:@//localhost:1521/
# scalar.db.contact_points=jdbc:postgresql://localhost:5432/
# scalar.db.contact_points=jdbc:sqlserver://localhost:1433;
# scalar.db.contact_points=jdbc:sqlite://localhost:3306.sqlite3?busy_timeout=10000

# The username and password for connecting to the database.
scalar.db.username=<USER_NAME>
scalar.db.password=<PASSWORD>
```
</div>
</div>

## Create and load the database schema

You need to define the database schema (the method in which the data will be organized) in the application. For details about the supported data types, see [Data type mapping between ScalarDB and other databases](schema-loader.md#data-type-mapping-between-scalardb-and-the-other-databases).

For this tutorial, create a file named **emoney.json** in the `scalardb/docs/getting-started` directory. Then, add the following JSON code to define the schema.

```json
{
  "emoney.account": {
    "transaction": true,
    "partition-key": [
      "id"
    ],
    "clustering-key": [],
    "columns": {
      "id": "TEXT",
      "balance": "INT"
    }
  }
}
```

To apply the schema, go to the [`scalardb` Releases](https://github.com/scalar-labs/scalardb/releases) page and download the ScalarDB Schema Loader that matches the version of ScalarDB that you are using to the `getting-started` folder. 

Then, run the following command, replacing `<VERSION>` with the version of the ScalarDB Schema Loader that you downloaded:

```shell
$ java -jar scalardb-schema-loader-<VERSION>.jar --config scalardb.properties --schema-file emoney.json --coordinator
```

{% capture notice--info %}
**Note**

The `--coordinator` option is specified because a table with `transaction` set to `true` exists in the schema. For details about configuring and loading a schema, see [ScalarDB Schema Loader](schema-loader.md).
{% endcapture %}

<div class="notice--info">{{ notice--info | markdownify }}</div>

## Execute transactions and retrieve data in the basic electronic money application

After loading the schema, you can execute transactions and retrieve data in the basic electronic money application that is included in the repository that you cloned.

The application supports the following types of transactions:

- Create an account.
- Add funds to an account.
- Send funds between two accounts.
- Get an account balance.

{% capture notice--info %}
**Note**

When you first execute a Gradle command, Gradle will automatically install the necessary libraries.
{% endcapture %}

<div class="notice--info">{{ notice--info | markdownify }}</div>

### Create an account with a balance

You need an account with a balance so that you can send funds between accounts.

To create an account for **customer1** that has a balance of **500**, run the following command:

```shell
$ ./gradlew run --args="-action charge -amount 500 -to customer1"
```

### Create an account without a balance

After setting up an account that has a balance, you need another account for sending funds to.

To create an account for **merchant1** that has a balance of **0**, run the following command:

```shell
$ ./gradlew run --args="-action charge -amount 0 -to merchant1"
```

### Add funds to an account

You can add funds to an account in the same way that you created and added funds to an account in [Create an account with a balance](#create-an-account-with-a-balance).

To add **500** to the account for **customer1**, run the following command:

```shell
$ ./gradlew run --args="-action charge -amount 500 -to customer1"
```

The account for **customer1** will now have a balance of **1000**.

### Send electronic money between two accounts

Now that you have created two accounts, with at least one of those accounts having a balance, you can send funds from one account to the other account.

To have **customer1** pay **100** to **merchant1**, run the following command:

```shell
$ ./gradlew run --args="-action pay -amount 100 -from customer1 -to merchant1"
```

### Get an account balance

After sending funds from one account to the other, you can check the balance of each account.

To get the balance of **customer1**, run the following command:

```shell
$ ./gradlew run --args="-action getBalance -id customer1"
```

You should see the following output:

```shell
...
The balance for customer1 is 900
...
```

To get the balance of **merchant1**, run the following command:

```shell
$ ./gradlew run --args="-action getBalance -id merchant1"
```

You should see the following output:

```shell
...
The balance for merchant1 is 100
...
```

## Reference

To see the source code for the electronic money application used in this tutorial, see [`ElectronicMoney.java`](./getting-started/src/main/java/sample/ElectronicMoney.java).
