package com.scalar.db.sql;

import com.scalar.db.sql.builder.SqlBuilder;
import com.scalar.db.sql.statement.BatchStatement;
import com.scalar.db.sql.statement.CreateNamespaceStatement;
import com.scalar.db.sql.statement.CreateTableStatement;
import com.scalar.db.sql.statement.DeleteStatement;
import com.scalar.db.sql.statement.DropNamespaceStatement;
import com.scalar.db.sql.statement.DropTableStatement;
import com.scalar.db.sql.statement.InsertStatement;
import com.scalar.db.sql.statement.SelectStatement;
import com.scalar.db.sql.statement.TruncateTableStatement;
import com.scalar.db.sql.statement.UpdateStatement;
import com.scalar.db.transaction.consensuscommit.Attribute;
import com.scalar.db.transaction.consensuscommit.Coordinator;

public class Example {

  private static final String NAMESPACE_NAME = "ns";
  private static final String TABLE_NAME = "tbl";
  private static final String COLUMN_NAME_1 = "col1";
  private static final String COLUMN_NAME_2 = "col2";
  private static final String COLUMN_NAME_3 = "col3";
  private static final String COLUMN_NAME_4 = "col4";

  public static void main(String[] args) {
    try (SqlSessionFactory sqlSessionFactory =
        SqlSessionFactory.builder()
            .withProperty("scalar.db.contact_points", "jdbc:mysql://localhost:3306/")
            .withProperty("scalar.db.username", "root")
            .withProperty("scalar.db.password", "mysql")
            .withProperty("scalar.db.storage", "jdbc")
            .build()) {
      storageModeExample(sqlSessionFactory);
      transactionModeExample(sqlSessionFactory);
      twoPhaseCommitTransactionModeExample(sqlSessionFactory);
    }
  }

  public static void storageModeExample(SqlSessionFactory sqlSessionFactory) {
    System.out.println("Start storage mode example");

    StorageSqlSession storageSqlSession = sqlSessionFactory.getStorageSqlSession();

    ResultSet resultSet;

    // Create namespace
    CreateNamespaceStatement createNamespaceStatement =
        SqlBuilder.createNamespace(NAMESPACE_NAME).ifNotExists().build();
    resultSet = storageSqlSession.execute(createNamespaceStatement);
    resultSet.close();

    // Create table
    CreateTableStatement createTableStatement =
        SqlBuilder.createTable(NAMESPACE_NAME, TABLE_NAME)
            .ifNotExists()
            .withPartitionKey(COLUMN_NAME_1, DataType.INT)
            .withClusteringKey(COLUMN_NAME_2, DataType.TEXT)
            .withColumn(COLUMN_NAME_3, DataType.BIGINT)
            .withColumn(COLUMN_NAME_4, DataType.FLOAT)
            .withClusteringOrder(COLUMN_NAME_2, Order.ASC)
            .build();
    resultSet = storageSqlSession.execute(createTableStatement);
    resultSet.close();

    // Insert
    InsertStatement insertStatement =
        SqlBuilder.insertInto(NAMESPACE_NAME, TABLE_NAME)
            .values(
                Assignment.column(COLUMN_NAME_1).value(Value.ofInt(10)),
                Assignment.column(COLUMN_NAME_2).value(Value.ofText("abc")),
                Assignment.column(COLUMN_NAME_3).value(Value.ofBigInt(100L)),
                Assignment.column(COLUMN_NAME_4).value(Value.ofFloat(1.23F)))
            .ifNotExists()
            .build();
    resultSet = storageSqlSession.execute(insertStatement);
    System.out.println("InsertStatement result: " + resultSet.one().get().getBoolean(0));
    resultSet.close();

    // Update
    UpdateStatement updateStatement =
        SqlBuilder.update(NAMESPACE_NAME, TABLE_NAME)
            .set(
                Assignment.column(COLUMN_NAME_3).value(Value.ofBigInt(200L)),
                Assignment.column(COLUMN_NAME_4).value(Value.ofFloat(4.56F)))
            .where(Condition.column(COLUMN_NAME_1).isEqualTo(Value.ofInt(10)))
            .and(Condition.column(COLUMN_NAME_2).isEqualTo(Value.ofText("abc")))
            .if_(Condition.column(COLUMN_NAME_3).isEqualTo(Value.ofBigInt(100L)))
            .and(Condition.column(COLUMN_NAME_4).isEqualTo(Value.ofFloat(1.23F)))
            .build();
    resultSet = storageSqlSession.execute(updateStatement);
    System.out.println("UpdateStatement result: " + resultSet.one().get().getBoolean(0));
    resultSet.close();

    // Batch
    InsertStatement batchStatement1 =
        SqlBuilder.insertInto(NAMESPACE_NAME, TABLE_NAME)
            .values(
                Assignment.column(COLUMN_NAME_1).value(Value.ofInt(10)),
                Assignment.column(COLUMN_NAME_2).value(Value.ofText("def")),
                Assignment.column(COLUMN_NAME_3).value(Value.ofBigInt(200L)),
                Assignment.column(COLUMN_NAME_4).value(Value.ofFloat(1.00F)))
            .ifNotExists()
            .build();
    InsertStatement batchStatement2 =
        SqlBuilder.insertInto(NAMESPACE_NAME, TABLE_NAME)
            .values(
                Assignment.column(COLUMN_NAME_1).value(Value.ofInt(10)),
                Assignment.column(COLUMN_NAME_2).value(Value.ofText("ghi")),
                Assignment.column(COLUMN_NAME_3).value(Value.ofBigInt(300L)),
                Assignment.column(COLUMN_NAME_4).value(Value.ofFloat(2.00F)))
            .ifNotExists()
            .build();
    BatchStatement batchStatement =
        SqlBuilder.batch().addStatement(batchStatement1).addStatement(batchStatement2).build();
    resultSet = storageSqlSession.execute(batchStatement);
    System.out.println("DeleteStatement result: " + resultSet.one().get().getBoolean(0));
    resultSet.close();

    // Select
    SelectStatement selectStatement =
        SqlBuilder.select(COLUMN_NAME_1, COLUMN_NAME_2, COLUMN_NAME_3)
            .from(NAMESPACE_NAME, TABLE_NAME)
            .where(Condition.column(COLUMN_NAME_1).isEqualTo(Value.ofInt(10)))
            .orderBy(Ordering.column(COLUMN_NAME_2).asc())
            .limit(10)
            .build();

    try (ResultSet rs = storageSqlSession.execute(selectStatement)) {
      for (Record record : rs) {
        System.out.println("column1 value: " + record.getInt(COLUMN_NAME_1));
        System.out.println("column2 value: " + record.getText(COLUMN_NAME_2));
        System.out.println("column3 value: " + record.getBigInt(COLUMN_NAME_3));
        System.out.println();
      }
    }

    // Delete
    DeleteStatement deleteStatement =
        SqlBuilder.deleteFrom(NAMESPACE_NAME, TABLE_NAME)
            .where(Condition.column(COLUMN_NAME_1).isEqualTo(Value.ofInt(10)))
            .and(Condition.column(COLUMN_NAME_2).isEqualTo(Value.ofText("abc")))
            .if_(Condition.column(COLUMN_NAME_3).isEqualTo(Value.ofBigInt(100L)))
            .and(Condition.column(COLUMN_NAME_4).isEqualTo(Value.ofFloat(1.23F)))
            .build();
    resultSet = storageSqlSession.execute(deleteStatement);
    System.out.println("DeleteStatement result: " + resultSet.one().get().getBoolean(0));
    resultSet.close();

    // Truncate table
    TruncateTableStatement truncateTableStatement =
        SqlBuilder.truncateTable(NAMESPACE_NAME, TABLE_NAME).build();
    resultSet = storageSqlSession.execute(truncateTableStatement);
    resultSet.close();

    // Drop table
    DropTableStatement dropTableStatement =
        SqlBuilder.dropTable(NAMESPACE_NAME, TABLE_NAME).ifExists().build();
    resultSet = storageSqlSession.execute(dropTableStatement);
    resultSet.close();

    // Drop namespace
    DropNamespaceStatement dropNamespaceStatement =
        SqlBuilder.dropNamespace(NAMESPACE_NAME).ifExists().cascade().build();
    resultSet = storageSqlSession.execute(dropNamespaceStatement);
    resultSet.close();
  }

  public static void transactionModeExample(SqlSessionFactory sqlSessionFactory) {
    System.out.println("Start transaction mode example");

    StorageSqlSession storageSqlSession = sqlSessionFactory.getStorageSqlSession();

    ResultSet resultSet;

    // Create namespace
    CreateNamespaceStatement createNamespaceStatement =
        SqlBuilder.createNamespace(NAMESPACE_NAME).ifNotExists().build();
    resultSet = storageSqlSession.execute(createNamespaceStatement);
    resultSet.close();

    createNamespaceStatement =
        SqlBuilder.createNamespace(Coordinator.NAMESPACE).ifNotExists().build();
    resultSet = storageSqlSession.execute(createNamespaceStatement);
    resultSet.close();

    // Create table
    CreateTableStatement createTableStatement =
        SqlBuilder.createTable(NAMESPACE_NAME, TABLE_NAME)
            .ifNotExists()
            .withPartitionKey(COLUMN_NAME_1, DataType.INT)
            .withClusteringKey(COLUMN_NAME_2, DataType.TEXT)
            .withColumn(COLUMN_NAME_3, DataType.BIGINT)
            .withColumn(COLUMN_NAME_4, DataType.FLOAT)
            .withColumn(Attribute.ID, DataType.TEXT)
            .withColumn(Attribute.STATE, DataType.INT)
            .withColumn(Attribute.VERSION, DataType.INT)
            .withColumn(Attribute.PREPARED_AT, DataType.BIGINT)
            .withColumn(Attribute.COMMITTED_AT, DataType.BIGINT)
            .withColumn(Attribute.BEFORE_PREFIX + COLUMN_NAME_3, DataType.BIGINT)
            .withColumn(Attribute.BEFORE_PREFIX + COLUMN_NAME_4, DataType.FLOAT)
            .withColumn(Attribute.BEFORE_ID, DataType.TEXT)
            .withColumn(Attribute.BEFORE_STATE, DataType.INT)
            .withColumn(Attribute.BEFORE_VERSION, DataType.INT)
            .withColumn(Attribute.BEFORE_PREPARED_AT, DataType.BIGINT)
            .withColumn(Attribute.BEFORE_COMMITTED_AT, DataType.BIGINT)
            .withClusteringOrder(COLUMN_NAME_2, Order.ASC)
            .build();
    resultSet = storageSqlSession.execute(createTableStatement);
    resultSet.close();

    createTableStatement =
        SqlBuilder.createTable(Coordinator.NAMESPACE, Coordinator.TABLE)
            .ifNotExists()
            .withPartitionKey(Attribute.ID, DataType.TEXT)
            .withColumn(Attribute.STATE, DataType.INT)
            .withColumn(Attribute.CREATED_AT, DataType.BIGINT)
            .build();
    resultSet = storageSqlSession.execute(createTableStatement);
    resultSet.close();

    // Begin
    TransactionSqlSession transactionSqlSession = sqlSessionFactory.beginTransaction();

    // Insert
    InsertStatement insertStatement =
        SqlBuilder.insertInto(NAMESPACE_NAME, TABLE_NAME)
            .values(
                Assignment.column(COLUMN_NAME_1).value(Value.ofInt(10)),
                Assignment.column(COLUMN_NAME_2).value(Value.ofText("abc")),
                Assignment.column(COLUMN_NAME_3).value(Value.ofBigInt(100L)),
                Assignment.column(COLUMN_NAME_4).value(Value.ofFloat(1.23F)))
            .build();
    resultSet = transactionSqlSession.execute(insertStatement);
    resultSet.close();

    // Update
    UpdateStatement updateStatement =
        SqlBuilder.update(NAMESPACE_NAME, TABLE_NAME)
            .set(
                Assignment.column(COLUMN_NAME_3).value(Value.ofBigInt(200L)),
                Assignment.column(COLUMN_NAME_4).value(Value.ofFloat(4.56F)))
            .where(Condition.column(COLUMN_NAME_1).isEqualTo(Value.ofInt(10)))
            .and(Condition.column(COLUMN_NAME_2).isEqualTo(Value.ofText("abc")))
            .build();
    resultSet = transactionSqlSession.execute(updateStatement);
    resultSet.close();

    // Select
    SelectStatement selectStatement =
        SqlBuilder.select(COLUMN_NAME_1, COLUMN_NAME_2, COLUMN_NAME_3)
            .from(NAMESPACE_NAME, TABLE_NAME)
            .where(Condition.column(COLUMN_NAME_1).isEqualTo(Value.ofInt(10)))
            .and(Condition.column(COLUMN_NAME_2).isEqualTo(Value.ofText("abc")))
            .build();

    try (ResultSet rs = transactionSqlSession.execute(selectStatement)) {
      for (Record record : rs) {
        System.out.println("column1 value: " + record.getInt(COLUMN_NAME_1));
        System.out.println("column2 value: " + record.getText(COLUMN_NAME_2));
        System.out.println("column3 value: " + record.getBigInt(COLUMN_NAME_3));
        System.out.println();
      }
    }

    // Delete
    DeleteStatement deleteStatement =
        SqlBuilder.deleteFrom(NAMESPACE_NAME, TABLE_NAME)
            .where(Condition.column(COLUMN_NAME_1).isEqualTo(Value.ofInt(10)))
            .and(Condition.column(COLUMN_NAME_2).isEqualTo(Value.ofText("abc")))
            .build();
    resultSet = transactionSqlSession.execute(deleteStatement);
    resultSet.close();

    // Commit
    transactionSqlSession.commit();

    // Drop table
    DropTableStatement dropTableStatement =
        SqlBuilder.dropTable(NAMESPACE_NAME, TABLE_NAME).ifExists().build();
    resultSet = storageSqlSession.execute(dropTableStatement);
    resultSet.close();

    dropTableStatement =
        SqlBuilder.dropTable(Coordinator.NAMESPACE, Coordinator.TABLE).ifExists().build();
    resultSet = storageSqlSession.execute(dropTableStatement);
    resultSet.close();

    // Drop namespace
    DropNamespaceStatement dropNamespaceStatement =
        SqlBuilder.dropNamespace(NAMESPACE_NAME).ifExists().cascade().build();
    resultSet = storageSqlSession.execute(dropNamespaceStatement);
    resultSet.close();

    dropNamespaceStatement =
        SqlBuilder.dropNamespace(Coordinator.NAMESPACE).ifExists().cascade().build();
    resultSet = storageSqlSession.execute(dropNamespaceStatement);
    resultSet.close();
  }

  public static void twoPhaseCommitTransactionModeExample(SqlSessionFactory sqlSessionFactory) {
    System.out.println("Start two-phase commit transaction mode example");

    StorageSqlSession storageSqlSession = sqlSessionFactory.getStorageSqlSession();

    ResultSet resultSet;

    // Create namespace
    CreateNamespaceStatement createNamespaceStatement =
        SqlBuilder.createNamespace(NAMESPACE_NAME).ifNotExists().build();
    resultSet = storageSqlSession.execute(createNamespaceStatement);
    resultSet.close();

    createNamespaceStatement =
        SqlBuilder.createNamespace(Coordinator.NAMESPACE).ifNotExists().build();
    resultSet = storageSqlSession.execute(createNamespaceStatement);
    resultSet.close();

    // Create table
    CreateTableStatement createTableStatement =
        SqlBuilder.createTable(NAMESPACE_NAME, TABLE_NAME)
            .ifNotExists()
            .withPartitionKey(COLUMN_NAME_1, DataType.INT)
            .withClusteringKey(COLUMN_NAME_2, DataType.TEXT)
            .withColumn(COLUMN_NAME_3, DataType.BIGINT)
            .withColumn(COLUMN_NAME_4, DataType.FLOAT)
            .withColumn(Attribute.ID, DataType.TEXT)
            .withColumn(Attribute.STATE, DataType.INT)
            .withColumn(Attribute.VERSION, DataType.INT)
            .withColumn(Attribute.PREPARED_AT, DataType.BIGINT)
            .withColumn(Attribute.COMMITTED_AT, DataType.BIGINT)
            .withColumn(Attribute.BEFORE_PREFIX + COLUMN_NAME_3, DataType.BIGINT)
            .withColumn(Attribute.BEFORE_PREFIX + COLUMN_NAME_4, DataType.FLOAT)
            .withColumn(Attribute.BEFORE_ID, DataType.TEXT)
            .withColumn(Attribute.BEFORE_STATE, DataType.INT)
            .withColumn(Attribute.BEFORE_VERSION, DataType.INT)
            .withColumn(Attribute.BEFORE_PREPARED_AT, DataType.BIGINT)
            .withColumn(Attribute.BEFORE_COMMITTED_AT, DataType.BIGINT)
            .withClusteringOrder(COLUMN_NAME_2, Order.ASC)
            .build();
    resultSet = storageSqlSession.execute(createTableStatement);
    resultSet.close();

    createTableStatement =
        SqlBuilder.createTable(Coordinator.NAMESPACE, Coordinator.TABLE)
            .ifNotExists()
            .withPartitionKey(Attribute.ID, DataType.TEXT)
            .withColumn(Attribute.STATE, DataType.INT)
            .withColumn(Attribute.CREATED_AT, DataType.BIGINT)
            .build();
    resultSet = storageSqlSession.execute(createTableStatement);
    resultSet.close();

    // Begin
    TwoPhaseCommitTransactionSqlSession twoPhaseCommitTransactionSqlSession1 =
        sqlSessionFactory.beginTwoPhaseCommitTransaction();

    TwoPhaseCommitTransactionSqlSession twoPhaseCommitTransactionSqlSession2 =
        sqlSessionFactory.joinTwoPhaseCommitTransaction(
            twoPhaseCommitTransactionSqlSession1.getTransactionId());

    // Insert
    InsertStatement insertStatement1 =
        SqlBuilder.insertInto(NAMESPACE_NAME, TABLE_NAME)
            .values(
                Assignment.column(COLUMN_NAME_1).value(Value.ofInt(10)),
                Assignment.column(COLUMN_NAME_2).value(Value.ofText("abc")),
                Assignment.column(COLUMN_NAME_3).value(Value.ofBigInt(100L)),
                Assignment.column(COLUMN_NAME_4).value(Value.ofFloat(1.23F)))
            .build();
    resultSet = twoPhaseCommitTransactionSqlSession1.execute(insertStatement1);
    resultSet.close();

    InsertStatement insertStatement2 =
        SqlBuilder.insertInto(NAMESPACE_NAME, TABLE_NAME)
            .values(
                Assignment.column(COLUMN_NAME_1).value(Value.ofInt(11)),
                Assignment.column(COLUMN_NAME_2).value(Value.ofText("def")),
                Assignment.column(COLUMN_NAME_3).value(Value.ofBigInt(200L)),
                Assignment.column(COLUMN_NAME_4).value(Value.ofFloat(2.46F)))
            .build();
    resultSet = twoPhaseCommitTransactionSqlSession2.execute(insertStatement2);
    resultSet.close();

    // Update
    UpdateStatement updateStatement1 =
        SqlBuilder.update(NAMESPACE_NAME, TABLE_NAME)
            .set(
                Assignment.column(COLUMN_NAME_3).value(Value.ofBigInt(200L)),
                Assignment.column(COLUMN_NAME_4).value(Value.ofFloat(4.56F)))
            .where(Condition.column(COLUMN_NAME_1).isEqualTo(Value.ofInt(10)))
            .and(Condition.column(COLUMN_NAME_2).isEqualTo(Value.ofText("abc")))
            .build();
    resultSet = twoPhaseCommitTransactionSqlSession1.execute(updateStatement1);
    resultSet.close();

    UpdateStatement updateStatement2 =
        SqlBuilder.update(NAMESPACE_NAME, TABLE_NAME)
            .set(
                Assignment.column(COLUMN_NAME_3).value(Value.ofBigInt(300L)),
                Assignment.column(COLUMN_NAME_4).value(Value.ofFloat(7.89F)))
            .where(Condition.column(COLUMN_NAME_1).isEqualTo(Value.ofInt(11)))
            .and(Condition.column(COLUMN_NAME_2).isEqualTo(Value.ofText("def")))
            .build();
    resultSet = twoPhaseCommitTransactionSqlSession2.execute(updateStatement2);
    resultSet.close();

    // Select
    SelectStatement selectStatement1 =
        SqlBuilder.select(COLUMN_NAME_1, COLUMN_NAME_2, COLUMN_NAME_3)
            .from(NAMESPACE_NAME, TABLE_NAME)
            .where(Condition.column(COLUMN_NAME_1).isEqualTo(Value.ofInt(10)))
            .and(Condition.column(COLUMN_NAME_2).isEqualTo(Value.ofText("abc")))
            .build();

    try (ResultSet rs = twoPhaseCommitTransactionSqlSession1.execute(selectStatement1)) {
      for (Record record : rs) {
        System.out.println("column1 value: " + record.getInt(COLUMN_NAME_1));
        System.out.println("column2 value: " + record.getText(COLUMN_NAME_2));
        System.out.println("column3 value: " + record.getBigInt(COLUMN_NAME_3));
        System.out.println();
      }
    }

    SelectStatement selectStatement2 =
        SqlBuilder.select(COLUMN_NAME_1, COLUMN_NAME_2, COLUMN_NAME_3)
            .from(NAMESPACE_NAME, TABLE_NAME)
            .where(Condition.column(COLUMN_NAME_1).isEqualTo(Value.ofInt(11)))
            .and(Condition.column(COLUMN_NAME_2).isEqualTo(Value.ofText("def")))
            .build();

    try (ResultSet rs = twoPhaseCommitTransactionSqlSession2.execute(selectStatement2)) {
      for (Record record : rs) {
        System.out.println("column1 value: " + record.getInt(COLUMN_NAME_1));
        System.out.println("column2 value: " + record.getText(COLUMN_NAME_2));
        System.out.println("column3 value: " + record.getBigInt(COLUMN_NAME_3));
        System.out.println();
      }
    }

    // Delete
    DeleteStatement deleteStatement1 =
        SqlBuilder.deleteFrom(NAMESPACE_NAME, TABLE_NAME)
            .where(Condition.column(COLUMN_NAME_1).isEqualTo(Value.ofInt(10)))
            .and(Condition.column(COLUMN_NAME_2).isEqualTo(Value.ofText("abc")))
            .build();
    resultSet = twoPhaseCommitTransactionSqlSession1.execute(deleteStatement1);
    resultSet.close();

    DeleteStatement deleteStatement2 =
        SqlBuilder.deleteFrom(NAMESPACE_NAME, TABLE_NAME)
            .where(Condition.column(COLUMN_NAME_1).isEqualTo(Value.ofInt(11)))
            .and(Condition.column(COLUMN_NAME_2).isEqualTo(Value.ofText("def")))
            .build();
    resultSet = twoPhaseCommitTransactionSqlSession2.execute(deleteStatement2);
    resultSet.close();

    // Prepare
    twoPhaseCommitTransactionSqlSession1.prepare();
    twoPhaseCommitTransactionSqlSession2.prepare();

    // Commit
    twoPhaseCommitTransactionSqlSession1.commit();
    twoPhaseCommitTransactionSqlSession2.commit();

    // Drop table
    DropTableStatement dropTableStatement =
        SqlBuilder.dropTable(NAMESPACE_NAME, TABLE_NAME).ifExists().build();
    resultSet = storageSqlSession.execute(dropTableStatement);
    resultSet.close();

    dropTableStatement =
        SqlBuilder.dropTable(Coordinator.NAMESPACE, Coordinator.TABLE).ifExists().build();
    resultSet = storageSqlSession.execute(dropTableStatement);
    resultSet.close();

    // Drop namespace
    DropNamespaceStatement dropNamespaceStatement =
        SqlBuilder.dropNamespace(NAMESPACE_NAME).ifExists().cascade().build();
    resultSet = storageSqlSession.execute(dropNamespaceStatement);
    resultSet.close();

    dropNamespaceStatement =
        SqlBuilder.dropNamespace(Coordinator.NAMESPACE).ifExists().cascade().build();
    resultSet = storageSqlSession.execute(dropNamespaceStatement);
    resultSet.close();
  }
}
