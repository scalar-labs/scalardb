package com.scalar.db.sql;

import com.scalar.db.sql.builder.StatementBuilder;
import com.scalar.db.sql.exception.SqlException;

public class Example {

  private static final String NAMESPACE_NAME = "ns";
  private static final String TABLE_NAME = "tbl";
  private static final String COLUMN_NAME_1 = "col1";
  private static final String COLUMN_NAME_2 = "col2";
  private static final String COLUMN_NAME_3 = "col3";
  private static final String COLUMN_NAME_4 = "col4";

  public static void main(String[] args) {
    try (SessionFactory sessionFactory =
        SessionFactory.builder()
            .withProperty("scalar.db.contact_points", "jdbc:mysql://localhost:3306/")
            .withProperty("scalar.db.username", "root")
            .withProperty("scalar.db.password", "mysql")
            .withProperty("scalar.db.storage", "jdbc")
            .build()) {
      transactionModeExample(sessionFactory);
      twoPhaseCommitTransactionModeExample(sessionFactory);
    }
  }

  public static void transactionModeExample(SessionFactory sessionFactory) {
    System.out.println("Start transaction mode example");

    Session session = sessionFactory.getTransactionSession();

    // Create a namespace
    session.execute(StatementBuilder.createNamespace(NAMESPACE_NAME).ifNotExists().build());

    // Create a table
    session.execute(
        StatementBuilder.createTable(NAMESPACE_NAME, TABLE_NAME)
            .ifNotExists()
            .withPartitionKey(COLUMN_NAME_1, DataType.INT)
            .withClusteringKey(COLUMN_NAME_2, DataType.TEXT)
            .withColumn(COLUMN_NAME_3, DataType.BIGINT)
            .withColumn(COLUMN_NAME_4, DataType.FLOAT)
            .withClusteringOrder(COLUMN_NAME_2, Order.ASC)
            .build());

    // Create a index
    session.execute(
        StatementBuilder.createIndex()
            .ifNotExists()
            .onTable(NAMESPACE_NAME, TABLE_NAME)
            .column(COLUMN_NAME_3)
            .build());

    // Create a coordinator table
    session.execute(StatementBuilder.createCoordinatorTable().ifNotExists().build());

    try {
      // Begin
      session.beginTransaction();

      // Insert
      session.execute(
          StatementBuilder.insertInto(NAMESPACE_NAME, TABLE_NAME)
              .values(
                  Assignment.column(COLUMN_NAME_1).value(Value.ofInt(10)),
                  Assignment.column(COLUMN_NAME_2).value(Value.ofText("abc")),
                  Assignment.column(COLUMN_NAME_3).value(Value.ofBigInt(100L)),
                  Assignment.column(COLUMN_NAME_4).value(Value.ofFloat(1.23F)))
              .build());

      // Update
      session.execute(
          StatementBuilder.update(NAMESPACE_NAME, TABLE_NAME)
              .set(
                  Assignment.column(COLUMN_NAME_3).value(Value.ofBigInt(200L)),
                  Assignment.column(COLUMN_NAME_4).value(Value.ofFloat(4.56F)))
              .where(Condition.column(COLUMN_NAME_1).isEqualTo(Value.ofInt(10)))
              .and(Condition.column(COLUMN_NAME_2).isEqualTo(Value.ofText("abc")))
              .build());

      // Select
      ResultSet resultSet =
          session.executeQuery(
              StatementBuilder.select(COLUMN_NAME_1, COLUMN_NAME_2, COLUMN_NAME_3)
                  .from(NAMESPACE_NAME, TABLE_NAME)
                  .where(Condition.column(COLUMN_NAME_1).isEqualTo(Value.ofInt(10)))
                  .and(Condition.column(COLUMN_NAME_2).isEqualTo(Value.ofText("abc")))
                  .build());
      for (Record record : resultSet) {
        System.out.println("column1 value: " + record.getInt(COLUMN_NAME_1));
        System.out.println("column2 value: " + record.getText(COLUMN_NAME_2));
        System.out.println("column3 value: " + record.getBigInt(COLUMN_NAME_3));
        System.out.println();
      }

      // Delete
      session.execute(
          StatementBuilder.deleteFrom(NAMESPACE_NAME, TABLE_NAME)
              .where(Condition.column(COLUMN_NAME_1).isEqualTo(Value.ofInt(10)))
              .and(Condition.column(COLUMN_NAME_2).isEqualTo(Value.ofText("abc")))
              .build());

      // Commit
      session.commit();
    } catch (SqlException e) {
      // Rollback
      session.rollback();
    }

    // Truncate a table
    session.execute(StatementBuilder.truncateTable(NAMESPACE_NAME, TABLE_NAME).build());

    // Truncate a coordinator table
    session.execute(StatementBuilder.truncateCoordinatorTable().build());

    // Drop an index
    session.execute(
        StatementBuilder.dropIndex()
            .ifExists()
            .onTable(NAMESPACE_NAME, TABLE_NAME)
            .column(COLUMN_NAME_3)
            .build());

    // Drop a table
    session.execute(StatementBuilder.dropTable(NAMESPACE_NAME, TABLE_NAME).ifExists().build());

    // Drop a namespace
    session.execute(StatementBuilder.dropNamespace(NAMESPACE_NAME).ifExists().cascade().build());

    // Drop a coordinator table
    session.execute(StatementBuilder.dropCoordinatorTable().ifExists().build());
  }

  public static void twoPhaseCommitTransactionModeExample(SessionFactory sessionFactory) {
    System.out.println("Start two-phase commit transaction mode example");

    Session session1 = sessionFactory.getTwoPhaseCommitTransactionSession();
    Session session2 = sessionFactory.getTwoPhaseCommitTransactionSession();

    // Create a namespace
    session1.execute(StatementBuilder.createNamespace(NAMESPACE_NAME).ifNotExists().build());

    // Create a table
    session1.execute(
        StatementBuilder.createTable(NAMESPACE_NAME, TABLE_NAME)
            .ifNotExists()
            .withPartitionKey(COLUMN_NAME_1, DataType.INT)
            .withClusteringKey(COLUMN_NAME_2, DataType.TEXT)
            .withColumn(COLUMN_NAME_3, DataType.BIGINT)
            .withColumn(COLUMN_NAME_4, DataType.FLOAT)
            .withClusteringOrder(COLUMN_NAME_2, Order.ASC)
            .build());

    // Create a index
    session1.execute(
        StatementBuilder.createIndex()
            .ifNotExists()
            .onTable(NAMESPACE_NAME, TABLE_NAME)
            .column(COLUMN_NAME_3)
            .build());

    // Create a coordinator table
    session1.execute(StatementBuilder.createCoordinatorTable().ifNotExists().build());

    try {
      // Begin and join
      session1.beginTransaction();
      session2.joinTransaction(session1.getTransactionId());

      // Insert
      session1.execute(
          StatementBuilder.insertInto(NAMESPACE_NAME, TABLE_NAME)
              .values(
                  Assignment.column(COLUMN_NAME_1).value(Value.ofInt(10)),
                  Assignment.column(COLUMN_NAME_2).value(Value.ofText("abc")),
                  Assignment.column(COLUMN_NAME_3).value(Value.ofBigInt(100L)),
                  Assignment.column(COLUMN_NAME_4).value(Value.ofFloat(1.23F)))
              .build());

      session2.execute(
          StatementBuilder.insertInto(NAMESPACE_NAME, TABLE_NAME)
              .values(
                  Assignment.column(COLUMN_NAME_1).value(Value.ofInt(11)),
                  Assignment.column(COLUMN_NAME_2).value(Value.ofText("def")),
                  Assignment.column(COLUMN_NAME_3).value(Value.ofBigInt(200L)),
                  Assignment.column(COLUMN_NAME_4).value(Value.ofFloat(2.46F)))
              .build());

      // Update
      session1.execute(
          StatementBuilder.update(NAMESPACE_NAME, TABLE_NAME)
              .set(
                  Assignment.column(COLUMN_NAME_3).value(Value.ofBigInt(200L)),
                  Assignment.column(COLUMN_NAME_4).value(Value.ofFloat(4.56F)))
              .where(Condition.column(COLUMN_NAME_1).isEqualTo(Value.ofInt(10)))
              .and(Condition.column(COLUMN_NAME_2).isEqualTo(Value.ofText("abc")))
              .build());

      session2.execute(
          StatementBuilder.update(NAMESPACE_NAME, TABLE_NAME)
              .set(
                  Assignment.column(COLUMN_NAME_3).value(Value.ofBigInt(300L)),
                  Assignment.column(COLUMN_NAME_4).value(Value.ofFloat(7.89F)))
              .where(Condition.column(COLUMN_NAME_1).isEqualTo(Value.ofInt(11)))
              .and(Condition.column(COLUMN_NAME_2).isEqualTo(Value.ofText("def")))
              .build());

      // Select
      ResultSet resultSet1 =
          session1.executeQuery(
              StatementBuilder.select(COLUMN_NAME_1, COLUMN_NAME_2, COLUMN_NAME_3)
                  .from(NAMESPACE_NAME, TABLE_NAME)
                  .where(Condition.column(COLUMN_NAME_1).isEqualTo(Value.ofInt(10)))
                  .and(Condition.column(COLUMN_NAME_2).isEqualTo(Value.ofText("abc")))
                  .build());
      for (Record record : resultSet1) {
        System.out.println("column1 value: " + record.getInt(COLUMN_NAME_1));
        System.out.println("column2 value: " + record.getText(COLUMN_NAME_2));
        System.out.println("column3 value: " + record.getBigInt(COLUMN_NAME_3));
        System.out.println();
      }

      ResultSet resultSet2 =
          session2.executeQuery(
              StatementBuilder.select(COLUMN_NAME_1, COLUMN_NAME_2, COLUMN_NAME_3)
                  .from(NAMESPACE_NAME, TABLE_NAME)
                  .where(Condition.column(COLUMN_NAME_1).isEqualTo(Value.ofInt(11)))
                  .and(Condition.column(COLUMN_NAME_2).isEqualTo(Value.ofText("def")))
                  .build());
      for (Record record : resultSet2) {
        System.out.println("column1 value: " + record.getInt(COLUMN_NAME_1));
        System.out.println("column2 value: " + record.getText(COLUMN_NAME_2));
        System.out.println("column3 value: " + record.getBigInt(COLUMN_NAME_3));
        System.out.println();
      }

      // Delete
      session1.execute(
          StatementBuilder.deleteFrom(NAMESPACE_NAME, TABLE_NAME)
              .where(Condition.column(COLUMN_NAME_1).isEqualTo(Value.ofInt(10)))
              .and(Condition.column(COLUMN_NAME_2).isEqualTo(Value.ofText("abc")))
              .build());

      session2.execute(
          StatementBuilder.deleteFrom(NAMESPACE_NAME, TABLE_NAME)
              .where(Condition.column(COLUMN_NAME_1).isEqualTo(Value.ofInt(11)))
              .and(Condition.column(COLUMN_NAME_2).isEqualTo(Value.ofText("def")))
              .build());

      // Prepare
      session1.prepare();
      session2.prepare();

      // Validate
      session1.validate();
      session2.validate();

      // Commit
      session1.commit();
      session2.commit();
    } catch (SqlException e) {
      // Rollback
      session1.rollback();
      session2.rollback();
    }

    // Truncate a table
    session1.execute(StatementBuilder.truncateTable(NAMESPACE_NAME, TABLE_NAME).build());

    // Truncate a coordinator table
    session1.execute(StatementBuilder.truncateCoordinatorTable().build());

    // Drop an index
    session1.execute(
        StatementBuilder.dropIndex()
            .ifExists()
            .onTable(NAMESPACE_NAME, TABLE_NAME)
            .column(COLUMN_NAME_3)
            .build());

    // Drop a table
    session1.execute(StatementBuilder.dropTable(NAMESPACE_NAME, TABLE_NAME).ifExists().build());

    // Drop a namespace
    session1.execute(StatementBuilder.dropNamespace(NAMESPACE_NAME).ifExists().cascade().build());

    // Drop a coordinator table
    session1.execute(StatementBuilder.dropCoordinatorTable().ifExists().build());
  }
}
