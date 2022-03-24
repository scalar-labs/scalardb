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
    try (SqlSessionFactory sqlSessionFactory =
        SqlSessionFactory.builder()
            .addContactPoint("jdbc:mysql://localhost:3306/")
            .withUsername("root")
            .withPassword("mysql")
            .withStorage("jdbc")
            .build()) {
      transactionModeExample(sqlSessionFactory);
      twoPhaseCommitTransactionModeExample(sqlSessionFactory);
    }
  }

  public static void transactionModeExample(SqlSessionFactory sqlSessionFactory) {
    System.out.println("Start transaction mode example");

    SqlSession sqlSession = sqlSessionFactory.getTransactionSession();

    // Create a namespace
    sqlSession.execute(StatementBuilder.createNamespace(NAMESPACE_NAME).ifNotExists().build());

    // Create a table
    sqlSession.execute(
        StatementBuilder.createTable(NAMESPACE_NAME, TABLE_NAME)
            .ifNotExists()
            .withPartitionKey(COLUMN_NAME_1, DataType.INT)
            .withClusteringKey(COLUMN_NAME_2, DataType.TEXT)
            .withColumn(COLUMN_NAME_3, DataType.BIGINT)
            .withColumn(COLUMN_NAME_4, DataType.FLOAT)
            .withClusteringOrder(COLUMN_NAME_2, ClusteringOrder.ASC)
            .build());

    // Create a index
    sqlSession.execute(
        StatementBuilder.createIndex()
            .ifNotExists()
            .onTable(NAMESPACE_NAME, TABLE_NAME)
            .column(COLUMN_NAME_3)
            .build());

    // Create a coordinator table
    sqlSession.execute(StatementBuilder.createCoordinatorTable().ifNotExists().build());

    try {
      // Begin
      sqlSession.beginTransaction();

      // Insert
      sqlSession.execute(
          StatementBuilder.insertInto(NAMESPACE_NAME, TABLE_NAME)
              .values(
                  Assignment.column(COLUMN_NAME_1).value(Value.ofInt(10)),
                  Assignment.column(COLUMN_NAME_2).value(Value.ofText("abc")),
                  Assignment.column(COLUMN_NAME_3).value(Value.ofBigInt(100L)),
                  Assignment.column(COLUMN_NAME_4).value(Value.ofFloat(1.23F)))
              .build());

      // Update
      sqlSession.execute(
          StatementBuilder.update(NAMESPACE_NAME, TABLE_NAME)
              .set(
                  Assignment.column(COLUMN_NAME_3).value(Value.ofBigInt(200L)),
                  Assignment.column(COLUMN_NAME_4).value(Value.ofFloat(4.56F)))
              .where(Predicate.column(COLUMN_NAME_1).isEqualTo(Value.ofInt(10)))
              .and(Predicate.column(COLUMN_NAME_2).isEqualTo(Value.ofText("abc")))
              .build());

      // Select
      ResultSet resultSet =
          sqlSession.executeQuery(
              StatementBuilder.select(COLUMN_NAME_1, COLUMN_NAME_2, COLUMN_NAME_3)
                  .from(NAMESPACE_NAME, TABLE_NAME)
                  .where(Predicate.column(COLUMN_NAME_1).isEqualTo(Value.ofInt(10)))
                  .and(Predicate.column(COLUMN_NAME_2).isEqualTo(Value.ofText("abc")))
                  .orderBy(ClusteringOrdering.column(COLUMN_NAME_2).desc())
                  .limit(10)
                  .build());
      for (Record record : resultSet) {
        System.out.println("column1 value: " + record.getInt(COLUMN_NAME_1));
        System.out.println("column2 value: " + record.getText(COLUMN_NAME_2));
        System.out.println("column3 value: " + record.getBigInt(COLUMN_NAME_3));
        System.out.println();
      }

      // Delete
      sqlSession.execute(
          StatementBuilder.deleteFrom(NAMESPACE_NAME, TABLE_NAME)
              .where(Predicate.column(COLUMN_NAME_1).isEqualTo(Value.ofInt(10)))
              .and(Predicate.column(COLUMN_NAME_2).isEqualTo(Value.ofText("abc")))
              .build());

      // Commit
      sqlSession.commit();
    } catch (SqlException e) {
      // Rollback
      sqlSession.rollback();
    }

    // Truncate a table
    sqlSession.execute(StatementBuilder.truncateTable(NAMESPACE_NAME, TABLE_NAME).build());

    // Truncate a coordinator table
    sqlSession.execute(StatementBuilder.truncateCoordinatorTable().build());

    // Drop an index
    sqlSession.execute(
        StatementBuilder.dropIndex()
            .ifExists()
            .onTable(NAMESPACE_NAME, TABLE_NAME)
            .column(COLUMN_NAME_3)
            .build());

    // Drop a table
    sqlSession.execute(StatementBuilder.dropTable(NAMESPACE_NAME, TABLE_NAME).ifExists().build());

    // Drop a namespace
    sqlSession.execute(StatementBuilder.dropNamespace(NAMESPACE_NAME).ifExists().cascade().build());

    // Drop a coordinator table
    sqlSession.execute(StatementBuilder.dropCoordinatorTable().ifExists().build());
  }

  public static void twoPhaseCommitTransactionModeExample(SqlSessionFactory sqlSessionFactory) {
    System.out.println("Start two-phase commit transaction mode example");

    SqlSession sqlSession1 = sqlSessionFactory.getTwoPhaseCommitTransactionSession();
    SqlSession sqlSession2 = sqlSessionFactory.getTwoPhaseCommitTransactionSession();

    // Create a namespace
    sqlSession1.execute(StatementBuilder.createNamespace(NAMESPACE_NAME).ifNotExists().build());

    // Create a table
    sqlSession1.execute(
        StatementBuilder.createTable(NAMESPACE_NAME, TABLE_NAME)
            .ifNotExists()
            .withPartitionKey(COLUMN_NAME_1, DataType.INT)
            .withClusteringKey(COLUMN_NAME_2, DataType.TEXT)
            .withColumn(COLUMN_NAME_3, DataType.BIGINT)
            .withColumn(COLUMN_NAME_4, DataType.FLOAT)
            .withClusteringOrder(COLUMN_NAME_2, ClusteringOrder.ASC)
            .build());

    // Create a index
    sqlSession1.execute(
        StatementBuilder.createIndex()
            .ifNotExists()
            .onTable(NAMESPACE_NAME, TABLE_NAME)
            .column(COLUMN_NAME_3)
            .build());

    // Create a coordinator table
    sqlSession1.execute(StatementBuilder.createCoordinatorTable().ifNotExists().build());

    try {
      // Begin and join
      sqlSession1.beginTransaction();
      sqlSession2.joinTransaction(sqlSession1.getTransactionId());

      // Insert
      sqlSession1.execute(
          StatementBuilder.insertInto(NAMESPACE_NAME, TABLE_NAME)
              .values(
                  Assignment.column(COLUMN_NAME_1).value(Value.ofInt(10)),
                  Assignment.column(COLUMN_NAME_2).value(Value.ofText("abc")),
                  Assignment.column(COLUMN_NAME_3).value(Value.ofBigInt(100L)),
                  Assignment.column(COLUMN_NAME_4).value(Value.ofFloat(1.23F)))
              .build());

      sqlSession2.execute(
          StatementBuilder.insertInto(NAMESPACE_NAME, TABLE_NAME)
              .values(
                  Assignment.column(COLUMN_NAME_1).value(Value.ofInt(11)),
                  Assignment.column(COLUMN_NAME_2).value(Value.ofText("def")),
                  Assignment.column(COLUMN_NAME_3).value(Value.ofBigInt(200L)),
                  Assignment.column(COLUMN_NAME_4).value(Value.ofFloat(2.46F)))
              .build());

      // Update
      sqlSession1.execute(
          StatementBuilder.update(NAMESPACE_NAME, TABLE_NAME)
              .set(
                  Assignment.column(COLUMN_NAME_3).value(Value.ofBigInt(200L)),
                  Assignment.column(COLUMN_NAME_4).value(Value.ofFloat(4.56F)))
              .where(Predicate.column(COLUMN_NAME_1).isEqualTo(Value.ofInt(10)))
              .and(Predicate.column(COLUMN_NAME_2).isEqualTo(Value.ofText("abc")))
              .build());

      sqlSession2.execute(
          StatementBuilder.update(NAMESPACE_NAME, TABLE_NAME)
              .set(
                  Assignment.column(COLUMN_NAME_3).value(Value.ofBigInt(300L)),
                  Assignment.column(COLUMN_NAME_4).value(Value.ofFloat(7.89F)))
              .where(Predicate.column(COLUMN_NAME_1).isEqualTo(Value.ofInt(11)))
              .and(Predicate.column(COLUMN_NAME_2).isEqualTo(Value.ofText("def")))
              .build());

      // Select
      ResultSet resultSet1 =
          sqlSession1.executeQuery(
              StatementBuilder.select(COLUMN_NAME_1, COLUMN_NAME_2, COLUMN_NAME_3)
                  .from(NAMESPACE_NAME, TABLE_NAME)
                  .where(Predicate.column(COLUMN_NAME_1).isEqualTo(Value.ofInt(10)))
                  .and(Predicate.column(COLUMN_NAME_2).isEqualTo(Value.ofText("abc")))
                  .orderBy(ClusteringOrdering.column(COLUMN_NAME_2).desc())
                  .limit(10)
                  .build());
      for (Record record : resultSet1) {
        System.out.println("column1 value: " + record.getInt(COLUMN_NAME_1));
        System.out.println("column2 value: " + record.getText(COLUMN_NAME_2));
        System.out.println("column3 value: " + record.getBigInt(COLUMN_NAME_3));
        System.out.println();
      }

      ResultSet resultSet2 =
          sqlSession2.executeQuery(
              StatementBuilder.select(COLUMN_NAME_1, COLUMN_NAME_2, COLUMN_NAME_3)
                  .from(NAMESPACE_NAME, TABLE_NAME)
                  .where(Predicate.column(COLUMN_NAME_1).isEqualTo(Value.ofInt(11)))
                  .and(Predicate.column(COLUMN_NAME_2).isEqualTo(Value.ofText("def")))
                  .orderBy(ClusteringOrdering.column(COLUMN_NAME_2).desc())
                  .limit(10)
                  .build());
      for (Record record : resultSet2) {
        System.out.println("column1 value: " + record.getInt(COLUMN_NAME_1));
        System.out.println("column2 value: " + record.getText(COLUMN_NAME_2));
        System.out.println("column3 value: " + record.getBigInt(COLUMN_NAME_3));
        System.out.println();
      }

      // Delete
      sqlSession1.execute(
          StatementBuilder.deleteFrom(NAMESPACE_NAME, TABLE_NAME)
              .where(Predicate.column(COLUMN_NAME_1).isEqualTo(Value.ofInt(10)))
              .and(Predicate.column(COLUMN_NAME_2).isEqualTo(Value.ofText("abc")))
              .build());

      sqlSession2.execute(
          StatementBuilder.deleteFrom(NAMESPACE_NAME, TABLE_NAME)
              .where(Predicate.column(COLUMN_NAME_1).isEqualTo(Value.ofInt(11)))
              .and(Predicate.column(COLUMN_NAME_2).isEqualTo(Value.ofText("def")))
              .build());

      // Prepare
      sqlSession1.prepare();
      sqlSession2.prepare();

      // Validate
      sqlSession1.validate();
      sqlSession2.validate();

      // Commit
      sqlSession1.commit();
      sqlSession2.commit();
    } catch (SqlException e) {
      // Rollback
      sqlSession1.rollback();
      sqlSession2.rollback();
    }

    // Truncate a table
    sqlSession1.execute(StatementBuilder.truncateTable(NAMESPACE_NAME, TABLE_NAME).build());

    // Truncate a coordinator table
    sqlSession1.execute(StatementBuilder.truncateCoordinatorTable().build());

    // Drop an index
    sqlSession1.execute(
        StatementBuilder.dropIndex()
            .ifExists()
            .onTable(NAMESPACE_NAME, TABLE_NAME)
            .column(COLUMN_NAME_3)
            .build());

    // Drop a table
    sqlSession1.execute(StatementBuilder.dropTable(NAMESPACE_NAME, TABLE_NAME).ifExists().build());

    // Drop a namespace
    sqlSession1.execute(
        StatementBuilder.dropNamespace(NAMESPACE_NAME).ifExists().cascade().build());

    // Drop a coordinator table
    sqlSession1.execute(StatementBuilder.dropCoordinatorTable().ifExists().build());
  }
}
