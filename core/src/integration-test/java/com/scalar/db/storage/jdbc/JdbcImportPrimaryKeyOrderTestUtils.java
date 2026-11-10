package com.scalar.db.storage.jdbc;

import com.google.common.collect.ImmutableList;
import com.scalar.db.config.DatabaseConfig;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.commons.dbcp2.BasicDataSource;

/**
 * Test utilities for importing a table whose primary key order differs from both the column name
 * order and the column declaration order.
 */
class JdbcImportPrimaryKeyOrderTestUtils implements AutoCloseable {
  static final String TABLE = "test_table";
  static final String PK_A = "pk_a";
  static final String PK_B = "pk_b";
  static final String PK_C = "pk_c";
  static final String COL = "col";

  /**
   * The primary key order of the table. The column name order is (pk_a, pk_b, pk_c), its reverse is
   * (pk_c, pk_b, pk_a), and the declaration order of the key columns is (pk_b, pk_c, pk_a), so none
   * of them matches this order.
   */
  static final ImmutableList<String> PRIMARY_KEY_ORDER = ImmutableList.of(PK_C, PK_A, PK_B);

  private final RdbEngineStrategy rdbEngine;
  private final BasicDataSource dataSource;

  JdbcImportPrimaryKeyOrderTestUtils(Properties properties) {
    JdbcConfig config = new JdbcConfig(new DatabaseConfig(properties));
    rdbEngine = RdbEngineFactory.create(config);
    dataSource = JdbcUtils.initDataSourceForAdmin(config, rdbEngine);
  }

  void createSchemaAndTable(String namespace) throws SQLException {
    execute(rdbEngine.createNamespaceSqls(rdbEngine.enclose(namespace)));
    // VARCHAR is used instead of CHAR because Oracle does not blank-pad a CHAR column when it is
    // compared with a VARCHAR2 value, which is how ScalarDB binds TEXT values
    execute(
        "CREATE TABLE "
            + rdbEngine.encloseFullTableName(namespace, TABLE)
            + "("
            + rdbEngine.enclose(PK_B)
            + " VARCHAR(8) NOT NULL,"
            + rdbEngine.enclose(PK_C)
            + " VARCHAR(8) NOT NULL,"
            + rdbEngine.enclose(PK_A)
            + " VARCHAR(8) NOT NULL,"
            + rdbEngine.enclose(COL)
            + " VARCHAR(8),"
            + " PRIMARY KEY("
            + PRIMARY_KEY_ORDER.stream().map(rdbEngine::enclose).collect(Collectors.joining(","))
            + "))");
  }

  void dropTable(String namespace, String table) throws SQLException {
    execute("DROP TABLE " + rdbEngine.encloseFullTableName(namespace, table));
  }

  private void execute(String sql) throws SQLException {
    try (Connection connection = dataSource.getConnection()) {
      JdbcAdmin.execute(connection, sql);
    }
  }

  private void execute(String[] sqls) throws SQLException {
    try (Connection connection = dataSource.getConnection()) {
      JdbcAdmin.execute(connection, sqls);
    }
  }

  @Override
  public void close() throws SQLException {
    dataSource.close();
  }
}
