package com.scalar.db.storage.jdbc.test;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.jdbc.JDBCUtils;
import com.scalar.db.storage.jdbc.RDBType;
import org.apache.commons.dbcp2.BasicDataSource;

import javax.sql.DataSource;
import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;

public class TestEnv implements Closeable {
  public static final RDBInfo MYSQL_RDB_INFO =
      new RDBInfo("jdbc:mysql://localhost:3306/", "root", "mysql");
  public static final RDBInfo POSTGRESQL_RDB_INFO =
      new RDBInfo("jdbc:postgresql://localhost:5432/", "postgres", "postgres");
  public static final RDBInfo ORACLE_RDB_INFO =
      new RDBInfo("jdbc:oracle:thin:@localhost:1521/ORACLE", "SYSTEM", "Oracle19");
  public static final RDBInfo SQLSERVER_RDB_INFO =
      new RDBInfo("jdbc:sqlserver://localhost:1433", "SA", "P@ssw0rd!");

  private final RDBInfo rdbInfo;
  private final String schemaPrefix;
  private final Statements statements;
  private final BasicDataSource dataSource;

  public TestEnv(RDBInfo rdbInfo, StatementsStrategy strategy) {
    this(rdbInfo, strategy, "");
  }

  public TestEnv(RDBInfo rdbInfo, StatementsStrategy strategy, String schemaPrefix) {
    this.rdbInfo = rdbInfo;
    this.schemaPrefix = schemaPrefix;

    RDBType rdbType = JDBCUtils.getRDBType(rdbInfo.jdbcJrl);
    switch (rdbType) {
      case MYSQL:
        statements = new MySQLStatements(strategy);
        break;
      case POSTGRESQL:
        statements = new PostgreSQLStatements(strategy);
        break;
      case ORACLE:
        statements = new OracleStatements(strategy);
        break;
      case SQLSERVER:
      default:
        statements = new SQLServerStatements(strategy);
        break;
    }

    dataSource = new BasicDataSource();
    dataSource.setUrl(rdbInfo.jdbcJrl);
    dataSource.setUsername(rdbInfo.username);
    dataSource.setPassword(rdbInfo.password);
    dataSource.setMinIdle(5);
    dataSource.setMaxIdle(10);
    dataSource.setMaxTotal(25);
  }

  private String schemaPrefix() {
    return schemaPrefix + (!schemaPrefix.isEmpty() ? "_" : "");
  }

  public void createMetadataTableAndInsertMetadata() throws SQLException {
    try (Connection connection = dataSource.getConnection();
        Statement stmt = connection.createStatement()) {
      executeAndIgnoreIfExceptionHappens(
          stmt, statements.dropMetadataTableStatements(schemaPrefix()));
      executeAndIgnoreIfExceptionHappens(
          stmt, statements.dropMetadataSchemaStatements(schemaPrefix()));
      execute(stmt, statements.createMetadataSchemaStatements(schemaPrefix()));
      execute(stmt, statements.createMetadataTableStatements(schemaPrefix()));
      execute(stmt, statements.insertMetadataStatements(schemaPrefix()));
    }
  }

  public void createDataTable() throws SQLException {
    try (Connection connection = dataSource.getConnection();
        Statement stmt = connection.createStatement()) {
      executeAndIgnoreIfExceptionHappens(stmt, statements.dropDataTableStatements(schemaPrefix()));
      executeAndIgnoreIfExceptionHappens(stmt, statements.dropDataSchemaStatements(schemaPrefix()));
      execute(stmt, statements.createDataSchemaStatements(schemaPrefix()));
      execute(stmt, statements.createDataTableStatements(schemaPrefix()));
    }
  }

  public void dropAllTablesAndSchemas() throws SQLException {
    try (Connection connection = dataSource.getConnection();
        Statement stmt = connection.createStatement()) {
      executeAndIgnoreIfExceptionHappens(stmt, statements.dropDataTableStatements(schemaPrefix()));
      executeAndIgnoreIfExceptionHappens(stmt, statements.dropDataSchemaStatements(schemaPrefix()));
      executeAndIgnoreIfExceptionHappens(
          stmt, statements.dropMetadataTableStatements(schemaPrefix()));
      executeAndIgnoreIfExceptionHappens(
          stmt, statements.dropMetadataSchemaStatements(schemaPrefix()));
    }
  }

  private void execute(Statement stmt, List<String> sqls) throws SQLException {
    for (String sql : sqls) {
      stmt.execute(sql);
    }
  }

  private void executeAndIgnoreIfExceptionHappens(Statement stmt, List<String> sqls) {
    try {
      for (String sql : sqls) {
        stmt.execute(sql);
      }
    } catch (SQLException ignored) {
    }
  }

  public DataSource getDataSource() {
    return dataSource;
  }

  public DatabaseConfig getDatabaseConfig() {
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, rdbInfo.jdbcJrl);
    props.setProperty(DatabaseConfig.USERNAME, rdbInfo.username);
    props.setProperty(DatabaseConfig.PASSWORD, rdbInfo.password);

    if (!schemaPrefix.isEmpty()) {
      props.setProperty(DatabaseConfig.NAMESPACE_PREFIX, schemaPrefix);
    }

    return new DatabaseConfig(props);
  }

  @Override
  public void close() throws IOException {
    try {
      dataSource.close();
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }
}
