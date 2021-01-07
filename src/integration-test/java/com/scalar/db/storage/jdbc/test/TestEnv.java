package com.scalar.db.storage.jdbc.test;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.jdbc.JdbcUtils;
import com.scalar.db.storage.jdbc.RdbEngine;
import org.apache.commons.dbcp2.BasicDataSource;

import javax.sql.DataSource;
import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

public class TestEnv implements Closeable {
  public static final JdbcConnectionInfo MYSQL_INFO =
      new JdbcConnectionInfo("jdbc:mysql://localhost:3306/", "root", "mysql");
  public static final JdbcConnectionInfo POSTGRESQL_INFO =
      new JdbcConnectionInfo("jdbc:postgresql://localhost:5432/", "postgres", "postgres");
  public static final JdbcConnectionInfo ORACLE_INFO =
      new JdbcConnectionInfo("jdbc:oracle:thin:@localhost:1521/ORACLE", "SYSTEM", "Oracle19");
  public static final JdbcConnectionInfo SQL_SERVER_INFO =
      new JdbcConnectionInfo("jdbc:sqlserver://localhost:1433", "SA", "P@ssw0rd!");

  private final JdbcConnectionInfo jdbcConnectionInfo;
  private final Optional<String> namespacePrefix;
  private final Statements statements;
  private final BasicDataSource dataSource;

  public TestEnv(
      JdbcConnectionInfo jdbcConnectionInfo,
      BaseStatements baseStatements,
      Optional<String> namespacePrefix) {
    this.jdbcConnectionInfo = jdbcConnectionInfo;
    this.namespacePrefix = namespacePrefix;

    RdbEngine rdbEngine = JdbcUtils.getRdbEngine(jdbcConnectionInfo.url);
    switch (rdbEngine) {
      case MYSQL:
        statements = new MySqlStatements(baseStatements);
        break;
      case POSTGRESQL:
        statements = new PostgreSqlStatements(baseStatements);
        break;
      case ORACLE:
        statements = new OracleStatements(baseStatements);
        break;
      case SQL_SERVER:
      default:
        statements = new SqlServerStatements(baseStatements);
        break;
    }

    dataSource = new BasicDataSource();
    dataSource.setUrl(jdbcConnectionInfo.url);
    dataSource.setUsername(jdbcConnectionInfo.username);
    dataSource.setPassword(jdbcConnectionInfo.password);
    dataSource.setMinIdle(5);
    dataSource.setMaxIdle(10);
    dataSource.setMaxTotal(25);
  }

  private Optional<String> namespacePrefix() {
    return namespacePrefix.map(prefix -> prefix + "_");
  }

  public void createMetadataTableAndInsertMetadata() throws SQLException {
    try (Connection connection = dataSource.getConnection();
        Statement stmt = connection.createStatement()) {
      executeAndIgnoreIfExceptionHappens(
          stmt, statements.dropMetadataTableStatements(namespacePrefix()));
      executeAndIgnoreIfExceptionHappens(
          stmt, statements.dropMetadataSchemaStatements(namespacePrefix()));
      execute(stmt, statements.createMetadataSchemaStatements(namespacePrefix()));
      execute(stmt, statements.createMetadataTableStatements(namespacePrefix()));
      execute(stmt, statements.insertMetadataStatements(namespacePrefix()));
    }
  }

  public void createTables() throws SQLException {
    try (Connection connection = dataSource.getConnection();
        Statement stmt = connection.createStatement()) {
      executeAndIgnoreIfExceptionHappens(stmt, statements.dropTableStatements(namespacePrefix()));
      executeAndIgnoreIfExceptionHappens(stmt, statements.dropSchemaStatements(namespacePrefix()));
      execute(stmt, statements.createSchemaStatements(namespacePrefix()));
      execute(stmt, statements.createTableStatements(namespacePrefix()));
    }
  }

  public void dropAllTablesAndSchemas() throws SQLException {
    try (Connection connection = dataSource.getConnection();
        Statement stmt = connection.createStatement()) {
      executeAndIgnoreIfExceptionHappens(stmt, statements.dropTableStatements(namespacePrefix()));
      executeAndIgnoreIfExceptionHappens(stmt, statements.dropSchemaStatements(namespacePrefix()));
      executeAndIgnoreIfExceptionHappens(
          stmt, statements.dropMetadataTableStatements(namespacePrefix()));
      executeAndIgnoreIfExceptionHappens(
          stmt, statements.dropMetadataSchemaStatements(namespacePrefix()));
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
    props.setProperty(DatabaseConfig.CONTACT_POINTS, jdbcConnectionInfo.url);
    props.setProperty(DatabaseConfig.USERNAME, jdbcConnectionInfo.username);
    props.setProperty(DatabaseConfig.PASSWORD, jdbcConnectionInfo.password);

    namespacePrefix.ifPresent(s -> props.setProperty(DatabaseConfig.NAMESPACE_PREFIX, s));

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
