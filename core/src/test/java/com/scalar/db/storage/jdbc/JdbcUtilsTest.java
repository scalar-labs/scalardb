package com.scalar.db.storage.jdbc;

import static com.scalar.db.storage.jdbc.JdbcUtils.createHikariConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.jdbc.RdbEngineStrategy.UnderlyingDataSourceConfig;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.util.IsolationLevel;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;
import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class JdbcUtilsTest {

  @Mock private RdbEngineStrategy rdbEngine;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
  }

  @Test
  public void initDataSource_NonTransactional_ShouldReturnProperDataSource() throws SQLException {
    // Arrange
    Properties properties = new Properties();
    properties.setProperty(DatabaseConfig.CONTACT_POINTS, "jdbc:mysql://localhost:3306/");
    properties.setProperty(DatabaseConfig.USERNAME, "root");
    properties.setProperty(DatabaseConfig.PASSWORD, "mysql");
    properties.setProperty(DatabaseConfig.STORAGE, "jdbc");
    properties.setProperty(JdbcConfig.ISOLATION_LEVEL, "SERIALIZABLE");
    properties.setProperty(JdbcConfig.CONNECTION_POOL_MIN_IDLE, "10");
    properties.setProperty(JdbcConfig.CONNECTION_POOL_MAX_IDLE, "20");
    properties.setProperty(JdbcConfig.CONNECTION_POOL_MAX_TOTAL, "30");
    properties.setProperty(JdbcConfig.PREPARED_STATEMENTS_POOL_ENABLED, "true");
    properties.setProperty(JdbcConfig.PREPARED_STATEMENTS_POOL_MAX_OPEN, "100");

    JdbcConfig config = new JdbcConfig(new DatabaseConfig(properties));
    Driver driver = new com.mysql.cj.jdbc.Driver();
    when(rdbEngine.getDriver()).thenReturn(driver);

    // Act
    AutoCloseableDataSource ds = JdbcUtils.initDataSource(config, rdbEngine);

    // Assert
    assertThat(ds.underlyingDataSource()).isInstanceOf(BasicDataSource.class);
    BasicDataSource dataSource = (BasicDataSource) ds.underlyingDataSource();

    assertThat(dataSource.getDriver()).isEqualTo(driver);
    assertThat(dataSource.getUrl()).isEqualTo("jdbc:mysql://localhost:3306/");
    assertThat(dataSource.getUsername()).isEqualTo("root");
    assertThat(dataSource.getPassword()).isEqualTo("mysql");

    assertThat(dataSource.getDefaultAutoCommit()).isEqualTo(null);
    assertThat(dataSource.getAutoCommitOnReturn()).isEqualTo(true);
    assertThat(dataSource.getDefaultTransactionIsolation())
        .isEqualTo(Connection.TRANSACTION_SERIALIZABLE);

    assertThat(dataSource.getMinIdle()).isEqualTo(10);
    assertThat(dataSource.getMaxIdle()).isEqualTo(20);
    assertThat(dataSource.getMaxTotal()).isEqualTo(30);
    assertThat(dataSource.isPoolPreparedStatements()).isEqualTo(true);
    assertThat(dataSource.getMaxOpenPreparedStatements()).isEqualTo(100);

    dataSource.close();
  }

  @Test
  public void initDataSource_Transactional_ShouldReturnProperDataSource() throws SQLException {
    // Arrange
    Properties properties = new Properties();
    properties.setProperty(DatabaseConfig.CONTACT_POINTS, "jdbc:postgresql://localhost:5432/");
    properties.setProperty(DatabaseConfig.USERNAME, "user");
    properties.setProperty(DatabaseConfig.PASSWORD, "postgres");
    properties.setProperty(DatabaseConfig.STORAGE, "jdbc");
    properties.setProperty(JdbcConfig.ISOLATION_LEVEL, "READ_COMMITTED");
    properties.setProperty(JdbcConfig.CONNECTION_POOL_MIN_IDLE, "30");
    properties.setProperty(JdbcConfig.CONNECTION_POOL_MAX_IDLE, "40");
    properties.setProperty(JdbcConfig.CONNECTION_POOL_MAX_TOTAL, "50");
    properties.setProperty(JdbcConfig.PREPARED_STATEMENTS_POOL_ENABLED, "true");
    properties.setProperty(JdbcConfig.PREPARED_STATEMENTS_POOL_MAX_OPEN, "200");

    JdbcConfig config = new JdbcConfig(new DatabaseConfig(properties));
    Driver driver = new org.postgresql.Driver();
    when(rdbEngine.getDriver()).thenReturn(driver);

    // Act
    AutoCloseableDataSource ds = JdbcUtils.initDataSource(config, rdbEngine, true);

    // Assert
    assertThat(ds.underlyingDataSource()).isInstanceOf(BasicDataSource.class);
    BasicDataSource dataSource = (BasicDataSource) ds.underlyingDataSource();

    assertThat(dataSource.getDriver()).isEqualTo(driver);
    assertThat(dataSource.getUrl()).isEqualTo("jdbc:postgresql://localhost:5432/");
    assertThat(dataSource.getUsername()).isEqualTo("user");
    assertThat(dataSource.getPassword()).isEqualTo("postgres");

    assertThat(dataSource.getDefaultAutoCommit()).isEqualTo(false);
    assertThat(dataSource.getAutoCommitOnReturn()).isEqualTo(false);
    assertThat(dataSource.getDefaultTransactionIsolation())
        .isEqualTo(Connection.TRANSACTION_READ_COMMITTED);

    assertThat(dataSource.getMinIdle()).isEqualTo(30);
    assertThat(dataSource.getMaxIdle()).isEqualTo(40);
    assertThat(dataSource.getMaxTotal()).isEqualTo(50);
    assertThat(dataSource.isPoolPreparedStatements()).isEqualTo(true);
    assertThat(dataSource.getMaxOpenPreparedStatements()).isEqualTo(200);

    dataSource.close();
  }

  @Test
  public void initDataSourceForTableMetadata_ShouldReturnProperDataSource() throws SQLException {
    // Arrange
    Properties properties = new Properties();
    properties.setProperty(
        DatabaseConfig.CONTACT_POINTS, "jdbc:oracle:thin:@localhost:1521/XEPDB1");
    properties.setProperty(DatabaseConfig.USERNAME, "user");
    properties.setProperty(DatabaseConfig.PASSWORD, "oracle");
    properties.setProperty(DatabaseConfig.STORAGE, "jdbc");
    properties.setProperty(JdbcConfig.TABLE_METADATA_CONNECTION_POOL_MIN_IDLE, "100");
    properties.setProperty(JdbcConfig.TABLE_METADATA_CONNECTION_POOL_MAX_IDLE, "200");
    properties.setProperty(JdbcConfig.TABLE_METADATA_CONNECTION_POOL_MAX_TOTAL, "300");

    JdbcConfig config = new JdbcConfig(new DatabaseConfig(properties));
    Driver driver = new oracle.jdbc.driver.OracleDriver();
    when(rdbEngine.getDriver()).thenReturn(driver);

    // Act
    AutoCloseableDataSource ds = JdbcUtils.initDataSourceForTableMetadata(config, rdbEngine);

    // Assert
    assertThat(ds.underlyingDataSource()).isInstanceOf(BasicDataSource.class);
    BasicDataSource tableMetadataDataSource = (BasicDataSource) ds.underlyingDataSource();

    assertThat(tableMetadataDataSource.getDriver()).isEqualTo(driver);
    assertThat(tableMetadataDataSource.getUrl())
        .isEqualTo("jdbc:oracle:thin:@localhost:1521/XEPDB1");
    assertThat(tableMetadataDataSource.getUsername()).isEqualTo("user");
    assertThat(tableMetadataDataSource.getPassword()).isEqualTo("oracle");

    assertThat(tableMetadataDataSource.getMinIdle()).isEqualTo(100);
    assertThat(tableMetadataDataSource.getMaxIdle()).isEqualTo(200);
    assertThat(tableMetadataDataSource.getMaxTotal()).isEqualTo(300);

    tableMetadataDataSource.close();
  }

  @Test
  public void initDataSourceForAdmin_ShouldReturnProperDataSource() throws SQLException {
    // Arrange
    Properties properties = new Properties();
    properties.setProperty(DatabaseConfig.CONTACT_POINTS, "jdbc:sqlserver://localhost:1433");
    properties.setProperty(DatabaseConfig.USERNAME, "user");
    properties.setProperty(DatabaseConfig.PASSWORD, "sqlserver");
    properties.setProperty(DatabaseConfig.STORAGE, "jdbc");
    properties.setProperty(JdbcConfig.ADMIN_CONNECTION_POOL_MIN_IDLE, "100");
    properties.setProperty(JdbcConfig.ADMIN_CONNECTION_POOL_MAX_IDLE, "200");
    properties.setProperty(JdbcConfig.ADMIN_CONNECTION_POOL_MAX_TOTAL, "300");

    JdbcConfig config = new JdbcConfig(new DatabaseConfig(properties));
    Driver driver = new com.microsoft.sqlserver.jdbc.SQLServerDriver();
    when(rdbEngine.getDriver()).thenReturn(driver);

    // Act
    AutoCloseableDataSource ds = JdbcUtils.initDataSourceForAdmin(config, rdbEngine);

    // Assert
    assertThat(ds.underlyingDataSource()).isInstanceOf(BasicDataSource.class);
    BasicDataSource adminDataSource = (BasicDataSource) ds.underlyingDataSource();

    assertThat(adminDataSource.getDriver()).isEqualTo(driver);
    assertThat(adminDataSource.getUrl()).isEqualTo("jdbc:sqlserver://localhost:1433");
    assertThat(adminDataSource.getUsername()).isEqualTo("user");
    assertThat(adminDataSource.getPassword()).isEqualTo("sqlserver");

    assertThat(adminDataSource.getMinIdle()).isEqualTo(100);
    assertThat(adminDataSource.getMaxIdle()).isEqualTo(200);
    assertThat(adminDataSource.getMaxTotal()).isEqualTo(300);

    adminDataSource.close();
  }

  @Test
  void createHikariConfig_ForTransactionManagerWithTransactional_ShouldReturnProperConfig() {
    // Arrange
    Properties props = new Properties();
    props.setProperty(
        DatabaseConfig.CONTACT_POINTS,
        "jdbc:yugabytedb://127.0.0.1:5433/mydb?additionalEndpoints=127.0.0.2:5433\\,127.0.0.3:5433");
    props.setProperty(DatabaseConfig.USERNAME, "my-user");
    props.setProperty(DatabaseConfig.PASSWORD, "my-password");
    props.setProperty(DatabaseConfig.STORAGE, "jdbc");
    props.setProperty(DatabaseConfig.SYSTEM_NAMESPACE_NAME, "my-namespace");
    props.setProperty(JdbcConfig.CONNECTION_POOL_MIN_IDLE, "1");
    props.setProperty(JdbcConfig.CONNECTION_POOL_MAX_IDLE, "100");
    props.setProperty(JdbcConfig.CONNECTION_POOL_MAX_TOTAL, "500");
    props.setProperty(JdbcConfig.PREPARED_STATEMENTS_POOL_ENABLED, "true");
    props.setProperty(JdbcConfig.PREPARED_STATEMENTS_POOL_MAX_OPEN, "300");
    props.setProperty(JdbcConfig.ISOLATION_LEVEL, Isolation.REPEATABLE_READ.name());
    props.setProperty(JdbcConfig.TABLE_METADATA_CONNECTION_POOL_MIN_IDLE, "100");
    props.setProperty(JdbcConfig.TABLE_METADATA_CONNECTION_POOL_MAX_IDLE, "200");
    props.setProperty(JdbcConfig.TABLE_METADATA_CONNECTION_POOL_MAX_TOTAL, "300");
    props.setProperty(JdbcConfig.ADMIN_CONNECTION_POOL_MIN_IDLE, "50");
    props.setProperty(JdbcConfig.ADMIN_CONNECTION_POOL_MAX_IDLE, "150");
    props.setProperty(JdbcConfig.ADMIN_CONNECTION_POOL_MAX_TOTAL, "200");
    JdbcConfig jdbcConfig = new JdbcConfig(new DatabaseConfig(props));

    UnderlyingDataSourceConfig underlyingDataSourceConfig =
        new UnderlyingDataSourceConfig(
            "com.yugabyte.ysql.YBClusterAwareDataSource",
            ImmutableMap.<String, String>builder().put("portNumber", "5433").build());

    // Act
    HikariConfig hikariConfig =
        createHikariConfig(jdbcConfig, underlyingDataSourceConfig, true, true);

    // Assert
    assertThat(hikariConfig.getDataSourceClassName())
        .isEqualTo("com.yugabyte.ysql.YBClusterAwareDataSource");
    assertThat(hikariConfig.getDataSourceProperties().getProperty("serverName"))
        .isEqualTo("127.0.0.1");
    assertThat(hikariConfig.getDataSourceProperties().getProperty("portNumber")).isEqualTo("5433");
    assertThat(hikariConfig.getDataSourceProperties().getProperty("databaseName"))
        .isEqualTo("mydb");
    assertThat(hikariConfig.getDataSourceProperties().getProperty("user")).isEqualTo("my-user");
    assertThat(hikariConfig.getDataSourceProperties().getProperty("password"))
        .isEqualTo("my-password");
    assertThat(hikariConfig.getDataSourceProperties().getProperty("additionalEndpoints"))
        .isEqualTo("127.0.0.2:5433,127.0.0.3:5433");
    assertThat(hikariConfig.getMinimumIdle()).isEqualTo(1);
    assertThat(hikariConfig.getMaximumPoolSize()).isEqualTo(500);
    assertThat(hikariConfig.getTransactionIsolation())
        .isEqualTo(IsolationLevel.TRANSACTION_REPEATABLE_READ.name());
    assertThat(hikariConfig.isAutoCommit()).isFalse();
  }
}
