package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.config.DatabaseConfig;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class JdbcUtilsTest {

  @Mock private RdbEngineStrategy rdbEngine;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
  }

  @Test
  public void initDataSource_NonTransactional_ShouldConfigureHikariConfigProperly() {
    // Arrange
    Properties properties = new Properties();
    properties.setProperty(DatabaseConfig.CONTACT_POINTS, "jdbc:mysql://localhost:3306/");
    properties.setProperty(DatabaseConfig.USERNAME, "root");
    properties.setProperty(DatabaseConfig.PASSWORD, "mysql");
    properties.setProperty(DatabaseConfig.STORAGE, "jdbc");
    properties.setProperty(JdbcConfig.ISOLATION_LEVEL, "SERIALIZABLE");
    properties.setProperty(JdbcConfig.CONNECTION_POOL_MIN_IDLE, "10");
    properties.setProperty(JdbcConfig.CONNECTION_POOL_MAX_TOTAL, "30");

    JdbcConfig config = new JdbcConfig(new DatabaseConfig(properties));
    when(rdbEngine.getDriverClassName()).thenReturn("com.mysql.cj.jdbc.Driver");
    when(rdbEngine.getConnectionProperties(config)).thenReturn(Collections.emptyMap());

    AtomicReference<HikariConfig> capturedConfig = new AtomicReference<>();

    try (MockedStatic<JdbcUtils> jdbcUtils =
        Mockito.mockStatic(
            JdbcUtils.class, withSettings().defaultAnswer(Answers.CALLS_REAL_METHODS))) {
      jdbcUtils
          .when(() -> JdbcUtils.createDataSource(Mockito.any(HikariConfig.class)))
          .thenAnswer(
              invocation -> {
                capturedConfig.set(invocation.getArgument(0));
                return Mockito.mock(HikariDataSource.class);
              });

      // Act
      JdbcUtils.initDataSource(config, rdbEngine);
    }

    // Assert
    HikariConfig hikariConfig = capturedConfig.get();
    assertThat(hikariConfig).isNotNull();
    assertThat(hikariConfig.getDriverClassName()).isEqualTo("com.mysql.cj.jdbc.Driver");
    assertThat(hikariConfig.getJdbcUrl()).isEqualTo("jdbc:mysql://localhost:3306/");
    assertThat(hikariConfig.getUsername()).isEqualTo("root");
    assertThat(hikariConfig.getPassword()).isEqualTo("mysql");

    // Non-transactional mode does not set autoCommit to false, so it defaults to true
    assertThat(hikariConfig.isAutoCommit()).isTrue();
    assertThat(hikariConfig.getTransactionIsolation()).isEqualTo("TRANSACTION_SERIALIZABLE");
    assertThat(hikariConfig.isReadOnly()).isFalse();

    assertThat(hikariConfig.getMinimumIdle()).isEqualTo(10);
    assertThat(hikariConfig.getMaximumPoolSize()).isEqualTo(30);
  }

  @Test
  public void initDataSource_Transactional_ShouldConfigureHikariConfigProperly() {
    // Arrange
    Properties properties = new Properties();
    properties.setProperty(DatabaseConfig.CONTACT_POINTS, "jdbc:postgresql://localhost:5432/");
    properties.setProperty(DatabaseConfig.USERNAME, "user");
    properties.setProperty(DatabaseConfig.PASSWORD, "postgres");
    properties.setProperty(DatabaseConfig.STORAGE, "jdbc");
    properties.setProperty(JdbcConfig.ISOLATION_LEVEL, "READ_COMMITTED");
    properties.setProperty(JdbcConfig.CONNECTION_POOL_MIN_IDLE, "30");
    properties.setProperty(JdbcConfig.CONNECTION_POOL_MAX_TOTAL, "50");

    JdbcConfig config = new JdbcConfig(new DatabaseConfig(properties));
    when(rdbEngine.getDriverClassName()).thenReturn("org.postgresql.Driver");
    when(rdbEngine.getConnectionProperties(config)).thenReturn(Collections.emptyMap());

    AtomicReference<HikariConfig> capturedConfig = new AtomicReference<>();

    try (MockedStatic<JdbcUtils> jdbcUtils =
        Mockito.mockStatic(
            JdbcUtils.class, withSettings().defaultAnswer(Answers.CALLS_REAL_METHODS))) {
      jdbcUtils
          .when(() -> JdbcUtils.createDataSource(Mockito.any(HikariConfig.class)))
          .thenAnswer(
              invocation -> {
                capturedConfig.set(invocation.getArgument(0));
                return Mockito.mock(HikariDataSource.class);
              });

      // Act
      JdbcUtils.initDataSource(config, rdbEngine, true);
    }

    // Assert
    HikariConfig hikariConfig = capturedConfig.get();
    assertThat(hikariConfig).isNotNull();
    assertThat(hikariConfig.getDriverClassName()).isEqualTo("org.postgresql.Driver");
    assertThat(hikariConfig.getJdbcUrl()).isEqualTo("jdbc:postgresql://localhost:5432/");
    assertThat(hikariConfig.getUsername()).isEqualTo("user");
    assertThat(hikariConfig.getPassword()).isEqualTo("postgres");

    // Transactional mode sets autoCommit to false
    assertThat(hikariConfig.isAutoCommit()).isFalse();
    assertThat(hikariConfig.getTransactionIsolation()).isEqualTo("TRANSACTION_READ_COMMITTED");
    assertThat(hikariConfig.isReadOnly()).isFalse();

    assertThat(hikariConfig.getMinimumIdle()).isEqualTo(30);
    assertThat(hikariConfig.getMaximumPoolSize()).isEqualTo(50);
  }

  @Test
  public void initDataSource_WithRdbEngineConnectionProperties_ShouldAddDataSourceProperties() {
    // Arrange
    Properties properties = new Properties();
    properties.setProperty(
        DatabaseConfig.CONTACT_POINTS,
        "jdbc:sqlserver://localhost:5432;prop1=prop1Value;prop3=prop3Value");
    properties.setProperty(DatabaseConfig.USERNAME, "foo");
    properties.setProperty(DatabaseConfig.PASSWORD, "pass");
    properties.setProperty(DatabaseConfig.STORAGE, "jdbc");

    JdbcConfig config = new JdbcConfig(new DatabaseConfig(properties));
    when(rdbEngine.getDriverClassName()).thenReturn("com.microsoft.sqlserver.jdbc.SQLServerDriver");
    when(rdbEngine.getConnectionProperties(config))
        .thenReturn(ImmutableMap.of("prop1", "prop1Value", "prop2", "prop2Value"));

    AtomicReference<HikariConfig> capturedConfig = new AtomicReference<>();

    try (MockedStatic<JdbcUtils> jdbcUtils =
        Mockito.mockStatic(
            JdbcUtils.class, withSettings().defaultAnswer(Answers.CALLS_REAL_METHODS))) {
      jdbcUtils
          .when(() -> JdbcUtils.createDataSource(Mockito.any(HikariConfig.class)))
          .thenAnswer(
              invocation -> {
                capturedConfig.set(invocation.getArgument(0));
                return Mockito.mock(HikariDataSource.class);
              });

      // Act
      JdbcUtils.initDataSource(config, rdbEngine);
    }

    // Assert
    HikariConfig hikariConfig = capturedConfig.get();
    assertThat(hikariConfig).isNotNull();
    assertThat(hikariConfig.getDataSourceProperties().getProperty("prop1")).isEqualTo("prop1Value");
    assertThat(hikariConfig.getDataSourceProperties().getProperty("prop2")).isEqualTo("prop2Value");
  }

  @Test
  public void initDataSourceForTableMetadata_ShouldConfigureHikariConfigProperly() {
    // Arrange
    Properties properties = new Properties();
    properties.setProperty(
        DatabaseConfig.CONTACT_POINTS, "jdbc:oracle:thin:@localhost:1521/XEPDB1");
    properties.setProperty(DatabaseConfig.USERNAME, "user");
    properties.setProperty(DatabaseConfig.PASSWORD, "oracle");
    properties.setProperty(DatabaseConfig.STORAGE, "jdbc");
    properties.setProperty(JdbcConfig.ISOLATION_LEVEL, "REPEATABLE_READ");
    properties.setProperty(JdbcConfig.TABLE_METADATA_CONNECTION_POOL_MIN_IDLE, "100");
    properties.setProperty(JdbcConfig.TABLE_METADATA_CONNECTION_POOL_MAX_TOTAL, "300");

    JdbcConfig config = new JdbcConfig(new DatabaseConfig(properties));
    when(rdbEngine.getDriverClassName()).thenReturn("oracle.jdbc.driver.OracleDriver");
    when(rdbEngine.getConnectionProperties(config)).thenReturn(Collections.emptyMap());

    AtomicReference<HikariConfig> capturedConfig = new AtomicReference<>();

    try (MockedStatic<JdbcUtils> jdbcUtils =
        Mockito.mockStatic(
            JdbcUtils.class, withSettings().defaultAnswer(Answers.CALLS_REAL_METHODS))) {
      jdbcUtils
          .when(() -> JdbcUtils.createDataSource(Mockito.any(HikariConfig.class)))
          .thenAnswer(
              invocation -> {
                capturedConfig.set(invocation.getArgument(0));
                return Mockito.mock(HikariDataSource.class);
              });

      // Act
      JdbcUtils.initDataSourceForTableMetadata(config, rdbEngine);
    }

    // Assert
    HikariConfig hikariConfig = capturedConfig.get();
    assertThat(hikariConfig).isNotNull();
    assertThat(hikariConfig.getDriverClassName()).isEqualTo("oracle.jdbc.driver.OracleDriver");
    assertThat(hikariConfig.getJdbcUrl()).isEqualTo("jdbc:oracle:thin:@localhost:1521/XEPDB1");
    assertThat(hikariConfig.getUsername()).isEqualTo("user");
    assertThat(hikariConfig.getPassword()).isEqualTo("oracle");

    assertThat(hikariConfig.getTransactionIsolation()).isEqualTo("TRANSACTION_REPEATABLE_READ");
    assertThat(hikariConfig.isReadOnly()).isFalse();

    assertThat(hikariConfig.getMinimumIdle()).isEqualTo(100);
    assertThat(hikariConfig.getMaximumPoolSize()).isEqualTo(300);
  }

  @Test
  public void initDataSourceForAdmin_ShouldConfigureHikariConfigProperly() {
    // Arrange
    Properties properties = new Properties();
    properties.setProperty(DatabaseConfig.CONTACT_POINTS, "jdbc:sqlserver://localhost:1433");
    properties.setProperty(DatabaseConfig.USERNAME, "user");
    properties.setProperty(DatabaseConfig.PASSWORD, "sqlserver");
    properties.setProperty(DatabaseConfig.STORAGE, "jdbc");
    properties.setProperty(JdbcConfig.ISOLATION_LEVEL, "READ_UNCOMMITTED");
    properties.setProperty(JdbcConfig.ADMIN_CONNECTION_POOL_MIN_IDLE, "100");
    properties.setProperty(JdbcConfig.ADMIN_CONNECTION_POOL_MAX_TOTAL, "300");

    JdbcConfig config = new JdbcConfig(new DatabaseConfig(properties));
    when(rdbEngine.getDriverClassName()).thenReturn("com.microsoft.sqlserver.jdbc.SQLServerDriver");
    when(rdbEngine.getConnectionProperties(config)).thenReturn(Collections.emptyMap());

    AtomicReference<HikariConfig> capturedConfig = new AtomicReference<>();

    try (MockedStatic<JdbcUtils> jdbcUtils =
        Mockito.mockStatic(
            JdbcUtils.class, withSettings().defaultAnswer(Answers.CALLS_REAL_METHODS))) {
      jdbcUtils
          .when(() -> JdbcUtils.createDataSource(Mockito.any(HikariConfig.class)))
          .thenAnswer(
              invocation -> {
                capturedConfig.set(invocation.getArgument(0));
                return Mockito.mock(HikariDataSource.class);
              });

      // Act
      JdbcUtils.initDataSourceForAdmin(config, rdbEngine);
    }

    // Assert
    HikariConfig hikariConfig = capturedConfig.get();
    assertThat(hikariConfig).isNotNull();
    assertThat(hikariConfig.getDriverClassName())
        .isEqualTo("com.microsoft.sqlserver.jdbc.SQLServerDriver");
    assertThat(hikariConfig.getJdbcUrl()).isEqualTo("jdbc:sqlserver://localhost:1433");
    assertThat(hikariConfig.getUsername()).isEqualTo("user");
    assertThat(hikariConfig.getPassword()).isEqualTo("sqlserver");

    assertThat(hikariConfig.getTransactionIsolation()).isEqualTo("TRANSACTION_READ_UNCOMMITTED");
    assertThat(hikariConfig.isReadOnly()).isFalse();

    assertThat(hikariConfig.getMinimumIdle()).isEqualTo(100);
    assertThat(hikariConfig.getMaximumPoolSize()).isEqualTo(300);
  }
}
