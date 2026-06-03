package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import com.scalar.db.common.CoreError;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class RdbEngineFactoryTest {

  private static JdbcConfig configWithUrl(String jdbcUrl) {
    JdbcConfig config = mock(JdbcConfig.class);
    when(config.getJdbcUrl()).thenReturn(jdbcUrl);
    return config;
  }

  @Test
  void create_GivenPostgresqlUrl_ShouldReturnRdbEnginePostgresql() {
    // Arrange
    JdbcConfig config = configWithUrl("jdbc:postgresql://localhost:5432/test");

    // Act
    RdbEngineStrategy engine = RdbEngineFactory.create(config);

    // Assert
    assertThat(engine).isInstanceOf(RdbEnginePostgresql.class);
  }

  @Test
  void create_GivenOracleUrl_ShouldReturnRdbEngineOracle() {
    // Arrange
    JdbcConfig config = configWithUrl("jdbc:oracle:thin:@//localhost:1521/FREEPDB1");

    // Act
    RdbEngineStrategy engine = RdbEngineFactory.create(config);

    // Assert
    assertThat(engine).isInstanceOf(RdbEngineOracle.class);
  }

  @Test
  void create_GivenSqlServerUrl_ShouldReturnRdbEngineSqlServer() {
    // Arrange
    JdbcConfig config = configWithUrl("jdbc:sqlserver://localhost:1433;databaseName=test");

    // Act
    RdbEngineStrategy engine = RdbEngineFactory.create(config);

    // Assert
    assertThat(engine).isInstanceOf(RdbEngineSqlServer.class);
  }

  @Test
  void create_GivenSqliteUrl_ShouldReturnRdbEngineSqlite() {
    // Arrange
    JdbcConfig config = configWithUrl("jdbc:sqlite:test.db");

    // Act
    RdbEngineStrategy engine = RdbEngineFactory.create(config);

    // Assert
    assertThat(engine).isInstanceOf(RdbEngineSqlite.class);
  }

  @Test
  void create_GivenYugabyteUrl_ShouldReturnRdbEngineYugabyte() {
    // Arrange
    JdbcConfig config = configWithUrl("jdbc:yugabytedb://localhost:5433/test");

    // Act
    RdbEngineStrategy engine = RdbEngineFactory.create(config);

    // Assert
    assertThat(engine).isInstanceOf(RdbEngineYugabyte.class);
  }

  @Test
  void create_GivenMariaDbUrl_ShouldReturnRdbEngineMariaDB() {
    // Arrange
    JdbcConfig config = configWithUrl("jdbc:mariadb://localhost:3306/test");

    // Act
    RdbEngineStrategy engine = RdbEngineFactory.create(config);

    // Assert
    assertThat(engine).isInstanceOf(RdbEngineMariaDB.class);
  }

  @Test
  void create_GivenDb2Url_ShouldReturnRdbEngineDb2() {
    // Arrange
    JdbcConfig config = configWithUrl("jdbc:db2://localhost:50000/test");

    // Act
    RdbEngineStrategy engine = RdbEngineFactory.create(config);

    // Assert
    assertThat(engine).isInstanceOf(RdbEngineDb2.class);
  }

  @Test
  void create_GivenCloudSpannerUrl_ShouldReturnRdbEngineSpanner() {
    // Arrange
    JdbcConfig config = configWithUrl("jdbc:cloudspanner:/projects/p/instances/i/databases/d");

    // Act
    RdbEngineStrategy engine = RdbEngineFactory.create(config);

    // Assert
    assertThat(engine).isInstanceOf(RdbEngineSpanner.class);
  }

  @Test
  void create_GivenSpannerUrl_ShouldReturnRdbEngineSpanner() {
    // Arrange
    JdbcConfig config = configWithUrl("jdbc:spanner:/projects/p/instances/i/databases/d");

    // Act
    RdbEngineStrategy engine = RdbEngineFactory.create(config);

    // Assert
    assertThat(engine).isInstanceOf(RdbEngineSpanner.class);
  }

  @Test
  void create_GivenMysqlUrlAndMysqlServer_ShouldReturnRdbEngineMysql() throws SQLException {
    // Arrange
    JdbcConfig config = configWithUrl("jdbc:mysql://localhost:3306/test");
    HikariDataSource dataSource = mock(HikariDataSource.class);
    Connection connection = mock(Connection.class);
    DatabaseMetaData metaData = mock(DatabaseMetaData.class);
    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.getMetaData()).thenReturn(metaData);
    when(metaData.getDatabaseProductVersion()).thenReturn("8.0.36");

    try (MockedStatic<JdbcUtils> mocked = mockStatic(JdbcUtils.class)) {
      mocked
          .when(() -> JdbcUtils.initDataSourceForAdmin(any(JdbcConfig.class), any()))
          .thenReturn(dataSource);

      // Act
      RdbEngineStrategy engine = RdbEngineFactory.create(config);

      // Assert
      assertThat(engine).isInstanceOf(RdbEngineMysql.class);
    }
  }

  @Test
  void create_GivenMysqlUrlAndTidbServer_ShouldReturnRdbEngineTidb() throws SQLException {
    // Arrange
    JdbcConfig config = configWithUrl("jdbc:mysql://localhost:4000/test");
    HikariDataSource dataSource = mock(HikariDataSource.class);
    Connection connection = mock(Connection.class);
    DatabaseMetaData metaData = mock(DatabaseMetaData.class);
    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.getMetaData()).thenReturn(metaData);
    when(metaData.getDatabaseProductVersion()).thenReturn("5.7.25-TiDB-v6.1.0");

    try (MockedStatic<JdbcUtils> mocked = mockStatic(JdbcUtils.class)) {
      mocked
          .when(() -> JdbcUtils.initDataSourceForAdmin(any(JdbcConfig.class), any()))
          .thenReturn(dataSource);

      // Act
      RdbEngineStrategy engine = RdbEngineFactory.create(config);

      // Assert
      assertThat(engine).isInstanceOf(RdbEngineTidb.class);
    }
  }

  @Test
  void create_GivenMysqlUrlAndConnectionFailure_ShouldThrowRuntimeException() throws SQLException {
    // Arrange
    JdbcConfig config = configWithUrl("jdbc:mysql://localhost:3306/test");
    HikariDataSource dataSource = mock(HikariDataSource.class);
    when(dataSource.getConnection()).thenThrow(new SQLException("connection refused"));

    try (MockedStatic<JdbcUtils> mocked = mockStatic(JdbcUtils.class)) {
      mocked
          .when(() -> JdbcUtils.initDataSourceForAdmin(any(JdbcConfig.class), any()))
          .thenReturn(dataSource);

      // Act & Assert
      assertThatThrownBy(() -> RdbEngineFactory.create(config))
          .isInstanceOf(RuntimeException.class)
          .hasMessageContaining(
              CoreError.JDBC_MYSQL_GETTING_CONNECTION_METADATA_FAILED.buildMessage(
                  "connection refused"));
    }
  }

  @Test
  void create_GivenUnsupportedUrl_ShouldThrowIllegalArgumentException() {
    // Arrange
    String unsupportedUrl = "jdbc:unknown://localhost/test";
    JdbcConfig config = configWithUrl(unsupportedUrl);

    // Act & Assert
    assertThatThrownBy(() -> RdbEngineFactory.create(config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(CoreError.JDBC_RDB_ENGINE_NOT_SUPPORTED.buildMessage(unsupportedUrl));
  }
}
