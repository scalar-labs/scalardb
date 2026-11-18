package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.CoreError;
import com.scalar.db.io.DataType;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.Key;
import com.scalar.db.storage.jdbc.query.QueryBuilder;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Optional;
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
  void create_GivenMysqlUrlAndMysql8019OrLaterServer_ShouldBuildUpsertsWithRowAlias()
      throws SQLException {
    // Arrange Act
    RdbEngineStrategy engine = createWithServerVersion("jdbc:mysql://localhost:3306/", "8.0.19");

    // Assert
    assertThat(upsertSql(engine))
        .isEqualTo(
            "INSERT INTO `ns`.`tbl` (`p1`,`v1`) VALUES (?,?) AS `new`"
                + " ON DUPLICATE KEY UPDATE `v1`=`new`.`v1`");
  }

  @Test
  void create_GivenMysqlUrlAndMysqlServerBefore8019_ShouldBuildUpsertsWithValuesFunction()
      throws SQLException {
    // Arrange Act
    RdbEngineStrategy engine = createWithServerVersion("jdbc:mysql://localhost:3306/", "8.0.18");

    // Assert
    assertThat(upsertSql(engine))
        .isEqualTo(
            "INSERT INTO `ns`.`tbl` (`p1`,`v1`) VALUES (?,?)"
                + " ON DUPLICATE KEY UPDATE `v1`=VALUES(`v1`)");
  }

  @Test
  void create_GivenMysqlUrlAndTidbServerReportingMysql8_ShouldBuildUpsertsWithValuesFunction()
      throws SQLException {
    // Arrange Act
    RdbEngineStrategy engine =
        createWithServerVersion("jdbc:mysql://localhost:4000/", "8.0.11-TiDB-v8.5.0");

    // Assert
    assertThat(engine).isInstanceOf(RdbEngineTidb.class);
    assertThat(upsertSql(engine)).endsWith("ON DUPLICATE KEY UPDATE `v1`=VALUES(`v1`)");
  }

  private static RdbEngineStrategy createWithServerVersion(String jdbcUrl, String productVersion)
      throws SQLException {
    JdbcConfig config = configWithUrl(jdbcUrl);
    HikariDataSource dataSource = mock(HikariDataSource.class);
    Connection connection = mock(Connection.class);
    DatabaseMetaData metaData = mock(DatabaseMetaData.class);
    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.getMetaData()).thenReturn(metaData);
    when(metaData.getDatabaseProductVersion()).thenReturn(productVersion);
    try (MockedStatic<JdbcUtils> mocked = mockStatic(JdbcUtils.class)) {
      mocked
          .when(() -> JdbcUtils.initDataSourceForAdmin(any(JdbcConfig.class), any()))
          .thenReturn(dataSource);
      return RdbEngineFactory.create(config);
    }
  }

  private static String upsertSql(RdbEngineStrategy engine) {
    TableMetadata metadata =
        TableMetadata.newBuilder()
            .addColumn("p1", DataType.INT)
            .addColumn("v1", DataType.INT)
            .addPartitionKey("p1")
            .build();
    return new QueryBuilder(engine)
        .upsertInto("ns", "tbl", metadata)
        .values(
            Key.ofInt("p1", 1),
            Optional.empty(),
            Collections.singletonMap("v1", IntColumn.of("v1", 1)))
        .build()
        .sql();
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

  @Test
  void create_GivenAwsWrapperPostgresqlUrl_ShouldReturnRdbEnginePostgresql() {
    // Arrange
    JdbcConfig config = configWithUrl("jdbc:aws-wrapper:postgresql://localhost:5432/test");

    // Act
    RdbEngineStrategy engine = RdbEngineFactory.create(config);

    // Assert
    assertThat(engine).isInstanceOf(RdbEnginePostgresql.class);
  }

  @Test
  void create_GivenAwsWrapperMysqlUrlAndMysqlServer_ShouldReturnRdbEngineMysql()
      throws SQLException {
    // Arrange
    JdbcConfig config = configWithUrl("jdbc:aws-wrapper:mysql://localhost:3306/test");
    HikariDataSource dataSource = mock(HikariDataSource.class);
    Connection connection = mock(Connection.class);
    DatabaseMetaData metaData = mock(DatabaseMetaData.class);
    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.getMetaData()).thenReturn(metaData);
    when(metaData.getDatabaseProductVersion()).thenReturn("8.0.44");

    // The wrapper-URL helpers must keep their real behavior, so only the data source is stubbed.
    try (MockedStatic<JdbcUtils> mocked = mockStatic(JdbcUtils.class, CALLS_REAL_METHODS)) {
      mocked
          .when(() -> JdbcUtils.initDataSourceForAdmin(any(JdbcConfig.class), any()))
          .thenReturn(dataSource);

      // Act
      RdbEngineStrategy engine = RdbEngineFactory.create(config);

      // Assert
      assertThat(engine).isInstanceOf(RdbEngineMysql.class);
      assertThat(engine).isNotInstanceOf(RdbEngineTidb.class);
    }
  }

  @Test
  void create_GivenAwsWrapperMysqlUrlAndTidbServer_ShouldThrowIllegalArgumentException()
      throws SQLException {
    // Arrange
    String jdbcUrl = "jdbc:aws-wrapper:mysql://localhost:4000/test";
    JdbcConfig config = configWithUrl(jdbcUrl);
    HikariDataSource dataSource = mock(HikariDataSource.class);
    Connection connection = mock(Connection.class);
    DatabaseMetaData metaData = mock(DatabaseMetaData.class);
    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.getMetaData()).thenReturn(metaData);
    when(metaData.getDatabaseProductVersion()).thenReturn("5.7.25-TiDB-v6.1.0");

    try (MockedStatic<JdbcUtils> mocked = mockStatic(JdbcUtils.class, CALLS_REAL_METHODS)) {
      mocked
          .when(() -> JdbcUtils.initDataSourceForAdmin(any(JdbcConfig.class), any()))
          .thenReturn(dataSource);

      // Act Assert
      // TiDB is untested through the wrapper, so it is rejected like MariaDB.
      assertThatThrownBy(() -> RdbEngineFactory.create(config))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage(
              CoreError.JDBC_RDB_ENGINE_NOT_SUPPORTED_WITH_AWS_ADVANCED_JDBC_WRAPPER.buildMessage(
                  jdbcUrl));
    }
  }

  @Test
  void create_GivenAwsWrapperMariadbUrl_ShouldThrowIllegalArgumentException() {
    // Arrange
    String jdbcUrl = "jdbc:aws-wrapper:mariadb://localhost:3306/test";
    JdbcConfig config = configWithUrl(jdbcUrl);

    // Act Assert
    // The wrapper supports MariaDB, but ScalarDB does not test that combination for Aurora.
    assertThatThrownBy(() -> RdbEngineFactory.create(config))
        .isInstanceOf(IllegalArgumentException.class)
        // The message must show the URL the user configured, not the prefix-stripped one.
        .hasMessage(
            CoreError.JDBC_RDB_ENGINE_NOT_SUPPORTED_WITH_AWS_ADVANCED_JDBC_WRAPPER.buildMessage(
                jdbcUrl));
  }

  @Test
  void create_GivenAwsWrapperUnsupportedUrl_ShouldThrowIllegalArgumentException() {
    // Arrange
    String jdbcUrl = "jdbc:aws-wrapper:oracle:thin:@//localhost:1521/FREEPDB1";
    JdbcConfig config = configWithUrl(jdbcUrl);

    // Act Assert
    assertThatThrownBy(() -> RdbEngineFactory.create(config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            CoreError.JDBC_RDB_ENGINE_NOT_SUPPORTED_WITH_AWS_ADVANCED_JDBC_WRAPPER.buildMessage(
                jdbcUrl));
  }

  @Test
  void create_GivenAwsWrapperUrlWithoutUnderlyingScheme_ShouldThrowIllegalArgumentException() {
    // Arrange
    String jdbcUrl = "jdbc:aws-wrapper:";
    JdbcConfig config = configWithUrl(jdbcUrl);

    // Act Assert
    assertThatThrownBy(() -> RdbEngineFactory.create(config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            CoreError.JDBC_RDB_ENGINE_NOT_SUPPORTED_WITH_AWS_ADVANCED_JDBC_WRAPPER.buildMessage(
                jdbcUrl));
  }

  @Test
  void create_GivenUrlOnlyResemblingAwsWrapperPrefix_ShouldNotBeTreatedAsWrapper() {
    // Arrange
    // "jdbc:aws-wrapperfoo:" must not be mistaken for the wrapper prefix.
    String jdbcUrl = "jdbc:aws-wrapperfoo:postgresql://localhost:5432/test";
    JdbcConfig config = configWithUrl(jdbcUrl);

    // Act Assert
    assertThatThrownBy(() -> RdbEngineFactory.create(config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(CoreError.JDBC_RDB_ENGINE_NOT_SUPPORTED.buildMessage(jdbcUrl));
  }
}
