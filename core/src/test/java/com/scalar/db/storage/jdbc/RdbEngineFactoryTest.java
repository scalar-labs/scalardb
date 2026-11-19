package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.MockedStatic;

class RdbEngineFactoryTest {

  private static JdbcConfig configWithUrl(String jdbcUrl) {
    JdbcConfig config = mock(JdbcConfig.class);
    when(config.getJdbcUrl()).thenReturn(jdbcUrl);
    return config;
  }

  @ParameterizedTest
  @CsvSource({"5.7.44, true", "8.0.2, true", "8.0.3, false", "8.4.3, false"})
  void create_GivenMysqlUrlAndMysqlServer_ShouldRestateTheTypeOnRenameColumnBefore803(
      String productVersion, boolean expected) throws SQLException {
    // Arrange Act
    RdbEngineStrategy engine =
        createWithServerVersion("jdbc:mysql://localhost:3306/", productVersion);

    // Assert
    assertThat(engine.renameColumnRestatesType()).isEqualTo(expected);
  }

  @Test
  void create_GivenMysqlUrlAndTidbServerReportingMysql8_ShouldRenameColumnWithoutRestatingTheType()
      throws SQLException {
    // Arrange Act
    RdbEngineStrategy engine =
        createWithServerVersion("jdbc:mysql://localhost:4000/", "8.0.11-TiDB-v8.5.0");

    // Assert
    assertThat(engine.renameColumnRestatesType()).isFalse();
  }

  private static RdbEngineStrategy createWithServerVersion(String jdbcUrl, String productVersion)
      throws SQLException {
    JdbcConfig config = configWithUrl(jdbcUrl);
    BasicDataSource dataSource = mock(BasicDataSource.class);
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
}
