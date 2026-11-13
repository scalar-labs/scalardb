package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.common.CoreError;
import com.scalar.db.config.DatabaseConfig;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class RdbEngineTidbTest {

  // TiDB is MySQL compatible and uses the same connection string, cf. RdbEngineFactory#create()
  private static final String ANY_JDBC_URL = "jdbc:mysql://localhost:4000/";

  private RdbEngineTidb rdbEngineTidb;

  @BeforeEach
  void setUp() {
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_JDBC_URL);
    props.setProperty(DatabaseConfig.STORAGE, "jdbc");
    rdbEngineTidb = new RdbEngineTidb(new JdbcConfig(new DatabaseConfig(props)));
  }

  @Test
  void setConnectionToReadOnly_ShouldDoNothing() throws SQLException {
    // TiDB rejects "SET SESSION TRANSACTION READ ONLY" with error 1235 unless
    // tidb_enable_noop_functions is enabled, and MariaDB Connector/J 3.5.10 and later issue that
    // statement from Connection#setReadOnly(). Inherited from RdbEngineMysql.
    Connection connection = mock(Connection.class);

    rdbEngineTidb.setConnectionToReadOnly(connection, true);

    verify(connection, never()).setReadOnly(anyBoolean());
  }

  @Test
  void importColumnAttributesSql_ShouldReturnSqlToGetExtraAndAutoRandomOfColumns() {
    // Arrange Act
    String sql = rdbEngineTidb.importColumnAttributesSql();

    // Assert
    assertThat(sql)
        .isEqualTo(
            "SELECT c.COLUMN_NAME, c.EXTRA,"
                + " CASE WHEN k.ORDINAL_POSITION = 1"
                + " AND t.TIDB_ROW_ID_SHARDING_INFO LIKE 'PK_AUTO_RANDOM_BITS%' THEN 1 ELSE 0 END"
                + " FROM INFORMATION_SCHEMA.COLUMNS c"
                + " JOIN INFORMATION_SCHEMA.TABLES t"
                + " ON t.TABLE_SCHEMA = c.TABLE_SCHEMA AND t.TABLE_NAME = c.TABLE_NAME"
                + " LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE k"
                + " ON k.TABLE_SCHEMA = c.TABLE_SCHEMA AND k.TABLE_NAME = c.TABLE_NAME"
                + " AND k.COLUMN_NAME = c.COLUMN_NAME AND k.CONSTRAINT_NAME = 'PRIMARY'"
                + " WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ?");
  }

  @Test
  void validateImportColumnAttributes_AutoRandomColumnGiven_ShouldThrowIllegalArgumentException()
      throws SQLException {
    // Arrange
    ResultSet resultSet = mockImportColumnAttributes("pk", "", 1);

    // Act Assert
    assertThatThrownBy(() -> rdbEngineTidb.validateImportColumnAttributes(resultSet, "ns", "tbl"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            CoreError.JDBC_IMPORT_AUTO_INCREMENT_COLUMN_NOT_SUPPORTED.buildMessage("ns.tbl", "pk"));
  }

  @Test
  void validateImportColumnAttributes_GeneratedColumnGiven_ShouldThrowIllegalArgumentException()
      throws SQLException {
    // Arrange
    ResultSet resultSet = mockImportColumnAttributes("col", "VIRTUAL GENERATED", 0);

    // Act Assert
    assertThatThrownBy(() -> rdbEngineTidb.validateImportColumnAttributes(resultSet, "ns", "tbl"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            CoreError.JDBC_IMPORT_GENERATED_COLUMN_NOT_SUPPORTED.buildMessage("ns.tbl", "col"));
  }

  @Test
  void validateImportColumnAttributes_AutoIncrementColumnGiven_ShouldThrowIllegalArgumentException()
      throws SQLException {
    // Arrange
    ResultSet resultSet = mockImportColumnAttributes("col", "auto_increment", 0);

    // Act Assert
    assertThatThrownBy(() -> rdbEngineTidb.validateImportColumnAttributes(resultSet, "ns", "tbl"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            CoreError.JDBC_IMPORT_AUTO_INCREMENT_COLUMN_NOT_SUPPORTED.buildMessage(
                "ns.tbl", "col"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "DEFAULT_GENERATED", "DEFAULT_GENERATED on update CURRENT_TIMESTAMP"})
  void validateImportColumnAttributes_ImportableColumnGiven_ShouldNotThrowAnyException(String extra)
      throws SQLException {
    // Arrange
    ResultSet resultSet = mockImportColumnAttributes("col", extra, 0);

    // Act Assert
    assertThatCode(() -> rdbEngineTidb.validateImportColumnAttributes(resultSet, "ns", "tbl"))
        .doesNotThrowAnyException();
  }

  private ResultSet mockImportColumnAttributes(String columnName, String extra, int isAutoRandom)
      throws SQLException {
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.getString(1)).thenReturn(columnName);
    when(resultSet.getString(2)).thenReturn(extra);
    when(resultSet.getInt(3)).thenReturn(isAutoRandom);
    return resultSet;
  }
}
