package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.common.CoreError;
import com.scalar.db.config.DatabaseConfig;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
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
  void renameColumnSql_ShouldRenameWithoutRestatingTheType() {
    // Arrange Act
    String sql = rdbEngineTidb.renameColumnSql("ns", "tbl", "c1", "c2", "LONGTEXT");

    // Assert
    assertThat(sql).isEqualTo("ALTER TABLE `ns`.`tbl` RENAME COLUMN `c1` TO `c2`");
    assertThat(rdbEngineTidb.renameColumnRestatesType()).isFalse();
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

  @ParameterizedTest
  @CsvSource({
    "8.0.11-TiDB-v6.5.12,utf8mb4_bin",
    "8.0.11-TiDB-v7.4.0,utf8mb4_0900_bin",
    "8.0.11-TiDB-v8.5.1,utf8mb4_0900_bin",
    "8.0.11,utf8mb4_bin"
  })
  void selectTidbBinaryCollation_GivenVersion_ShouldReturnNoPadOnlyFrom74(
      String version, String expected) {
    // Arrange Act
    String collation = RdbEngineTidb.selectTidbBinaryCollation(version);

    // Assert
    assertThat(collation).isEqualTo(expected);
  }

  @Test
  void getBinaryTextCollationClause_ShouldReturnClauseWithSelectedCollation() {
    // Arrange Act
    String clause = rdbEngineTidb.getBinaryTextCollationClause();

    // Assert
    assertThat(clause).isEqualTo("CHARACTER SET utf8mb4 COLLATE utf8mb4_bin");
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

  @Test
  void getCollationClause_ShouldReturnCharacterSetAndCollate() {
    // Arrange
    TextColumnCollation collation = new TextColumnCollation("utf8mb4_bin", "utf8mb4");

    // Act
    String clause = rdbEngineTidb.getCollationClause(collation);

    // Assert
    assertThat(clause).isEqualTo("CHARACTER SET utf8mb4 COLLATE utf8mb4_bin");
  }

  @Test
  void getTextColumnCollations_ShouldReadTheInformationSchema() throws SQLException {
    // Arrange
    Connection connection = mock(Connection.class);
    PreparedStatement ps = mock(PreparedStatement.class);
    ResultSet rs = mock(ResultSet.class);
    when(connection.prepareStatement(anyString())).thenReturn(ps);
    when(ps.executeQuery()).thenReturn(rs);
    when(rs.next()).thenReturn(true, false);
    when(rs.getString("COLUMN_NAME")).thenReturn("c1");
    when(rs.getString("COLLATION_NAME")).thenReturn("utf8mb4_bin");
    when(rs.getString("CHARACTER_SET_NAME")).thenReturn("utf8mb4");

    // Act
    Map<String, TextColumnCollation> collations =
        rdbEngineTidb.getTextColumnCollations(connection, "ns", "tbl");

    // Assert
    assertThat(collations)
        .containsOnly(entry("c1", new TextColumnCollation("utf8mb4_bin", "utf8mb4")));
  }

  @Test
  void classify_Utf8mb4BinOnTidbWithoutNoPad_ShouldReturnCodePointOrder() {
    // Arrange
    RdbEngineTidb padEngine = new RdbEngineTidb(new JdbcConfig(new DatabaseConfig(props())));
    TextColumnCollation collation = new TextColumnCollation("utf8mb4_bin", "utf8mb4");

    // Act
    TextColumnCollationKind actual = padEngine.classify(collation);

    // Assert
    assertThat(actual).isEqualTo(TextColumnCollationKind.CODE_POINT_ORDER);
  }

  @Test
  void classify_Utf8mb4BinOnTidbWithNoPad_ShouldReturnOtherOrder() {
    // Arrange
    RdbEngineTidb noPadEngine =
        new RdbEngineTidb(new JdbcConfig(new DatabaseConfig(props())), "utf8mb4_0900_bin");
    TextColumnCollation collation = new TextColumnCollation("utf8mb4_bin", "utf8mb4");

    // Act
    TextColumnCollationKind actual = noPadEngine.classify(collation);

    // Assert
    assertThat(actual).isEqualTo(TextColumnCollationKind.OTHER_ORDER);
  }

  private static Properties props() {
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, ANY_JDBC_URL);
    props.setProperty(DatabaseConfig.STORAGE, "jdbc");
    return props;
  }

  @Test
  void isFullUnicodeCharset_Latin1_ShouldReturnTrue() {
    // Arrange
    RdbEngineTidb engine = new RdbEngineTidb(new JdbcConfig(new DatabaseConfig(props())));
    TextColumnCollation collation = new TextColumnCollation("latin1_bin", "latin1");

    // Act
    boolean actual = engine.isFullUnicodeCharset(collation);

    // Assert
    assertThat(actual).isTrue();
  }
}
