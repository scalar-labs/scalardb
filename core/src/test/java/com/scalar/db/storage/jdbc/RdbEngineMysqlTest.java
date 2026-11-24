package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.scalar.db.common.CoreError;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class RdbEngineMysqlTest {

  private RdbEngineMysql rdbEngineMysql;

  @BeforeEach
  void setUp() {
    rdbEngineMysql = new RdbEngineMysql();
  }

  @Test
  void adjustJdbcUrl_WithNoParams_ShouldAppendPermitMysqlScheme() {
    String result = rdbEngineMysql.adjustJdbcUrl("jdbc:mysql://localhost:3306/");
    assertThat(result).isEqualTo("jdbc:mysql://localhost:3306/?permitMysqlScheme=true");
  }

  @Test
  void adjustJdbcUrl_WithExistingParams_ShouldAppendPermitMysqlScheme() {
    String result = rdbEngineMysql.adjustJdbcUrl("jdbc:mysql://localhost:3306/?sslMode=REQUIRED");
    assertThat(result)
        .isEqualTo("jdbc:mysql://localhost:3306/?sslMode=REQUIRED&permitMysqlScheme=true");
  }

  @Test
  void adjustJdbcUrl_WithPermitMysqlSchemeAlreadyPresent_ShouldReturnAsIs() {
    String url = "jdbc:mysql://localhost:3306/?permitMysqlScheme=true";
    String result = rdbEngineMysql.adjustJdbcUrl(url);
    assertThat(result).isEqualTo(url);
  }

  @ParameterizedTest
  @CsvSource({
    "5.7.44, false",
    "8.0.2, false",
    "8.0.3, true",
    "8.0.45-0ubuntu0.22.04.1, true",
    "8.4.3, true",
    "9.1.0, true",
    "8.0.custom_build, true",
    "5.7.custom_build, false",
    "11.4.2-MariaDB, false",
    "8.0.11-TiDB-v8.5.0, false",
    "'', false"
  })
  void supportsRenameColumn_ShouldBeTrueFromMysql803AndFalseForMariaDbAndTidb(
      String productVersion, boolean expected) throws SQLException {
    // Arrange
    DatabaseMetaData metaData = mock(DatabaseMetaData.class);
    when(metaData.getDatabaseProductVersion()).thenReturn(productVersion);

    // Act Assert
    assertThat(RdbEngineMysql.supportsRenameColumn(metaData)).isEqualTo(expected);
  }

  @Test
  void renameColumnSql_WithRenameColumnSupported_ShouldNotRestateTheType() {
    // Arrange
    RdbEngineMysql engine = new RdbEngineMysql(mock(JdbcConfig.class), true);

    // Act
    String sql = engine.renameColumnSql("ns", "tbl", "c1", "c2", "LONGTEXT");

    // Assert
    assertThat(sql).isEqualTo("ALTER TABLE `ns`.`tbl` RENAME COLUMN `c1` TO `c2`");
    assertThat(engine.renameColumnRestatesType()).isFalse();
  }

  @Test
  void renameColumnSql_WithRenameColumnNotSupported_ShouldRestateTheType() {
    // Arrange
    RdbEngineMysql engine = new RdbEngineMysql(mock(JdbcConfig.class), false);

    // Act
    String sql = engine.renameColumnSql("ns", "tbl", "c1", "c2", "LONGTEXT");

    // Assert
    assertThat(sql).isEqualTo("ALTER TABLE `ns`.`tbl` CHANGE COLUMN `c1` `c2` LONGTEXT");
    assertThat(engine.renameColumnRestatesType()).isTrue();
  }

  @Test
  void importColumnAttributesSql_ShouldReturnSqlToGetExtraOfColumns() {
    // Arrange Act
    String sql = rdbEngineMysql.importColumnAttributesSql();

    // Assert
    assertThat(sql)
        .isEqualTo(
            "SELECT COLUMN_NAME, EXTRA FROM INFORMATION_SCHEMA.COLUMNS"
                + " WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "VIRTUAL GENERATED",
        "STORED GENERATED",
        "VIRTUAL GENERATED INVISIBLE",
        "STORED GENERATED INVISIBLE",
        // For MariaDB
        "VIRTUAL GENERATED, INVISIBLE",
        "STORED GENERATED, INVISIBLE",
        // For MariaDB 10.1 or earlier
        "VIRTUAL",
        "PERSISTENT"
      })
  void validateImportColumnAttributes_GeneratedColumnGiven_ShouldThrowIllegalArgumentException(
      String extra) throws SQLException {
    // Arrange
    ResultSet resultSet = mockImportColumnAttributes("col", extra);

    // Act Assert
    assertThatThrownBy(() -> rdbEngineMysql.validateImportColumnAttributes(resultSet, "ns", "tbl"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            CoreError.JDBC_IMPORT_GENERATED_COLUMN_NOT_SUPPORTED.buildMessage("ns.tbl", "col"));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "auto_increment",
        "auto_increment INVISIBLE",
        // For MariaDB
        "auto_increment, INVISIBLE"
      })
  void validateImportColumnAttributes_AutoIncrementColumnGiven_ShouldThrowIllegalArgumentException(
      String extra) throws SQLException {
    // Arrange
    ResultSet resultSet = mockImportColumnAttributes("col", extra);

    // Act Assert
    assertThatThrownBy(() -> rdbEngineMysql.validateImportColumnAttributes(resultSet, "ns", "tbl"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            CoreError.JDBC_IMPORT_AUTO_INCREMENT_COLUMN_NOT_SUPPORTED.buildMessage(
                "ns.tbl", "col"));
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(
      strings = {
        // A column with an expression default, which is not a generated column
        "DEFAULT_GENERATED",
        "DEFAULT_GENERATED on update CURRENT_TIMESTAMP",
        "on update CURRENT_TIMESTAMP",
        // For MariaDB
        "on update current_timestamp()",
        "INVISIBLE"
      })
  void validateImportColumnAttributes_ImportableColumnGiven_ShouldNotThrowAnyException(String extra)
      throws SQLException {
    // Arrange
    ResultSet resultSet = mockImportColumnAttributes("col", extra);

    // Act Assert
    assertThatCode(() -> rdbEngineMysql.validateImportColumnAttributes(resultSet, "ns", "tbl"))
        .doesNotThrowAnyException();
  }

  private ResultSet mockImportColumnAttributes(String columnName, String extra)
      throws SQLException {
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.getString(1)).thenReturn(columnName);
    when(resultSet.getString(2)).thenReturn(extra);
    return resultSet;
  }

  @Test
  void getCollationClause_WithCharset_ShouldReturnCharacterSetAndCollate() {
    // Arrange
    TextColumnCollation collation = new TextColumnCollation("latin1_swedish_ci", "latin1");

    // Act
    String clause = rdbEngineMysql.getCollationClause(collation);

    // Assert
    assertThat(clause).isEqualTo("CHARACTER SET latin1 COLLATE latin1_swedish_ci");
  }

  @Test
  void getCollationClause_WithoutCharset_ShouldReturnCollateOnly() {
    // Arrange
    TextColumnCollation collation = new TextColumnCollation("utf8mb4_bin", null);

    // Act
    String clause = rdbEngineMysql.getCollationClause(collation);

    // Assert
    assertThat(clause).isEqualTo("COLLATE utf8mb4_bin");
  }
}
