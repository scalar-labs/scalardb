package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import com.scalar.db.storage.jdbc.query.QueryBuilder;
import com.scalar.db.storage.jdbc.query.UpsertQuery;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class RdbEngineMysqlTest {

  private static final TableMetadata ROW_ALIAS_TABLE_METADATA =
      TableMetadata.newBuilder()
          .addColumn("p1", DataType.TEXT)
          .addColumn("c1", DataType.TEXT)
          .addColumn("v1", DataType.TEXT)
          .addColumn("v2", DataType.TEXT)
          .addPartitionKey("p1")
          .addClusteringKey("c1")
          .build();

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

  @Test
  void setConnectionToReadOnly_ShouldDoNothing() throws SQLException {
    // MariaDB Connector/J 3.5.10 and later issue SET SESSION TRANSACTION READ ONLY / READ WRITE
    // from Connection#setReadOnly(), which adds two round trips per read and which TiDB rejects.
    // This override was first added in #2801 and dropped in #3428; if this test starts failing,
    // check whether the driver still propagates the read-only state before relaxing it.
    Connection connection = mock(Connection.class);

    rdbEngineMysql.setConnectionToReadOnly(connection, true);

    verify(connection, never()).setReadOnly(anyBoolean());
  }

  /**
   * The AWS Advanced JDBC Wrapper reports failover with SQLState 08001, 08S02, and 08007. These
   * must never be treated as conflicts. {@link com.scalar.db.storage.jdbc.JdbcDatabase} converts a
   * conflict into a {@code RetriableExecutionException}, which tells the caller the operation
   * definitely did not apply and is safe to retry. That guarantee does not hold for a failover,
   * where the outcome may be unknown or the write may already have been applied. Adding a failover
   * error to this check would silently break the storage layer's safety.
   */
  @Test
  void isConflict_GivenFailoverSqlStates_ShouldReturnFalse() {
    // Act
    // Assert
    assertThat(rdbEngineMysql.isConflict(new SQLException("failover failed", "08001"))).isFalse();
    assertThat(rdbEngineMysql.isConflict(new SQLException("communication link changed", "08S02")))
        .isFalse();
    assertThat(
            rdbEngineMysql.isConflict(new SQLException("transaction resolution unknown", "08007")))
        .isFalse();
  }

  @Test
  void isConflict_GivenDeadlockOrLockWaitTimeout_ShouldReturnTrue() {
    // Act
    // Assert
    assertThat(rdbEngineMysql.isConflict(new SQLException("deadlock found", "40001", 1213)))
        .isTrue();
    assertThat(rdbEngineMysql.isConflict(new SQLException("lock wait timeout", "HY000", 1205)))
        .isTrue();
  }

  @Test
  void isConflict_GivenNullSqlState_ShouldReturnFalse() {
    // Act
    // Assert
    assertThat(rdbEngineMysql.isConflict(new SQLException("no sql state"))).isFalse();
  }

  /**
   * ScalarDB reaches MySQL through MariaDB Connector/J and does not bundle MySQL Connector/J, but
   * the AWS Advanced JDBC Wrapper defaults to the MySQL Connector/J dialect for a "jdbc:mysql://"
   * URL. Without the dialect parameter the wrapper asks for a driver that is not on the classpath.
   */
  @Test
  void adjustJdbcUrl_GivenAwsWrapperUrl_ShouldAppendSchemeAndDialectParameters() {
    String result = rdbEngineMysql.adjustJdbcUrl("jdbc:aws-wrapper:mysql://localhost:3306/");
    assertThat(result)
        .isEqualTo(
            "jdbc:aws-wrapper:mysql://localhost:3306/"
                + "?permitMysqlScheme=true&wrapperTargetDriverDialect=mariadb-connector-j-3");
  }

  @Test
  void adjustJdbcUrl_GivenAwsWrapperUrlWithExistingParams_ShouldAppendWithAmpersand() {
    String result =
        rdbEngineMysql.adjustJdbcUrl("jdbc:aws-wrapper:mysql://localhost:3306/?sslMode=REQUIRED");
    assertThat(result)
        .isEqualTo(
            "jdbc:aws-wrapper:mysql://localhost:3306/?sslMode=REQUIRED"
                + "&permitMysqlScheme=true&wrapperTargetDriverDialect=mariadb-connector-j-3");
  }

  @Test
  void adjustJdbcUrl_GivenAwsWrapperUrlWithDialectAlreadySet_ShouldNotOverrideIt() {
    String url =
        "jdbc:aws-wrapper:mysql://localhost:3306/?wrapperTargetDriverDialect=mysql-connector-j";
    String result = rdbEngineMysql.adjustJdbcUrl(url);
    // The user's explicit choice survives; only the scheme parameter is added.
    assertThat(result).isEqualTo(url + "&permitMysqlScheme=true");
  }

  @Test
  void adjustJdbcUrl_GivenAwsWrapperUrlWithSchemeParameterAlreadySet_ShouldAppendDialectOnly() {
    // What an existing MySQL user ends up with after prefixing their URL to adopt the wrapper.
    String url = "jdbc:aws-wrapper:mysql://localhost:3306/?permitMysqlScheme=true";
    String result = rdbEngineMysql.adjustJdbcUrl(url);
    assertThat(result).isEqualTo(url + "&wrapperTargetDriverDialect=mariadb-connector-j-3");
  }

  @Test
  void adjustJdbcUrl_GivenAwsWrapperUrlWithBothParametersSet_ShouldReturnAsIs() {
    String url =
        "jdbc:aws-wrapper:mysql://localhost:3306/"
            + "?permitMysqlScheme=true&wrapperTargetDriverDialect=mariadb-connector-j-3";
    String result = rdbEngineMysql.adjustJdbcUrl(url);
    assertThat(result).isEqualTo(url);
  }

  @Test
  void adjustJdbcUrl_GivenNonAwsWrapperUrl_ShouldNotAppendDialectParameter() {
    String result = rdbEngineMysql.adjustJdbcUrl("jdbc:mysql://localhost:3306/");
    assertThat(result).isEqualTo("jdbc:mysql://localhost:3306/?permitMysqlScheme=true");
    assertThat(result).doesNotContain("wrapperTargetDriverDialect");
  }

  @ParameterizedTest
  @CsvSource({
    "5.7.44, false",
    "8.0.18, false",
    "8.0.19, true",
    "8.0.45-0ubuntu0.22.04.1, true",
    "8.4.3, true",
    "9.1.0, true",
    "11.4.2-MariaDB, false",
    "'', false"
  })
  void supportsInsertRowAlias_ShouldBeTrueFromMysql8019AndFalseForMariaDb(
      String productVersion, boolean expected) throws SQLException {
    // Arrange
    DatabaseMetaData metaData = mock(DatabaseMetaData.class);
    when(metaData.getDatabaseProductVersion()).thenReturn(productVersion);

    // Act Assert
    assertThat(RdbEngineMysql.supportsInsertRowAlias(metaData)).isEqualTo(expected);
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
    RdbEngineMysql engine = new RdbEngineMysql(mock(JdbcConfig.class), false, true);

    // Act
    String sql = engine.renameColumnSql("ns", "tbl", "c1", "c2", "LONGTEXT");

    // Assert
    assertThat(sql).isEqualTo("ALTER TABLE `ns`.`tbl` RENAME COLUMN `c1` TO `c2`");
    assertThat(engine.renameColumnRestatesType()).isFalse();
  }

  @Test
  void renameColumnSql_WithRenameColumnNotSupported_ShouldRestateTheType() {
    // Arrange
    RdbEngineMysql engine = new RdbEngineMysql(mock(JdbcConfig.class), false, false);

    // Act
    String sql = engine.renameColumnSql("ns", "tbl", "c1", "c2", "LONGTEXT");

    // Assert
    assertThat(sql).isEqualTo("ALTER TABLE `ns`.`tbl` CHANGE COLUMN `c1` `c2` LONGTEXT");
    assertThat(engine.renameColumnRestatesType()).isTrue();
  }

  @Test
  void buildUpsertQuery_WithRowAliasSupported_ShouldReadRowAliasAndBindEachValueOnce()
      throws SQLException {
    // Arrange
    RdbEngineMysql engine = new RdbEngineMysql(mock(JdbcConfig.class), true, false);
    PreparedStatement preparedStatement = mock(PreparedStatement.class);
    Map<String, Column<?>> columns = new LinkedHashMap<>();
    columns.put("v1", TextColumn.of("v1", "v1Value"));
    columns.put("v2", TextColumn.of("v2", "v2Value"));

    // Act
    UpsertQuery query =
        new QueryBuilder(engine)
            .upsertInto("ns", "tbl", ROW_ALIAS_TABLE_METADATA)
            .values(Key.ofText("p1", "p1Value"), Optional.of(Key.ofText("c1", "c1Value")), columns)
            .build();
    query.bind(preparedStatement);

    // Assert
    assertThat(query.sql())
        .isEqualTo(
            "INSERT INTO `ns`.`tbl` (`p1`,`c1`,`v1`,`v2`) VALUES (?,?,?,?) AS `new`"
                + " ON DUPLICATE KEY UPDATE `v1`=`new`.`v1`,`v2`=`new`.`v2`");
    verify(preparedStatement).setString(1, "p1Value");
    verify(preparedStatement).setString(2, "c1Value");
    verify(preparedStatement).setString(3, "v1Value");
    verify(preparedStatement).setString(4, "v2Value");
    verifyNoMoreInteractions(preparedStatement);
  }

  @Test
  void buildUpsertQuery_WithRowAliasSupportedAndTableNamedNew_ShouldUseAnotherRowAlias() {
    // Arrange
    RdbEngineMysql engine = new RdbEngineMysql(mock(JdbcConfig.class), true, false);

    // Act
    String sql =
        new QueryBuilder(engine)
            .upsertInto("ns", "NEW", ROW_ALIAS_TABLE_METADATA)
            .values(
                Key.ofText("p1", "p1Value"),
                Optional.of(Key.ofText("c1", "c1Value")),
                Collections.singletonMap("v1", TextColumn.of("v1", "v1Value")))
            .build()
            .sql();

    // Assert
    assertThat(sql)
        .isEqualTo(
            "INSERT INTO `ns`.`NEW` (`p1`,`c1`,`v1`) VALUES (?,?,?) AS `new_row`"
                + " ON DUPLICATE KEY UPDATE `v1`=`new_row`.`v1`");
  }

  @Test
  void buildUpsertQuery_WithRowAliasSupportedAndNoValueColumns_ShouldInsertIgnore() {
    // Arrange
    RdbEngineMysql engine = new RdbEngineMysql(mock(JdbcConfig.class), true, false);

    // Act
    String sql =
        new QueryBuilder(engine)
            .upsertInto("ns", "tbl", ROW_ALIAS_TABLE_METADATA)
            .values(
                Key.ofText("p1", "p1Value"),
                Optional.of(Key.ofText("c1", "c1Value")),
                Collections.emptyMap())
            .build()
            .sql();

    // Assert
    assertThat(sql).isEqualTo("INSERT IGNORE INTO `ns`.`tbl` (`p1`,`c1`) VALUES (?,?)");
  }

  @ParameterizedTest
  @CsvSource({
    "8.0.17,utf8mb4_0900_bin",
    "8.0.16,utf8mb4_bin",
    "8.4.3,utf8mb4_0900_bin",
    "9.7.0,utf8mb4_0900_bin",
    "5.7.44-log,utf8mb4_bin",
    "5.5.5-10.11.6-MariaDB,utf8mb4_bin",
    "garbage,utf8mb4_bin"
  })
  void selectMysqlBinaryCollation_GivenVersion_ShouldReturnNoPadOnlyFrom8017(
      String version, String expected) {
    // Arrange Act
    String collation = RdbEngineMysql.selectMysqlBinaryCollation(version);

    // Assert
    assertThat(collation).isEqualTo(expected);
  }

  @Test
  void getBinaryTextCollationClause_ForMysql_ShouldReturnNoPadBinaryClause() {
    // Arrange
    RdbEngineStrategy rdbEngine = new RdbEngineMysql();

    // Act
    String clause = rdbEngine.getBinaryTextCollationClause();

    // Assert
    assertThat(clause).isEqualTo("CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin");
  }

  @Test
  void getBinaryTextCollationClause_ForMariaDb_ShouldReturnNopadBinClause() {
    // Arrange
    RdbEngineStrategy rdbEngine = new RdbEngineMariaDB();

    // Act
    String clause = rdbEngine.getBinaryTextCollationClause();

    // Assert
    assertThat(clause).isEqualTo("CHARACTER SET utf8mb4 COLLATE utf8mb4_nopad_bin");
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

  @Test
  void getTextColumnCollations_ShouldReturnCollationAndCharsetByColumnName() throws SQLException {
    // Arrange
    Connection connection = mock(Connection.class);
    PreparedStatement ps = mock(PreparedStatement.class);
    ResultSet rs = mock(ResultSet.class);
    when(connection.prepareStatement(anyString())).thenReturn(ps);
    when(ps.executeQuery()).thenReturn(rs);
    when(rs.next()).thenReturn(true, true, false);
    when(rs.getString("COLUMN_NAME")).thenReturn("c1", "c2");
    when(rs.getString("COLLATION_NAME")).thenReturn("utf8mb4_bin", "latin1_swedish_ci");
    when(rs.getString("CHARACTER_SET_NAME")).thenReturn("utf8mb4", "latin1");

    // Act
    Map<String, TextColumnCollation> collations =
        rdbEngineMysql.getTextColumnCollations(connection, "ns", "tbl");

    // Assert
    assertThat(collations)
        .containsOnly(
            entry("c1", new TextColumnCollation("utf8mb4_bin", "utf8mb4")),
            entry("c2", new TextColumnCollation("latin1_swedish_ci", "latin1")));
    verify(ps).setString(1, "ns");
    verify(ps).setString(2, "tbl");
  }

  @Test
  void keepsCollationOnTypeRedefinition_ShouldReturnFalse() {
    // Arrange Act
    boolean actual = rdbEngineMysql.keepsCollationOnTypeRedefinition();

    // Assert
    assertThat(actual).isFalse();
  }

  @Test
  void appendCollationClause_ShouldAppendNonEmptyClauseOnly() {
    // Arrange Act
    String withClause = rdbEngineMysql.appendCollationClause("VARCHAR(10)", "COLLATE c");
    String withoutClause = rdbEngineMysql.appendCollationClause("VARCHAR(10)", "");

    // Assert
    assertThat(withClause).isEqualTo("VARCHAR(10) COLLATE c");
    assertThat(withoutClause).isEqualTo("VARCHAR(10)");
  }

  @Test
  void isBinaryCollation_Utf8mb4BinOnNoPadServer_ShouldReturnFalse() {
    // Arrange
    TextColumnCollation collation = new TextColumnCollation("utf8mb4_bin", "utf8mb4");

    // Act
    boolean actual = rdbEngineMysql.isBinaryCollation(collation);

    // Assert
    assertThat(actual).isFalse();
  }

  @Test
  void isBinaryCollation_Utf8mb4BinOnPadOnlyServer_ShouldReturnTrue() {
    // Arrange
    RdbEngineMysql padOnlyEngine =
        new RdbEngineMysql(mock(JdbcConfig.class), false, false, "utf8mb4_bin");
    TextColumnCollation collation = new TextColumnCollation("utf8mb4_bin", "utf8mb4");

    // Act
    boolean actual = padOnlyEngine.isBinaryCollation(collation);

    // Assert
    assertThat(actual).isTrue();
  }

  @Test
  void isBinaryCollation_Utf8mb40900Bin_ShouldReturnTrue() {
    // Arrange
    TextColumnCollation collation = new TextColumnCollation("utf8mb4_0900_bin", "utf8mb4");

    // Act
    boolean actual = rdbEngineMysql.isBinaryCollation(collation);

    // Assert
    assertThat(actual).isTrue();
  }

  @Test
  void isBinaryCollation_Utf8mb4NopadBin_ShouldReturnTrue() {
    // Arrange
    TextColumnCollation collation = new TextColumnCollation("utf8mb4_nopad_bin", "utf8mb4");

    // Act
    boolean actual = rdbEngineMysql.isBinaryCollation(collation);

    // Assert
    assertThat(actual).isTrue();
  }

  @Test
  void isBinaryCollation_CaseInsensitiveCollation_ShouldReturnFalse() {
    // Arrange
    TextColumnCollation collation = new TextColumnCollation("utf8mb4_general_ci", "utf8mb4");

    // Act
    boolean actual = rdbEngineMysql.isBinaryCollation(collation);

    // Assert
    assertThat(actual).isFalse();
  }

  @Test
  void isFullUnicodeCharset_Utf8mb4_ShouldReturnTrue() {
    // Arrange
    TextColumnCollation collation = new TextColumnCollation("utf8mb4_0900_bin", "utf8mb4");

    // Act
    boolean actual = rdbEngineMysql.isFullUnicodeCharset(collation);

    // Assert
    assertThat(actual).isTrue();
  }

  @ParameterizedTest
  @ValueSource(strings = {"utf16", "utf16le", "utf32"})
  void isFullUnicodeCharset_OtherUnicodeEncoding_ShouldReturnTrue(String charset) {
    // Arrange
    TextColumnCollation collation = new TextColumnCollation(charset + "_bin", charset);

    // Act
    boolean actual = rdbEngineMysql.isFullUnicodeCharset(collation);

    // Assert
    assertThat(actual).isTrue();
  }

  @ParameterizedTest
  @ValueSource(strings = {"ucs2", "latin1"})
  void isFullUnicodeCharset_PartialCharset_ShouldReturnFalse(String charset) {
    // Arrange
    TextColumnCollation collation = new TextColumnCollation(charset + "_bin", charset);

    // Act
    boolean actual = rdbEngineMysql.isFullUnicodeCharset(collation);

    // Assert
    assertThat(actual).isFalse();
  }

  @Test
  void isFullUnicodeCharset_Utf8mb3_ShouldReturnFalse() {
    // Arrange
    TextColumnCollation collation = new TextColumnCollation("utf8mb3_bin", "utf8mb3");

    // Act
    boolean actual = rdbEngineMysql.isFullUnicodeCharset(collation);

    // Assert
    assertThat(actual).isFalse();
  }

  @Test
  void getDatabaseEncodingIssue_ShouldReturnNull() throws SQLException {
    // Arrange Act
    DatabaseEncodingIssue actual = rdbEngineMysql.getDatabaseEncodingIssue(mock(Connection.class));

    // Assert
    assertThat(actual).isNull();
  }
}
