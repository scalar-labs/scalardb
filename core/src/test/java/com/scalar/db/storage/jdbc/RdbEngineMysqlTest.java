package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyBoolean;
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
import java.sql.SQLException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

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

  @Test
  void buildUpsertQuery_WithRowAliasSupported_ShouldReadRowAliasAndBindEachValueOnce()
      throws SQLException {
    // Arrange
    RdbEngineMysql engine = new RdbEngineMysql(mock(JdbcConfig.class), true);
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
    RdbEngineMysql engine = new RdbEngineMysql(mock(JdbcConfig.class), true);

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
    RdbEngineMysql engine = new RdbEngineMysql(mock(JdbcConfig.class), true);

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
}
