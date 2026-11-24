package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.io.TimestampTZColumn;
import com.scalar.db.storage.jdbc.query.QueryBuilder;
import com.scalar.db.storage.jdbc.query.SelectQuery;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class RdbEngineSqlServerTest {
  private static final TableMetadata METADATA_WITH_TIMESTAMPTZ =
      TableMetadata.newBuilder()
          .addColumn("pk", DataType.INT)
          .addColumn("ck", DataType.TIMESTAMPTZ)
          .addColumn("v", DataType.TEXT)
          .addPartitionKey("pk")
          .addClusteringKey("ck", Order.ASC)
          .build();

  private final RdbEngineSqlServer rdbEngine = new RdbEngineSqlServer();

  @Test
  void getProjectionsSqlForSelectQuery_GivenNoProjectionAndNoTimestampTZColumn_ShouldReturnStar() {
    TableMetadata metadata =
        TableMetadata.newBuilder()
            .addColumn("pk", DataType.INT)
            .addColumn("v", DataType.TIMESTAMP)
            .addPartitionKey("pk")
            .build();

    assertThat(rdbEngine.getProjectionsSqlForSelectQuery(metadata, Collections.emptyList()))
        .isEqualTo("*");
  }

  @Test
  void
      getProjectionsSqlForSelectQuery_GivenNoProjectionAndTimestampTZColumn_ShouldListAllColumnsWithTimestampTZCast() {
    assertThat(
            rdbEngine.getProjectionsSqlForSelectQuery(
                METADATA_WITH_TIMESTAMPTZ, Collections.emptyList()))
        .isEqualTo("[pk],CAST(SWITCHOFFSET([ck], '+00:00') AS DATETIME2(7)) AS [ck],[v]");
  }

  @Test
  void getProjectionsSqlForSelectQuery_GivenProjections_ShouldCastOnlyTimestampTZColumns() {
    assertThat(
            rdbEngine.getProjectionsSqlForSelectQuery(
                METADATA_WITH_TIMESTAMPTZ, Arrays.asList("v", "ck")))
        .isEqualTo("[v],CAST(SWITCHOFFSET([ck], '+00:00') AS DATETIME2(7)) AS [ck]");
  }

  @Test
  void getOrderingColumnSql_ShouldQualifyColumnWithTableName() {
    assertThat(rdbEngine.getOrderingColumnSql("ns", "tbl", "ck")).isEqualTo("[tbl].[ck]");
  }

  @Test
  void buildSelectQuery_GivenRangeScanOnTimestampTZClusteringKey_ShouldOrderByQualifiedColumn() {
    SelectQuery query =
        new QueryBuilder(rdbEngine)
            .select(Collections.emptyList())
            .from("ns", "tbl", METADATA_WITH_TIMESTAMPTZ)
            .where(
                Key.ofInt("pk", 1),
                Optional.empty(),
                false,
                Optional.empty(),
                false,
                ImmutableSet.of())
            .build();

    assertThat(query.sql())
        .isEqualTo(
            "SELECT [pk],CAST(SWITCHOFFSET([ck], '+00:00') AS DATETIME2(7)) AS [ck],[v]"
                + " FROM [ns].[tbl] WHERE [pk]=? ORDER BY [tbl].[ck] ASC");
  }

  @Test
  void parseTimestampTZColumn_GivenUtcDateTime_ShouldReturnSameInstant() throws SQLException {
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.getObject("ck", LocalDateTime.class))
        .thenReturn(LocalDateTime.of(1582, 10, 5, 23, 59, 59, 123_000_000));

    assertThat(rdbEngine.parseTimestampTZColumn(resultSet, "ck"))
        .isEqualTo(TimestampTZColumn.of("ck", Instant.parse("1582-10-05T23:59:59.123Z")));
  }

  @Test
  void parseTimestampTZColumn_GivenNull_ShouldReturnNullColumn() throws SQLException {
    ResultSet resultSet = mock(ResultSet.class);
    when(resultSet.getObject("ck", LocalDateTime.class)).thenReturn(null);

    assertThat(rdbEngine.parseTimestampTZColumn(resultSet, "ck"))
        .isEqualTo(TimestampTZColumn.ofNull("ck"));
  }

  @Test
  void getConnectionProperties_ShouldSendStringParametersAsNonUnicode() {
    // Act
    Map<String, String> properties = rdbEngine.getConnectionProperties(mock(JdbcConfig.class));

    // Assert
    assertThat(properties).containsEntry("sendStringParametersAsUnicode", "false");
  }

  @Test
  void getBinaryTextCollationClause_ShouldReturnBin2Utf8Collation() {
    // Arrange
    RdbEngineStrategy strategy = rdbEngine;

    // Act
    String clause = strategy.getBinaryTextCollationClause();

    // Assert
    assertThat(clause).isEqualTo("COLLATE Latin1_General_100_BIN2_UTF8");
  }

  @Test
  void classify_BinOnUtf8_ShouldReturnOtherOrder() {
    // Arrange
    TextColumnCollation collation = new TextColumnCollation("Latin1_General_BIN", "UTF-8");

    // Act
    TextColumnCollationKind actual = rdbEngine.classify(collation);

    // Assert
    assertThat(actual).isEqualTo(TextColumnCollationKind.OTHER_ORDER);
  }

  @Test
  void classify_Bin2InAnyCaseOnUtf8_ShouldReturnCodePointOrder() {
    // Arrange
    TextColumnCollation collation =
        new TextColumnCollation("Latin1_General_100_Bin2_UTF8", "UTF-8");

    // Act
    TextColumnCollationKind actual = rdbEngine.classify(collation);

    // Assert
    assertThat(actual).isEqualTo(TextColumnCollationKind.CODE_POINT_ORDER);
  }

  @Test
  void classify_CodePage_ShouldReturnPartialCharacterSet() {
    // Arrange
    TextColumnCollation collation = new TextColumnCollation("Latin1_General_BIN2", "CP1252");

    // Act
    TextColumnCollationKind actual = rdbEngine.classify(collation);

    // Assert
    assertThat(actual).isEqualTo(TextColumnCollationKind.PARTIAL_CHARACTER_SET);
  }

  @Test
  void getCollationClause_ShouldReturnCollate() {
    // Arrange
    TextColumnCollation collation = new TextColumnCollation("Japanese_BIN2", "CP932");

    // Act
    String clause = rdbEngine.getCollationClause(collation);

    // Assert
    assertThat(clause).isEqualTo("COLLATE Japanese_BIN2");
  }

  @Test
  void getDatabaseTextIssues_Cp1252_ShouldReturnPartialCharacterSet() throws SQLException {
    // Arrange
    Connection connection = mockDatabaseCodePage(1252);

    // Act
    List<DatabaseTextIssue> actual = rdbEngine.getDatabaseTextIssues(connection);

    // Assert
    assertThat(actual)
        .containsExactly(
            new DatabaseTextIssue("CP1252", DatabaseTextIssue.Kind.PARTIAL_CHARACTER_SET));
  }

  @Test
  void getDatabaseTextIssues_Utf8_ShouldReturnEmpty() throws SQLException {
    // Arrange
    Connection connection = mockDatabaseCodePage(65001);

    // Act
    List<DatabaseTextIssue> actual = rdbEngine.getDatabaseTextIssues(connection);

    // Assert
    assertThat(actual).isEmpty();
  }

  private static Connection mockDatabaseCodePage(int codePage) throws SQLException {
    Connection connection = mock(Connection.class);
    Statement statement = mock(Statement.class);
    ResultSet rs = mock(ResultSet.class);
    when(connection.createStatement()).thenReturn(statement);
    when(statement.executeQuery(anyString())).thenReturn(rs);
    when(rs.next()).thenReturn(true);
    when(rs.getInt(1)).thenReturn(codePage);
    return connection;
  }
}
