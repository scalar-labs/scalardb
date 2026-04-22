package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import com.scalar.db.io.TimeColumn;
import com.scalar.db.storage.jdbc.query.InsertOnConflictDoUpdateQuery;
import com.scalar.db.storage.jdbc.query.QueryBuilder;
import com.scalar.db.storage.jdbc.query.UpsertQuery;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RdbEngineSpannerTest {

  private RdbEngineSpanner engine;

  @BeforeEach
  void setUp() {
    engine = new RdbEngineSpanner(mock(JdbcConfig.class));
  }

  @Test
  void getDriverClassName_ShouldReturnSpannerJdbcDriver() {
    // Act
    String driverClassName = engine.getDriverClassName();

    // Assert
    assertThat(driverClassName).isEqualTo("com.google.cloud.spanner.jdbc.JdbcDriver");
  }

  @Test
  void getConnectionProperties_ShouldContainRetryAbortsInternallyFalse() {
    // Arrange
    JdbcConfig config = mock(JdbcConfig.class);

    // Act
    Map<String, String> props = engine.getConnectionProperties(config);

    // Assert
    assertThat(props).containsEntry("retryAbortsInternally", "false");
  }

  @Test
  void getConnectionProperties_ShouldReturnExactlyOneProperty() {
    // Arrange
    JdbcConfig config = mock(JdbcConfig.class);

    // Act
    Map<String, String> props = engine.getConnectionProperties(config);

    // Assert
    assertThat(props).hasSize(1);
  }

  @Test
  void constructor_WithJdbcConfig_ShouldCreateInstance() {
    // Arrange
    JdbcConfig config = mock(JdbcConfig.class);

    // Act
    RdbEngineSpanner engineWithConfig = new RdbEngineSpanner(config);

    // Assert
    assertThat(engineWithConfig).isNotNull();
    assertThat(engineWithConfig.getDriverClassName())
        .isEqualTo("com.google.cloud.spanner.jdbc.JdbcDriver");
  }

  @Test
  void factoryCreate_WithCloudSpannerUrl_ShouldReturnRdbEngineSpanner() {
    // Arrange
    JdbcConfig config = mock(JdbcConfig.class);
    when(config.getJdbcUrl())
        .thenReturn(
            "jdbc:cloudspanner://localhost:9010/projects/test/instances/test/databases/testdb");

    // Act
    RdbEngineStrategy result = RdbEngineFactory.create(config);

    // Assert
    assertThat(result).isInstanceOf(RdbEngineSpanner.class);
  }

  @Test
  void enclose_ShouldReturnDoubleQuotedName_InheritedFromPostgresql() {
    // Assert PostgreSQL dialect inheritance
    assertThat(engine.enclose("my_column")).isEqualTo("\"my_column\"");
  }

  // === Type mapping tests (TYPE-01 through TYPE-10) ===

  @Test
  void getDataTypeForEngine_Boolean_ShouldReturnBoolean() {
    assertThat(engine.getDataTypeForEngine(DataType.BOOLEAN)).isEqualTo("BOOLEAN");
  }

  @Test
  void getDataTypeForEngine_Int_ShouldReturnBigint() {
    // TYPE-02: Spanner PG has no 4-byte int; INT maps to BIGINT
    assertThat(engine.getDataTypeForEngine(DataType.INT)).isEqualTo("BIGINT");
  }

  @Test
  void getDataTypeForEngine_Bigint_ShouldReturnBigint() {
    assertThat(engine.getDataTypeForEngine(DataType.BIGINT)).isEqualTo("BIGINT");
  }

  @Test
  void getDataTypeForEngine_Float_ShouldReturnReal() {
    assertThat(engine.getDataTypeForEngine(DataType.FLOAT)).isEqualTo("REAL");
  }

  @Test
  void getDataTypeForEngine_Double_ShouldReturnDoublePrecision() {
    assertThat(engine.getDataTypeForEngine(DataType.DOUBLE)).isEqualTo("DOUBLE PRECISION");
  }

  @Test
  void getDataTypeForEngine_Text_ShouldReturnText() {
    assertThat(engine.getDataTypeForEngine(DataType.TEXT)).isEqualTo("TEXT");
  }

  @Test
  void getDataTypeForEngine_Blob_ShouldReturnBytea() {
    assertThat(engine.getDataTypeForEngine(DataType.BLOB)).isEqualTo("BYTEA");
  }

  @Test
  void getDataTypeForEngine_Timestamp_ShouldReturnTimestamptz() {
    // TYPE-08: Spanner PG only has TIMESTAMPTZ
    assertThat(engine.getDataTypeForEngine(DataType.TIMESTAMP)).isEqualTo("TIMESTAMPTZ");
  }

  @Test
  void getDataTypeForEngine_Date_ShouldReturnDate() {
    assertThat(engine.getDataTypeForEngine(DataType.DATE)).isEqualTo("DATE");
  }

  @Test
  void getDataTypeForEngine_Time_ShouldReturnTimestamptz6() {
    // TYPE-10: No native TIME type in Spanner PG. Spanner PG does not support type modifiers
    // for timestamptz (TIMESTAMPTZ(6) is invalid). Use plain TIMESTAMPTZ which provides
    // microsecond precision by default.
    assertThat(engine.getDataTypeForEngine(DataType.TIME)).isEqualTo("TIMESTAMPTZ");
  }

  @Test
  void getDataTypeForEngine_Timestamptz_ShouldReturnTimestampWithTimeZone() {
    assertThat(engine.getDataTypeForEngine(DataType.TIMESTAMPTZ))
        .isEqualTo("TIMESTAMP WITH TIME ZONE");
  }

  // === getSqlTypes tests ===

  @Test
  void getSqlTypes_Int_ShouldReturnBigint() {
    assertThat(engine.getSqlTypes(DataType.INT)).isEqualTo(Types.BIGINT);
  }

  @Test
  void getSqlTypes_Time_ShouldReturnTimestampWithTimezone() {
    assertThat(engine.getSqlTypes(DataType.TIME)).isEqualTo(Types.TIMESTAMP_WITH_TIMEZONE);
  }

  // === TRUNCATE emulation (DDL-09) ===

  @Test
  void truncateTableSql_ShouldReturnDeleteStatement() {
    String sql = engine.truncateTableSql("my_schema", "my_table");
    assertThat(sql).isEqualTo("DELETE FROM \"my_schema\".\"my_table\" WHERE TRUE");
  }

  // === Error detection (gRPC codes) ===

  @Test
  void isDuplicateTableError_WithGrpcAlreadyExists_ShouldReturnTrue() {
    SQLException e = mock(SQLException.class);
    when(e.getErrorCode()).thenReturn(6);
    assertThat(engine.isDuplicateTableError(e)).isTrue();
  }

  @Test
  void isDuplicateTableError_WithOtherCode_ShouldReturnFalse() {
    SQLException e = mock(SQLException.class);
    when(e.getErrorCode()).thenReturn(5);
    assertThat(engine.isDuplicateTableError(e)).isFalse();
  }

  @Test
  void isDuplicateTableError_WithNullSqlState_ShouldStillDetect() {
    // Spanner JDBC driver always sends null SQLSTATE
    SQLException e = mock(SQLException.class);
    when(e.getSQLState()).thenReturn(null);
    when(e.getErrorCode()).thenReturn(6);
    assertThat(engine.isDuplicateTableError(e)).isTrue();
  }

  @Test
  void isDuplicateKeyError_WithGrpcAlreadyExists_ShouldReturnTrue() {
    SQLException e = mock(SQLException.class);
    when(e.getErrorCode()).thenReturn(6);
    assertThat(engine.isDuplicateKeyError(e)).isTrue();
  }

  @Test
  void isUndefinedTableError_WithGrpcNotFound_ShouldReturnTrue() {
    SQLException e = mock(SQLException.class);
    when(e.getErrorCode()).thenReturn(5);
    assertThat(engine.isUndefinedTableError(e)).isTrue();
  }

  @Test
  void isUndefinedTableError_WithOtherCode_ShouldReturnFalse() {
    SQLException e = mock(SQLException.class);
    when(e.getErrorCode()).thenReturn(6);
    assertThat(engine.isUndefinedTableError(e)).isFalse();
  }

  @Test
  void isConflict_WithGrpcAborted_ShouldReturnTrue() {
    SQLException e = mock(SQLException.class);
    when(e.getErrorCode()).thenReturn(10);
    assertThat(engine.isConflict(e)).isTrue();
  }

  @Test
  void isConflict_WithOtherCode_ShouldReturnFalse() {
    SQLException e = mock(SQLException.class);
    when(e.getErrorCode()).thenReturn(6);
    assertThat(engine.isConflict(e)).isFalse();
  }

  // === Unsupported operations (D-15) ===

  @Test
  void throwIfRenameColumnNotSupported_ShouldAlwaysThrow() {
    assertThatThrownBy(
            () -> engine.throwIfRenameColumnNotSupported("col", mock(TableMetadata.class)))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("Spanner does not support renaming columns");
  }

  @Test
  void throwIfAlterColumnTypeNotSupported_ShouldAlwaysThrow() {
    assertThatThrownBy(() -> engine.throwIfAlterColumnTypeNotSupported(DataType.INT, DataType.TEXT))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("Spanner does not support altering the column type");
  }

  @Test
  void renameTableSql_ShouldThrowUnsupportedOperationException() {
    assertThatThrownBy(() -> engine.renameTableSql("schema", "old", "new"))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("Spanner does not support renaming tables");
  }

  // === Time type strategy (TYPE-11) ===

  @Test
  void getTimeTypeStrategy_ShouldReturnRdbEngineTimeTypeSpanner() {
    assertThat(engine.getTimeTypeStrategy()).isInstanceOf(RdbEngineTimeTypeSpanner.class);
  }

  // === parseTimeColumn null handling ===

  @Test
  void parseTimeColumn_WithNullValue_ShouldReturnNullTimeColumn() throws SQLException {
    ResultSet rs = mock(ResultSet.class);
    when(rs.getObject("col", OffsetDateTime.class)).thenReturn(null);
    TimeColumn result = engine.parseTimeColumn(rs, "col");
    assertThat(result.hasNullValue()).isTrue();
  }

  @Test
  void parseTimeColumn_WithValue_ShouldExtractLocalTime() throws SQLException {
    ResultSet rs = mock(ResultSet.class);
    OffsetDateTime odt = OffsetDateTime.of(1970, 1, 1, 14, 30, 45, 0, ZoneOffset.UTC);
    when(rs.getObject("col", OffsetDateTime.class)).thenReturn(odt);
    TimeColumn result = engine.parseTimeColumn(rs, "col");
    assertThat(result.getTimeValue()).isEqualTo(LocalTime.of(14, 30, 45));
  }

  // === UPSERT tests (CRUD-05) ===

  @Test
  void buildUpsertQuery_ShouldReturnInsertOnConflictDoUpdateQueryWithExcludedAlias() {
    // Arrange
    TableMetadata metadata =
        TableMetadata.newBuilder()
            .addColumn("pk", DataType.TEXT)
            .addColumn("val", DataType.TEXT)
            .addPartitionKey("pk")
            .build();
    QueryBuilder queryBuilder = new QueryBuilder(engine);
    UpsertQuery.Builder builder = queryBuilder.upsertInto("ns", "tbl", metadata);
    builder.values(Key.ofText("pk", "pkVal"), Optional.empty(), createColumns("val", "valData"));

    // Act
    UpsertQuery query = builder.build();

    // Assert
    assertThat(query).isInstanceOf(InsertOnConflictDoUpdateQuery.class);
    // Verify the excluded-alias SET form is used (Spanner-specific)
    assertThat(query.sql()).contains("=excluded.");
  }

  @Test
  void buildUpsertQuery_ShouldGenerateExcludedBasedSql() {
    // Arrange
    TableMetadata metadata =
        TableMetadata.newBuilder()
            .addColumn("pk", DataType.TEXT)
            .addColumn("col1", DataType.TEXT)
            .addColumn("col2", DataType.TEXT)
            .addPartitionKey("pk")
            .build();
    QueryBuilder queryBuilder = new QueryBuilder(engine);
    UpsertQuery.Builder builder = queryBuilder.upsertInto("ns", "tbl", metadata);
    Map<String, Column<?>> columns = new LinkedHashMap<>();
    columns.put("col1", TextColumn.of("col1", "v1"));
    columns.put("col2", TextColumn.of("col2", "v2"));
    builder.values(Key.ofText("pk", "pkVal"), Optional.empty(), columns);

    // Act
    UpsertQuery query = builder.build();

    // Assert
    String sql = query.sql();
    assertThat(sql).contains("excluded.\"col1\"");
    assertThat(sql).contains("excluded.\"col2\"");
    assertThat(sql).doesNotContain("SET \"col1\"=?");
    assertThat(sql).doesNotContain("SET \"col2\"=?");
  }

  @Test
  void buildUpsertQuery_WithEmptyColumns_ShouldGenerateDoNothing() {
    // Arrange
    TableMetadata metadata =
        TableMetadata.newBuilder().addColumn("pk", DataType.TEXT).addPartitionKey("pk").build();
    QueryBuilder queryBuilder = new QueryBuilder(engine);
    UpsertQuery.Builder builder = queryBuilder.upsertInto("ns", "tbl", metadata);
    builder.values(Key.ofText("pk", "pkVal"), Optional.empty(), new LinkedHashMap<>());

    // Act
    UpsertQuery query = builder.build();

    // Assert
    assertThat(query.sql()).contains("DO NOTHING");
  }

  // === Transaction commit behavior (CRUD-08, per D-10) ===

  @Test
  void requiresExplicitCommit_ShouldReturnFalse() {
    // Per D-10: Spanner JDBC autocommit=true by default, inherited false is correct
    // Method signature: requiresExplicitCommit(int isolationLevel)
    assertThat(engine.requiresExplicitCommit(Connection.TRANSACTION_READ_COMMITTED)).isFalse();
  }

  private Map<String, Column<?>> createColumns(String name, String value) {
    Map<String, Column<?>> columns = new LinkedHashMap<>();
    columns.put(name, TextColumn.of(name, value));
    return columns;
  }
}
