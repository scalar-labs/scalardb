package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.rpc.Code;
import com.scalar.db.api.LikeExpression;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import com.scalar.db.io.DateColumn;
import com.scalar.db.io.TimeColumn;
import com.scalar.db.io.TimestampColumn;
import com.zaxxer.hikari.HikariConfig;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RdbEngineSpannerTest {

  private RdbEngineSpanner rdbEngine;

  @BeforeEach
  void setUp() {
    rdbEngine = new RdbEngineSpanner();
  }

  @AfterEach
  void tearDown() {
    SpannerCredentialsProvider.clear();
  }

  @Test
  void getDriverClassName_ShouldReturnSpannerDriver() {
    assertThat(rdbEngine.getDriverClassName())
        .isEqualTo("com.google.cloud.spanner.jdbc.JdbcDriver");
  }

  @Test
  void getDataTypeForEngine_ShouldMapEachScalarDbDataTypeCorrectly() {
    assertThat(rdbEngine.getDataTypeForEngine(DataType.INT)).isEqualTo("bigint");
    assertThat(rdbEngine.getDataTypeForEngine(DataType.BIGINT)).isEqualTo("bigint");
    assertThat(rdbEngine.getDataTypeForEngine(DataType.BLOB)).isEqualTo("bytea");
    assertThat(rdbEngine.getDataTypeForEngine(DataType.BOOLEAN)).isEqualTo("boolean");
    assertThat(rdbEngine.getDataTypeForEngine(DataType.FLOAT)).isEqualTo("real");
    assertThat(rdbEngine.getDataTypeForEngine(DataType.DOUBLE)).isEqualTo("double precision");
    assertThat(rdbEngine.getDataTypeForEngine(DataType.TEXT)).isEqualTo("text");
    assertThat(rdbEngine.getDataTypeForEngine(DataType.DATE)).isEqualTo("date");
    assertThat(rdbEngine.getDataTypeForEngine(DataType.TIME)).isEqualTo("timestamp with time zone");
    assertThat(rdbEngine.getDataTypeForEngine(DataType.TIMESTAMP))
        .isEqualTo("timestamp with time zone");
    assertThat(rdbEngine.getDataTypeForEngine(DataType.TIMESTAMPTZ))
        .isEqualTo("timestamp with time zone");
  }

  @Test
  void getDataTypeForKey_ShouldReturnNullForAllTypes() {
    for (DataType dataType : DataType.values()) {
      assertThat(rdbEngine.getDataTypeForKey(dataType)).isNull();
    }
  }

  @Test
  void getSqlTypes_IntShouldMapToBigint() {
    assertThat(rdbEngine.getSqlTypes(DataType.INT)).isEqualTo(Types.BIGINT);
  }

  @Test
  void getSqlTypes_TimeAndTimestampShouldMapToTimestampWithTimezone() {
    assertThat(rdbEngine.getSqlTypes(DataType.TIME)).isEqualTo(Types.TIMESTAMP_WITH_TIMEZONE);
    assertThat(rdbEngine.getSqlTypes(DataType.TIMESTAMP)).isEqualTo(Types.TIMESTAMP_WITH_TIMEZONE);
  }

  @Test
  void getSqlTypes_OtherTypesShouldFallThroughToPostgresqlMapping() {
    // Sanity: non-overridden type should not return BIGINT or TIMESTAMP_WITH_TIMEZONE.
    assertThat(rdbEngine.getSqlTypes(DataType.TEXT)).isNotEqualTo(Types.BIGINT);
    assertThat(rdbEngine.getSqlTypes(DataType.BOOLEAN)).isNotEqualTo(Types.TIMESTAMP_WITH_TIMEZONE);
  }

  @Test
  void truncateTableSql_ShouldReturnDeleteWhereTrue() {
    assertThat(rdbEngine.truncateTableSql("ns", "tbl"))
        .isEqualTo("DELETE FROM \"ns\".\"tbl\" WHERE TRUE");
  }

  @Test
  void renameTableSql_ShouldThrowUnsupportedOperationException() {
    assertThatThrownBy(() -> rdbEngine.renameTableSql("ns", "old", "new"))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void dropTableSql_WithoutIndexesOrMixedOrders_ShouldReturnDropTableOnly() {
    TableMetadata metadata = mock(TableMetadata.class);
    when(metadata.getSecondaryIndexNames()).thenReturn(Collections.emptySet());
    when(metadata.getClusteringOrders()).thenReturn(Collections.emptyMap());

    String[] sqls = rdbEngine.dropTableSql(metadata, "ns", "tbl");

    assertThat(sqls).hasSize(1);
    assertThat(sqls[0]).isEqualTo("DROP TABLE \"ns\".\"tbl\"");
  }

  @Test
  void dropTableSql_WithSecondaryIndexes_ShouldDropIndexesBeforeTable() {
    TableMetadata metadata = mock(TableMetadata.class);
    when(metadata.getSecondaryIndexNames()).thenReturn(ImmutableSet.of("c1"));
    when(metadata.getClusteringOrders()).thenReturn(Collections.emptyMap());

    String[] sqls = rdbEngine.dropTableSql(metadata, "ns", "tbl");

    assertThat(sqls).hasSize(2);
    assertThat(sqls[0]).startsWith("DROP INDEX");
    assertThat(sqls[1]).isEqualTo("DROP TABLE \"ns\".\"tbl\"");
  }

  @Test
  void dropTableSql_WithMixedClusteringOrders_ShouldDropClusteringOrderIndex() {
    TableMetadata metadata = mock(TableMetadata.class);
    when(metadata.getSecondaryIndexNames()).thenReturn(Collections.emptySet());
    when(metadata.getClusteringOrders())
        .thenReturn(ImmutableMap.of("ck1", Order.ASC, "ck2", Order.DESC));

    String[] sqls = rdbEngine.dropTableSql(metadata, "ns", "tbl");

    assertThat(sqls).hasSize(2);
    assertThat(sqls[0]).startsWith("DROP INDEX");
    assertThat(sqls[1]).isEqualTo("DROP TABLE \"ns\".\"tbl\"");
  }

  @Test
  void getEscape_BackslashEscape_ShouldReturnNull() {
    LikeExpression like = mock(LikeExpression.class);
    when(like.getEscape()).thenReturn("\\");

    assertThat(rdbEngine.getEscape(like)).isNull();
  }

  @Test
  void getEscape_NonBackslashEscape_ShouldThrowUnsupportedOperationException() {
    LikeExpression like = mock(LikeExpression.class);
    when(like.getEscape()).thenReturn("+");

    assertThatThrownBy(() -> rdbEngine.getEscape(like))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void getEscape_EmptyEscape_ShouldThrowUnsupportedOperationException() {
    LikeExpression like = mock(LikeExpression.class);
    when(like.getEscape()).thenReturn("");

    assertThatThrownBy(() -> rdbEngine.getEscape(like))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void isDuplicateTableError_ShouldAlwaysReturnFalse() {
    SQLException e = mock(SQLException.class);
    when(e.getErrorCode()).thenReturn(Code.ALREADY_EXISTS_VALUE);

    assertThat(rdbEngine.isDuplicateTableError(e)).isFalse();
  }

  @Test
  void isCreateMetadataSchemaDuplicateSchemaError_ShouldAlwaysReturnFalse() {
    SQLException e = mock(SQLException.class);
    when(e.getErrorCode()).thenReturn(Code.ALREADY_EXISTS_VALUE);

    assertThat(rdbEngine.isCreateMetadataSchemaDuplicateSchemaError(e)).isFalse();
  }

  @Test
  void isDuplicateKeyError_AlreadyExistsCode_ShouldReturnTrue() {
    SQLException e = mock(SQLException.class);
    when(e.getErrorCode()).thenReturn(Code.ALREADY_EXISTS_VALUE);

    assertThat(rdbEngine.isDuplicateKeyError(e)).isTrue();
  }

  @Test
  void isDuplicateKeyError_OtherCode_ShouldReturnFalse() {
    SQLException e = mock(SQLException.class);
    when(e.getErrorCode()).thenReturn(Code.NOT_FOUND_VALUE);

    assertThat(rdbEngine.isDuplicateKeyError(e)).isFalse();
  }

  @Test
  void isUndefinedTableError_InvalidArgumentCode_ShouldReturnTrue() {
    SQLException e = mock(SQLException.class);
    when(e.getErrorCode()).thenReturn(Code.INVALID_ARGUMENT_VALUE);

    assertThat(rdbEngine.isUndefinedTableError(e)).isTrue();
  }

  @Test
  void isUndefinedTableError_OtherCode_ShouldReturnFalse() {
    SQLException e = mock(SQLException.class);
    when(e.getErrorCode()).thenReturn(Code.NOT_FOUND_VALUE);

    assertThat(rdbEngine.isUndefinedTableError(e)).isFalse();
  }

  @Test
  void isUndefinedIndexError_NotFoundCode_ShouldReturnTrue() {
    SQLException e = mock(SQLException.class);
    when(e.getErrorCode()).thenReturn(Code.NOT_FOUND_VALUE);

    assertThat(rdbEngine.isUndefinedIndexError(e)).isTrue();
  }

  @Test
  void isUndefinedIndexError_OtherCode_ShouldReturnFalse() {
    SQLException e = mock(SQLException.class);
    when(e.getErrorCode()).thenReturn(Code.INVALID_ARGUMENT_VALUE);

    assertThat(rdbEngine.isUndefinedIndexError(e)).isFalse();
  }

  @Test
  void isConflict_AbortedCode_ShouldReturnTrue() {
    SQLException e = mock(SQLException.class);
    when(e.getErrorCode()).thenReturn(Code.ABORTED_VALUE);

    assertThat(rdbEngine.isConflict(e)).isTrue();
  }

  @Test
  void isConflict_OtherCode_ShouldReturnFalse() {
    SQLException e = mock(SQLException.class);
    when(e.getErrorCode()).thenReturn(Code.ALREADY_EXISTS_VALUE);

    assertThat(rdbEngine.isConflict(e)).isFalse();
  }

  @Test
  void throwIfRenameColumnNotSupported_ShouldAlwaysThrow() {
    TableMetadata metadata = mock(TableMetadata.class);
    assertThatThrownBy(() -> rdbEngine.throwIfRenameColumnNotSupported("col", metadata))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void throwIfAlterColumnTypeNotSupported_BlobToText_ShouldNotThrow() {
    assertThatCode(() -> rdbEngine.throwIfAlterColumnTypeNotSupported(DataType.BLOB, DataType.TEXT))
        .doesNotThrowAnyException();
  }

  @Test
  void throwIfAlterColumnTypeNotSupported_TextToBlob_ShouldThrow() {
    // Reverse direction is not supported.
    assertThatThrownBy(
            () -> rdbEngine.throwIfAlterColumnTypeNotSupported(DataType.TEXT, DataType.BLOB))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void throwIfAlterColumnTypeNotSupported_IntToBigint_ShouldThrow() {
    assertThatThrownBy(
            () -> rdbEngine.throwIfAlterColumnTypeNotSupported(DataType.INT, DataType.BIGINT))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void requiresExplicitDropIndexBeforeDropColumn_ShouldReturnTrue() {
    assertThat(rdbEngine.requiresExplicitDropIndexBeforeDropColumn()).isTrue();
  }

  @Test
  void parseTimeColumn_NullValue_ShouldReturnNullColumn() throws SQLException {
    ResultSet rs = mock(ResultSet.class);
    when(rs.getObject("t", OffsetDateTime.class)).thenReturn(null);

    TimeColumn column = rdbEngine.parseTimeColumn(rs, "t");

    assertThat(column.hasNullValue()).isTrue();
    assertThat(column.getName()).isEqualTo("t");
  }

  @Test
  void parseTimeColumn_NonUtcOffset_ShouldNormalizeToUtc() throws SQLException {
    ResultSet rs = mock(ResultSet.class);
    // 14:30 at +09:00 == 05:30 UTC
    OffsetDateTime stored = OffsetDateTime.of(2024, 1, 1, 14, 30, 0, 0, ZoneOffset.ofHours(9));
    when(rs.getObject("t", OffsetDateTime.class)).thenReturn(stored);

    TimeColumn column = rdbEngine.parseTimeColumn(rs, "t");

    assertThat(column.hasNullValue()).isFalse();
    assertThat(column.getTimeValue()).isEqualTo(LocalTime.of(5, 30));
  }

  @Test
  void parseTimestampColumn_NullValue_ShouldReturnNullColumn() throws SQLException {
    ResultSet rs = mock(ResultSet.class);
    when(rs.getObject("ts", OffsetDateTime.class)).thenReturn(null);

    TimestampColumn column = rdbEngine.parseTimestampColumn(rs, "ts");

    assertThat(column.hasNullValue()).isTrue();
    assertThat(column.getName()).isEqualTo("ts");
  }

  @Test
  void parseTimestampColumn_NonUtcOffset_ShouldNormalizeToUtc() throws SQLException {
    ResultSet rs = mock(ResultSet.class);
    // 2024-06-15 14:30:45 at +09:00 == 2024-06-15 05:30:45 UTC
    OffsetDateTime stored = OffsetDateTime.of(2024, 6, 15, 14, 30, 45, 0, ZoneOffset.ofHours(9));
    when(rs.getObject("ts", OffsetDateTime.class)).thenReturn(stored);

    TimestampColumn column = rdbEngine.parseTimestampColumn(rs, "ts");

    assertThat(column.hasNullValue()).isFalse();
    assertThat(column.getTimestampValue()).isEqualTo(LocalDateTime.of(2024, 6, 15, 5, 30, 45));
  }

  @Test
  void parseDateColumn_NullValue_ShouldReturnNullColumn() throws SQLException {
    ResultSet rs = mock(ResultSet.class);
    when(rs.getString("d")).thenReturn(null);

    DateColumn column = rdbEngine.parseDateColumn(rs, "d");

    assertThat(column.hasNullValue()).isTrue();
    assertThat(column.getName()).isEqualTo("d");
  }

  @Test
  void parseDateColumn_ValidDateString_ShouldParseToLocalDate() throws SQLException {
    ResultSet rs = mock(ResultSet.class);
    when(rs.getString("d")).thenReturn("2024-06-15");

    DateColumn column = rdbEngine.parseDateColumn(rs, "d");

    assertThat(column.hasNullValue()).isFalse();
    assertThat(column.getDateValue()).isEqualTo(LocalDate.of(2024, 6, 15));
  }

  @Test
  void setConnectionCredentials_NoPassword_ShouldNotConfigureProvider() {
    JdbcConfig config = mock(JdbcConfig.class);
    when(config.getPassword()).thenReturn(Optional.empty());
    HikariConfig hikariConfig = mock(HikariConfig.class);

    rdbEngine.setConnectionCredentials(config, hikariConfig);

    verify(hikariConfig, never()).addDataSourceProperty(eq("credentialsProvider"), any());
  }

  @Test
  void setConnectionCredentials_ValidServiceAccountKey_ShouldRegisterAndSetProperty() {
    JdbcConfig config = mock(JdbcConfig.class);
    when(config.getPassword()).thenReturn(Optional.of(SAMPLE_SERVICE_ACCOUNT_KEY));
    HikariConfig hikariConfig = mock(HikariConfig.class);

    rdbEngine.setConnectionCredentials(config, hikariConfig);

    // The first registration in a clean JVM goes into Slot0.
    verify(hikariConfig, times(1))
        .addDataSourceProperty(
            "credentialsProvider", SpannerCredentialsProvider.Slot0.class.getName());
    assertThat(new SpannerCredentialsProvider.Slot0().getCredentials()).isNotNull();
  }

  @Test
  void setConnectionCredentials_InvalidKey_ShouldThrowIllegalArgumentException() {
    JdbcConfig config = mock(JdbcConfig.class);
    when(config.getPassword()).thenReturn(Optional.of("not-a-service-account-json"));
    HikariConfig hikariConfig = mock(HikariConfig.class);

    assertThatThrownBy(() -> rdbEngine.setConnectionCredentials(config, hikariConfig))
        .isInstanceOf(IllegalArgumentException.class);
    verify(hikariConfig, never()).addDataSourceProperty(eq("credentialsProvider"), any());
  }

  /**
   * A syntactically valid (but non-functional) service-account key — enough for {@code
   * GoogleCredentials.fromStream} to parse without making a network call. The private key is a
   * freshly-generated RSA key not associated with any real Google Cloud account.
   */
  private static final String SAMPLE_SERVICE_ACCOUNT_KEY =
      "{\n"
          + "  \"type\": \"service_account\",\n"
          + "  \"project_id\": \"test-project\",\n"
          + "  \"private_key_id\": \"00000000000000000000000000000000\",\n"
          + "  \"private_key\": \"-----BEGIN PRIVATE KEY-----\\n"
          + "MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDJ5uvFzWuQJG+e\\n"
          + "pgWcRk/1nKQqQQTQFA8D1lxFB2QM7NlV4bAnHU7u4q5sGmu/Wkp8VeQmPCb1Cqtm\\n"
          + "9PpKcQlmgY0HYqWvk+TgnBp6qYoLWXjrUiwT8FhMQeqGn1x0mflp1tHo/p/bYYpJ\\n"
          + "lDc3VXkD0Rx6Z2QdOG5y+kMU4Df1BX2xvGmx2OBP3K9hREsUlHWN8TUMBl3uAvjB\\n"
          + "qflbS6FA2kmJrK6pIY7w08DTkvmf4nW4xMtWMS/X5y+fQeYg0yc7Bd5b4LPlfWJ7\\n"
          + "+H/qZWkfEoq3pCVpHocoVSahNGowS4JfIv5PT9TXk8m4Yg4xpgFNIz5uUgsVDohh\\n"
          + "UyR9OaRnAgMBAAECggEAOoaPjdU6/hpPx9MX0KVxUUH7VVHYkmf7AnphLWjfAMBT\\n"
          + "8rLAZSZ3gXBgaQ6f4f8gsEAfYn0wZc+I2spJL0V3X0xfEGwzkQUAPkpcaPuJAJIm\\n"
          + "ouEKfkPCYsDJhBu6nqJ0Q1G8mztMAdvdwQHjRy03PQ5Z+rK2qEOTKOqXqGTLrIhu\\n"
          + "Z9j5TBN0F+QMpfM6EAMCihMpAA3dVoH+6MvK0c2tLwqOQ6eWylPbVyzCVgNcJC8I\\n"
          + "gE0RxIb+u4KYdt1ECqhrHckRjV7xUIqjKqMqYY/rrAIDC0OzPe2C/HEN6Y+mqg5Z\\n"
          + "PQYW5L6dQwUmBn0cJ30/w7Sf/8Rk+yEQU6f5vGEpgQKBgQD+NS3dFpDjeySt9HhT\\n"
          + "EzFnzJ/RYMQiPofPgpwGWE/QYPaKdQtaFu2pHjAzZ8HDvMGpDHYK4Bz1q6VKNYQ7\\n"
          + "yFY4bJv7uG6ZkGLbtcfSxSpqWYzz4cpSBjzpb7K6S2QaHxHxFaCbgGxC2hcJNTMG\\n"
          + "8MmS8VgvgDxuJ9R4Bpp6yvHv+wKBgQDLZfOJZ8UzS9gmkF9k1zN+w9Z8jp5l5Bne\\n"
          + "Y3WP5Ctwf5HzZk2FbTDP9j2LGhSlGOj8dfbOl4Gh9w6JzLgYvDKIrV5G7DWkoNZ7\\n"
          + "HxRjrAgxoYhOoofmBpe0IxYyZMe+i1B0c8M+EfQF13X1uwLqWaT4Tcw6QIxuNi8E\\n"
          + "QmKuLY01BQKBgFWqB2gDYKD1iNJp+qEK35JHYTxc6Zwgl3a4ZyyOY6ePzFdQ3K6v\\n"
          + "fU2rH+CkQGJeXqe3QwDIBafMmRu9N7sC9o7NDCeyahVLAE6NxYpnOhz3Y9Lu0AVe\\n"
          + "oyV2ESgKR+TrJM3uLbTApMrZgUPYPoynRjJZhZ8XU+Y5bSMqrUOPmO99AoGBAJtA\\n"
          + "TEZD9WuQOzbAKxXxmm+e2YT8JgC+XpBjZEgPL5T+ZrV2bk8Y2hpeP2ydeBz6Hh4k\\n"
          + "FjC5bjMuLArD5y1eCrqjdNXLdIFNMLlhZCwgI31g9BYZxuDdiTkMOZkEEv5SBGYQ\\n"
          + "5WDKnAxsLpdcLsDdMhgexsQk59m/Jz6oKnCAbHJ9AoGAOxDbV/Z31D4mOjkTW2HG\\n"
          + "0OVkxgcjFmRf06AbTVO0pCZ6fH3vjJj1hytiAswABQ41DkhBKyaKv8OzvAbACEPs\\n"
          + "lWY/LqJ5vG2x3/jvJSc6YL8FUBmcBzaLNfDoXM4gOxQzRzxEm4XKdL+wxx6/RFhE\\n"
          + "0Ns6YZmhjXzqnzVOUBNG7vU=\\n"
          + "-----END PRIVATE KEY-----\\n\",\n"
          + "  \"client_email\": \"test@test-project.iam.gserviceaccount.com\",\n"
          + "  \"client_id\": \"000000000000000000000\",\n"
          + "  \"auth_uri\": \"https://accounts.google.com/o/oauth2/auth\",\n"
          + "  \"token_uri\": \"https://oauth2.googleapis.com/token\"\n"
          + "}";
}
