package com.scalar.db.storage.jdbc;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.LikeExpression;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.CoreError;
import com.scalar.db.io.DataType;
import com.scalar.db.io.DateColumn;
import com.scalar.db.io.TimeColumn;
import com.scalar.db.io.TimestampColumn;
import com.scalar.db.storage.jdbc.query.InsertOnConflictDoUpdateExcludedQuery;
import com.scalar.db.storage.jdbc.query.UpsertQuery;
import com.zaxxer.hikari.HikariConfig;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

class RdbEngineSpanner extends RdbEnginePostgresql {

  private final RdbEngineTimeTypeSpanner timeTypeEngine;

  @VisibleForTesting
  RdbEngineSpanner() {
    timeTypeEngine = new RdbEngineTimeTypeSpanner();
  }

  @Override
  public String getDriverClassName() {
    return "com.google.cloud.spanner.jdbc.JdbcDriver";
  }

  @Override
  public String getDataTypeForEngine(DataType scalarDbDataType) {
    switch (scalarDbDataType) {
      case TEXT:
        return "text";
      case INT:
        return "bigint"; // Spanner PG int is always 8-byte
      case TIME:
      case TIMESTAMP:
        return "timestamptz";
      default:
        return super.getDataTypeForEngine(scalarDbDataType);
    }
  }

  @Override
  public String getDataTypeForKey(DataType dataType) {
    return null;
  }

  @Override
  public int getSqlTypes(DataType dataType) {
    switch (dataType) {
      case INT:
        return Types.BIGINT;
      case TIME:
      case TIMESTAMP:
        return Types.TIMESTAMP_WITH_TIMEZONE;
      default:
        return super.getSqlTypes(dataType);
    }
  }

  @Override
  public String truncateTableSql(String namespace, String table) {
    return "DELETE FROM " + encloseFullTableName(namespace, table) + " WHERE TRUE";
  }

  @Override
  public boolean isDuplicateTableError(SQLException e) {
    // In Spanner GoogleSQL dialect, duplicate errors use gRPC ALREADY_EXISTS = 6.
    // In Spanner PostgreSQL dialect, duplicate table/schema errors use gRPC FAILED_PRECONDITION = 9
    // with a message like "Duplicate name in schema: schema.table" or "Duplicate name in schema:
    // schema_name."
    return isSpannerDuplicateNameError(e);
  }

  @Override
  public boolean isCreateMetadataSchemaDuplicateSchemaError(SQLException e) {
    // In Spanner PG dialect, duplicate schema errors use gRPC FAILED_PRECONDITION = 9
    // with message "Duplicate name in schema: schema_name."
    return isSpannerDuplicateNameError(e);
  }

  private boolean isSpannerDuplicateNameError(SQLException e) {
    if (e.getErrorCode() == 6) {
      return true; // ALREADY_EXISTS (GoogleSQL dialect)
    }
    String message = e.getMessage();
    return e.getErrorCode() == 9 && message != null && message.contains("Duplicate name in schema");
  }

  @Override
  public boolean isDuplicateKeyError(SQLException e) {
    // Spanner JDBC driver passes null SQLSTATE; uses gRPC ALREADY_EXISTS = 6
    return e.getErrorCode() == 6;
  }

  @Override
  public boolean isUndefinedTableError(SQLException e) {
    // In Spanner GoogleSQL dialect, undefined table errors use gRPC NOT_FOUND = 5.
    // In Spanner PostgreSQL dialect, undefined relation errors use gRPC INVALID_ARGUMENT = 3
    // with a message like "relation ... does not exist".
    if (e.getErrorCode() == 5) {
      return true;
    }
    String message = e.getMessage();
    return e.getErrorCode() == 3 && message != null && message.contains("does not exist");
  }

  @Override
  public boolean isConflict(SQLException e) {
    // Spanner JDBC driver passes null SQLSTATE; uses gRPC ABORTED = 10
    return e.getErrorCode() == 10;
  }

  @Override
  public void throwIfRenameColumnNotSupported(String columnName, TableMetadata tableMetadata) {
    throw new UnsupportedOperationException(
        CoreError.JDBC_SPANNER_RENAME_COLUMN_NOT_SUPPORTED.buildMessage());
  }

  @Override
  public void throwIfAlterColumnTypeNotSupported(DataType from, DataType to) {
    if (!(from == DataType.BLOB && to == DataType.TEXT)) {
      throw new UnsupportedOperationException(
          CoreError.JDBC_SPANNER_UNSUPPORTED_COLUMN_TYPE_CONVERSION.buildMessage(
              from.toString(), to.toString()));
    }
  }

  @Override
  public String renameTableSql(String namespace, String oldTableName, String newTableName) {
    // Renaming a table is supported but is quite limited since it relocates it to the `public`
    // schema instead of keeping it in the original schema. Because of this limitation, we can't
    // support it.
    throw new UnsupportedOperationException(
        CoreError.JDBC_SPANNER_RENAME_TABLE_NOT_SUPPORTED.buildMessage());
  }

  @Override
  public boolean requiresExplicitIndexDropBeforeDropColumn() {
    return true;
  }

  @Override
  public boolean requiresExplicitIndexDropBeforeDropTable() {
    return true;
  }

  @Override
  public RdbEngineTimeTypeStrategy<LocalDate, OffsetDateTime, OffsetDateTime, OffsetDateTime>
      getTimeTypeStrategy() {
    return timeTypeEngine;
  }

  @Override
  public String createTableInternalPrimaryKeyClause(
      boolean hasDescClusteringOrder, TableMetadata metadata) {
    // TODO MySQL can create a primary key clauses by specifying the ordering on each column,
    // the emulator rejected it but check if the real Spanner can do it
    return super.createTableInternalPrimaryKeyClause(hasDescClusteringOrder, metadata);
  }

  @Override
  public TimeColumn parseTimeColumn(ResultSet resultSet, String columnName) throws SQLException {
    OffsetDateTime odt = resultSet.getObject(columnName, OffsetDateTime.class);
    if (odt == null) {
      return TimeColumn.ofNull(columnName);
    }
    return TimeColumn.ofStrict(columnName, odt.withOffsetSameInstant(ZoneOffset.UTC).toLocalTime());
  }

  @Override
  public TimestampColumn parseTimestampColumn(ResultSet resultSet, String columnName)
      throws SQLException {
    OffsetDateTime odt = resultSet.getObject(columnName, OffsetDateTime.class);
    if (odt == null) {
      return TimestampColumn.ofNull(columnName);
    }
    return TimestampColumn.ofStrict(
        columnName, odt.withOffsetSameInstant(ZoneOffset.UTC).toLocalDateTime());
  }

  @Override
  public String[] createTableInternalSqlsAfterCreateTable(
      boolean hasDifferentClusteringOrders,
      String schema,
      String table,
      TableMetadata metadata,
      boolean ifNotExists) {
    // Spanner PG does not support ASC/DESC in the PRIMARY KEY clause of CREATE TABLE, and does not
    // support backward index scans. To enforce clustering order, create a separate unique index
    // with explicit ASC/DESC on the primary key columns.
    // Spanner PG does not support schema-qualified index names in CREATE INDEX.
    // Unlike PostgreSQL, the index name must be unqualified (no "schema.index_name" syntax).
    ArrayList<String> sqls = new ArrayList<>();
    if (hasDifferentClusteringOrders) {
      sqls.add(
          "CREATE UNIQUE INDEX "
              + (ifNotExists ? "IF NOT EXISTS " : "")
              + enclose(table + "_clustering_order_idx")
              + " ON "
              + encloseFullTableName(schema, table)
              + " ("
              + Stream.concat(
                      metadata.getPartitionKeyNames().stream().map(c -> enclose(c) + " ASC"),
                      metadata.getClusteringKeyNames().stream()
                          .map(c -> enclose(c) + " " + metadata.getClusteringOrder(c)))
                  .collect(Collectors.joining(","))
              + ")");
    }
    return sqls.toArray(new String[0]);
  }

  @Override
  DataType getDataTypeForScalarDbInternal(
      JDBCType type,
      String typeName,
      int columnSize,
      int digits,
      String columnDescription,
      @Nullable DataType overrideDataType) {
    switch (type) {
      case BOOLEAN:
        return DataType.BOOLEAN;
      case BIGINT:
        return DataType.BIGINT;
      case REAL:
        return DataType.FLOAT;
      case DOUBLE:
        return DataType.DOUBLE;
      case NVARCHAR:
        if (typeName.equalsIgnoreCase("jsonb")) {
          throw new IllegalArgumentException(
              CoreError.JDBC_IMPORT_DATA_TYPE_NOT_SUPPORTED.buildMessage(
                  typeName, columnDescription));
        }
        return DataType.TEXT;
      case BINARY:
        return DataType.BLOB;
      case DATE:
        return DataType.DATE;
      case TIMESTAMP:
        if (overrideDataType == DataType.TIME) {
          return DataType.TIME;
        }
        if (overrideDataType == DataType.TIMESTAMP) {
          return DataType.TIMESTAMP;
        }
        return DataType.TIMESTAMPTZ;
      default:
        throw new IllegalArgumentException(
            CoreError.JDBC_IMPORT_DATA_TYPE_NOT_SUPPORTED.buildMessage(
                typeName, columnDescription));
    }
  }

  @Override
  public UpsertQuery buildUpsertQuery(UpsertQuery.Builder builder) {
    return new InsertOnConflictDoUpdateExcludedQuery(builder);
  }

  @Override
  public DateColumn parseDateColumn(ResultSet resultSet, String columnName) throws SQLException {
    String dateStr = resultSet.getString(columnName);
    if (dateStr == null) {
      return DateColumn.ofNull(columnName);
    } else {
      LocalDate date = RdbEngineTimeTypeSpanner.DATE_FORMATTER.parse(dateStr, LocalDate::from);
      return DateColumn.of(columnName, date);
    }
  }

  @Override
  public String getEscape(LikeExpression likeExpression) {
    if (likeExpression.getEscape().isEmpty() || !likeExpression.getEscape().equals("\\")) {
      throw new UnsupportedOperationException(
          CoreError.JDBC_SPANNER_LIKE_ESCAPE_CHARACTER_NOT_SUPPORTED.buildMessage(
              likeExpression.getEscape()));
    }
    return likeExpression.getEscape();
  }

  @Override
  public void setConnectionCredentials(JdbcConfig config, HikariConfig connectionConfig) {
    if (config.getPassword().isPresent()) {
      try (ByteArrayInputStream keyStream =
          new ByteArrayInputStream(config.getPassword().get().getBytes(StandardCharsets.UTF_8))) {
        // Validate the credentials
        ServiceAccountCredentials.fromStream(keyStream);
      } catch (IOException e) {
        throw new IllegalArgumentException(
            CoreError.JDBC_SPANNER_SERVICE_ACCOUNT_KEY_LOAD_FAILED.buildMessage(), e);
      }
      String encodedCredentials =
          Base64.getEncoder()
              .encodeToString(config.getPassword().get().getBytes(StandardCharsets.UTF_8));
      // Setting this property is required to use encoded credentials authentication
      System.setProperty("ENABLE_ENCODED_CREDENTIALS", "true");
      connectionConfig.addDataSourceProperty("encodedcredentials", encodedCredentials);
    }
  }
}
