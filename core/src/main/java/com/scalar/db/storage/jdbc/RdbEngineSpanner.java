package com.scalar.db.storage.jdbc;

import static com.scalar.db.storage.jdbc.JdbcUtils.shortenIndexNameIfNeeded;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.common.annotations.VisibleForTesting;
import com.google.rpc.Code;
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
import java.util.List;
import javax.annotation.Nullable;

class RdbEngineSpanner extends RdbEnginePostgresql {

  private final RdbEngineTimeTypeSpanner timeTypeEngine;

  RdbEngineSpanner(JdbcConfig config) {
    timeTypeEngine = new RdbEngineTimeTypeSpanner(config);
  }

  @VisibleForTesting
  RdbEngineSpanner() {
    timeTypeEngine = null;
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
        return "bigint"; // Spanner has not INT type coded on 4 bytes. INT type is an alias for a
        // bigint coded on 8 bytes.
      case TIME:
      case TIMESTAMP:
        return "timestamptz";
      default:
        return super.getDataTypeForEngine(scalarDbDataType);
    }
  }

  @Override
  public String getDataTypeForKey(DataType dataType) {
    // No specific handling for key data type is required
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
    // Since the "IF NOT EXISTS" syntax is used to create a table, we always return false
    return false;
  }

  @Override
  public boolean isCreateMetadataSchemaDuplicateSchemaError(SQLException e) {
    // Since the "IF NOT EXISTS" syntax is used to create a schema, we always return false
    return false;
  }

  @Override
  public boolean isDuplicateKeyError(SQLException e) {
    return e.getErrorCode() == Code.ALREADY_EXISTS_VALUE;
  }

  @Override
  public boolean isUndefinedTableError(SQLException e) {
    return e.getErrorCode() == Code.INVALID_ARGUMENT_VALUE;
  }

  @Override
  public boolean isUndefinedIndexError(SQLException e) {
    return e.getErrorCode() == Code.NOT_FOUND_VALUE;
  }

  @Override
  public boolean isConflict(SQLException e) {
    return e.getErrorCode() == Code.ABORTED_VALUE;
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
  public RdbEngineTimeTypeStrategy<LocalDate, OffsetDateTime, OffsetDateTime, OffsetDateTime>
      getTimeTypeStrategy() {
    return timeTypeEngine;
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

  @Override
  public String[] dropTableSql(TableMetadata metadata, String schema, String table) {
    List<String> sqls = new ArrayList<>();
    // Index needs to be explicitly dropped before dropping the table
    for (String index : metadata.getSecondaryIndexNames()) {
      String indexName = JdbcAdmin.getIndexName(schema, table, index);
      sqls.add(dropIndexSql(schema, table, indexName));
    }
    if (JdbcAdmin.hasDifferentClusteringOrders(metadata)) {
      String indexName =
          shortenIndexNameIfNeeded(
              CLUSTERING_ORDER_INDEX_NAME_PREFIX + schema + "_" + table,
              CLUSTERING_ORDER_INDEX_NAME_PREFIX);
      sqls.add(dropIndexSql(schema, table, indexName));
    }
    sqls.add("DROP TABLE " + encloseFullTableName(schema, table));

    return sqls.toArray(new String[0]);
  }

  @Override
  public boolean requiresExplicitDropIndexBeforeDropColumn() {
    return true;
  }
}
