package com.scalar.db.storage.jdbc;

import com.scalar.db.api.LikeExpression;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.io.DateColumn;
import com.scalar.db.io.TimeColumn;
import com.scalar.db.io.TimestampColumn;
import com.scalar.db.io.TimestampTZColumn;
import com.scalar.db.storage.jdbc.query.SelectQuery;
import com.scalar.db.storage.jdbc.query.UpsertQuery;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * An interface to hide the difference between underlying JDBC SQL engines in SQL dialects, error
 * codes, and so on. It's NOT responsible for actually connecting to underlying engines.
 */
public interface RdbEngineStrategy {

  boolean isDuplicateTableError(SQLException e);

  boolean isDuplicateKeyError(SQLException e);

  boolean isUndefinedTableError(SQLException e);

  boolean isConflict(SQLException e);

  String getDataTypeForEngine(DataType dataType);

  @Nullable
  String getDataTypeForKey(DataType dataType);

  @Nullable
  default String getDataTypeForSecondaryIndex(DataType dataType) {
    return getDataTypeForKey(dataType);
  }

  DataType getDataTypeForScalarDb(
      JDBCType type,
      String typeName,
      int columnSize,
      int digits,
      String columnDescription,
      @Nullable DataType overrideDataType);

  int getSqlTypes(DataType dataType);

  String getTextType(int charLength, boolean isKey);

  String computeBooleanValue(boolean value);

  String[] createNamespaceSqls(String fullNamespace);

  default boolean isValidTableName(String tableName) {
    return true;
  }

  String createTableInternalPrimaryKeyClause(
      boolean hasDescClusteringOrder, TableMetadata metadata);

  String[] createTableInternalSqlsAfterCreateTable(
      boolean hasDifferentClusteringOrders, String schema, String table, TableMetadata metadata);

  String tryAddIfNotExistsToCreateTableSql(String createTableSql);

  String[] createMetadataSchemaIfNotExistsSql(String metadataSchema);

  boolean isCreateMetadataSchemaDuplicateSchemaError(SQLException e);

  String deleteMetadataSchemaSql(String metadataSchema);

  String dropNamespaceSql(String namespace);

  default String truncateTableSql(String namespace, String table) {
    return "TRUNCATE TABLE " + encloseFullTableName(namespace, table);
  }

  void dropNamespaceTranslateSQLException(SQLException e, String namespace)
      throws ExecutionException;

  String namespaceExistsStatement();

  default String namespaceExistsPlaceholder(String namespace) {
    return namespace;
  }

  String alterColumnTypeSql(String namespace, String table, String columnName, String columnType);

  String tableExistsInternalTableCheckSql(String fullTableName);

  default String createIndexSql(
      String schema, String table, String indexName, String indexedColumn) {
    return "CREATE INDEX "
        + enclose(indexName)
        + " ON "
        + encloseFullTableName(schema, table)
        + " ("
        + enclose(indexedColumn)
        + ")";
  }

  String dropIndexSql(String schema, String table, String indexName);

  /**
   * Enclose the target (schema, table or column) to use reserved words and special characters.
   *
   * @param name The target name to enclose
   * @return An enclosed string of the target name
   */
  String enclose(String name);

  default String encloseFullTableName(String schema, String table) {
    return enclose(schema) + "." + enclose(table);
  }

  SelectQuery buildSelectQuery(SelectQuery.Builder builder, int limit);

  UpsertQuery buildUpsertQuery(UpsertQuery.Builder builder);

  Driver getDriver();

  default boolean isImportable() {
    return true;
  }

  /**
   * Return properly-preprocessed like pattern for each underlying database.
   *
   * @param likeExpression A like conditional expression
   * @return The properly-preprocessed like pattern
   */
  default String getPattern(LikeExpression likeExpression) {
    return likeExpression.getTextValue();
  }

  /**
   * Return properly-preprocessed escape character for each underlying database. Return null if the
   * escape clause must be excluded.
   *
   * @param likeExpression A like conditional expression
   * @return The properly-preprocessed escape character
   */
  default @Nullable String getEscape(LikeExpression likeExpression) {
    return likeExpression.getEscape();
  }

  default @Nullable String getCatalogName(String namespace) {
    return null;
  }

  default @Nullable String getSchemaName(String namespace) {
    return namespace;
  }

  default LocalDate encode(DateColumn column) {
    assert column.getDateValue() != null;
    return column.getDateValue();
  }

  default LocalTime encode(TimeColumn column) {
    assert column.getTimeValue() != null;
    return column.getTimeValue();
  }

  default LocalDateTime encode(TimestampColumn column) {
    assert column.getTimestampValue() != null;
    return column.getTimestampValue();
  }

  default OffsetDateTime encode(TimestampTZColumn column) {
    assert column.getTimestampTZValue() != null;
    return column.getTimestampTZValue().atOffset(ZoneOffset.UTC);
  }

  default DateColumn parseDateColumn(ResultSet resultSet, String columnName) throws SQLException {
    return DateColumn.of(columnName, resultSet.getObject(columnName, LocalDate.class));
  }

  default TimeColumn parseTimeColumn(ResultSet resultSet, String columnName) throws SQLException {
    return TimeColumn.of(columnName, resultSet.getObject(columnName, LocalTime.class));
  }

  default TimestampColumn parseTimestampColumn(ResultSet resultSet, String columnName)
      throws SQLException {
    return TimestampColumn.of(columnName, resultSet.getObject(columnName, LocalDateTime.class));
  }

  default TimestampTZColumn parseTimestampTZColumn(ResultSet resultSet, String columnName)
      throws SQLException {
    OffsetDateTime offsetDateTime = resultSet.getObject(columnName, OffsetDateTime.class);
    if (offsetDateTime == null) {
      return TimestampTZColumn.ofNull(columnName);
    } else {
      return TimestampTZColumn.of(columnName, offsetDateTime.toInstant());
    }
  }

  /**
   * Return the connection properties for the underlying database.
   *
   * @param config the JDBC configuration
   * @return a map where key=property_name and value=property_value
   */
  default Map<String, String> getConnectionProperties(JdbcConfig config) {
    return Collections.emptyMap();
  }

  RdbEngineTimeTypeStrategy<?, ?, ?, ?> getTimeTypeStrategy();

  default String getProjectionsSqlForSelectQuery(TableMetadata metadata, List<String> projections) {
    if (projections.isEmpty()) {
      return "*";
    }
    return projections.stream().map(this::enclose).collect(Collectors.joining(","));
  }

  /**
   * Throws an exception if the given SQLWarning is a duplicate index warning.
   *
   * @param warning the SQLWarning to check
   * @throws SQLException if the warning is a duplicate index warning
   */
  default void throwIfDuplicatedIndexWarning(SQLWarning warning) throws SQLException {
    // Do nothing
  }

  default void setConnectionToReadOnly(Connection connection, boolean readOnly)
      throws SQLException {
    connection.setReadOnly(readOnly);
  }

  String getTableNamesInNamespaceSql();
}
