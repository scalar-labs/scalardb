package com.scalar.db.storage.jdbc;

import static com.scalar.db.storage.jdbc.JdbcAdmin.execute;
import static com.scalar.db.util.ScalarDbUtils.getFullTableName;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import javax.annotation.Nullable;

@SuppressFBWarnings({"OBL_UNSATISFIED_OBLIGATION", "SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE"})
public class TableMetadataService {
  @VisibleForTesting public static final String TABLE_NAME = "metadata";
  @VisibleForTesting public static final String COL_FULL_TABLE_NAME = "full_table_name";
  @VisibleForTesting static final String COL_COLUMN_NAME = "column_name";
  @VisibleForTesting static final String COL_DATA_TYPE = "data_type";
  @VisibleForTesting static final String COL_KEY_TYPE = "key_type";
  @VisibleForTesting static final String COL_CLUSTERING_ORDER = "clustering_order";
  @VisibleForTesting static final String COL_INDEXED = "indexed";
  @VisibleForTesting static final String COL_ORDINAL_POSITION = "ordinal_position";

  private final String metadataSchema;
  private final RdbEngineStrategy rdbEngine;

  TableMetadataService(String metadataSchema, RdbEngineStrategy rdbEngine) {
    this.metadataSchema = metadataSchema;
    this.rdbEngine = rdbEngine;
  }

  void addTableMetadata(
      Connection connection,
      String namespace,
      String table,
      TableMetadata metadata,
      boolean createMetadataTable,
      boolean overwriteMetadata)
      throws SQLException {
    if (createMetadataTable) {
      createTableMetadataTableIfNotExists(connection);
    }
    if (overwriteMetadata) {
      // Delete the metadata for the table before we add them
      execute(connection, getDeleteTableMetadataStatement(namespace, table));
    }
    LinkedHashSet<String> orderedColumns = new LinkedHashSet<>(metadata.getPartitionKeyNames());
    orderedColumns.addAll(metadata.getClusteringKeyNames());
    orderedColumns.addAll(metadata.getColumnNames());
    int ordinalPosition = 1;
    for (String column : orderedColumns) {
      insertMetadataColumn(namespace, table, metadata, connection, ordinalPosition++, column);
    }
  }

  @VisibleForTesting
  void createTableMetadataTableIfNotExists(Connection connection) throws SQLException {
    String createTableStatement =
        "CREATE TABLE "
            + encloseFullTableName(metadataSchema, TABLE_NAME)
            + "("
            + enclose(COL_FULL_TABLE_NAME)
            + " "
            + getTextType(128, true)
            + ","
            + enclose(COL_COLUMN_NAME)
            + " "
            + getTextType(128, true)
            + ","
            + enclose(COL_DATA_TYPE)
            + " "
            + getTextType(20, false)
            + " NOT NULL,"
            + enclose(COL_KEY_TYPE)
            + " "
            + getTextType(20, false)
            + ","
            + enclose(COL_CLUSTERING_ORDER)
            + " "
            + getTextType(10, false)
            + ","
            + enclose(COL_INDEXED)
            + " "
            + getBooleanType()
            + " NOT NULL,"
            + enclose(COL_ORDINAL_POSITION)
            + " INTEGER NOT NULL,"
            + "PRIMARY KEY ("
            + enclose(COL_FULL_TABLE_NAME)
            + ", "
            + enclose(COL_COLUMN_NAME)
            + "))";

    createTable(connection, createTableStatement, true);
  }

  private void insertMetadataColumn(
      String schema,
      String table,
      TableMetadata metadata,
      Connection connection,
      int ordinalPosition,
      String column)
      throws SQLException {
    KeyType keyType = null;
    if (metadata.getPartitionKeyNames().contains(column)) {
      keyType = KeyType.PARTITION;
    }
    if (metadata.getClusteringKeyNames().contains(column)) {
      keyType = KeyType.CLUSTERING;
    }

    String insertStatement =
        getInsertStatement(
            schema,
            table,
            column,
            metadata.getColumnDataType(column),
            keyType,
            metadata.getClusteringOrder(column),
            metadata.getSecondaryIndexNames().contains(column),
            ordinalPosition);
    execute(connection, insertStatement);
  }

  private String getInsertStatement(
      String schema,
      String table,
      String columnName,
      DataType dataType,
      @Nullable KeyType keyType,
      @Nullable Scan.Ordering.Order ckOrder,
      boolean indexed,
      int ordinalPosition) {

    return String.format(
        "INSERT INTO %s VALUES ('%s','%s','%s',%s,%s,%s,%d)",
        encloseFullTableName(metadataSchema, TABLE_NAME),
        getFullTableName(schema, table),
        columnName,
        dataType.toString(),
        keyType != null ? "'" + keyType + "'" : "NULL",
        ckOrder != null ? "'" + ckOrder + "'" : "NULL",
        computeBooleanValue(indexed),
        ordinalPosition);
  }

  void deleteTableMetadata(
      Connection connection, String namespace, String table, boolean deleteMetadataTableIfEmpty)
      throws SQLException {
    try {
      execute(connection, getDeleteTableMetadataStatement(namespace, table));
      if (deleteMetadataTableIfEmpty) {
        deleteMetadataTableIfEmpty(connection);
      }
    } catch (SQLException e) {
      if (e.getMessage().contains("Unknown table") || e.getMessage().contains("does not exist")) {
        return;
      }
      throw e;
    }
  }

  private String getDeleteTableMetadataStatement(String schema, String table) {
    return "DELETE FROM "
        + encloseFullTableName(metadataSchema, TABLE_NAME)
        + " WHERE "
        + enclose(COL_FULL_TABLE_NAME)
        + " = '"
        + getFullTableName(schema, table)
        + "'";
  }

  private void deleteMetadataTableIfEmpty(Connection connection) throws SQLException {
    if (isMetadataTableEmpty(connection)) {
      deleteTable(connection, encloseFullTableName(metadataSchema, TABLE_NAME));
    }
  }

  private boolean isMetadataTableEmpty(Connection connection) throws SQLException {
    String selectAllTables =
        "SELECT DISTINCT "
            + enclose(COL_FULL_TABLE_NAME)
            + " FROM "
            + encloseFullTableName(metadataSchema, TABLE_NAME);
    try (Statement statement = connection.createStatement();
        ResultSet results = statement.executeQuery(selectAllTables)) {
      return !results.next();
    }
  }

  TableMetadata getTableMetadata(Connection connection, String namespace, String table)
      throws SQLException {
    TableMetadata.Builder builder = TableMetadata.newBuilder();
    boolean tableExists = false;

    try {
      try (PreparedStatement preparedStatement =
          connection.prepareStatement(getSelectColumnsStatement())) {
        preparedStatement.setString(1, getFullTableName(namespace, table));

        try (ResultSet resultSet = preparedStatement.executeQuery()) {
          while (resultSet.next()) {
            tableExists = true;

            String columnName = resultSet.getString(COL_COLUMN_NAME);
            DataType dataType = DataType.valueOf(resultSet.getString(COL_DATA_TYPE));
            builder.addColumn(columnName, dataType);

            boolean indexed = resultSet.getBoolean(COL_INDEXED);
            if (indexed) {
              builder.addSecondaryIndex(columnName);
            }

            String keyType = resultSet.getString(COL_KEY_TYPE);
            if (keyType == null) {
              continue;
            }

            switch (KeyType.valueOf(keyType)) {
              case PARTITION:
                builder.addPartitionKey(columnName);
                break;
              case CLUSTERING:
                Scan.Ordering.Order clusteringOrder =
                    Scan.Ordering.Order.valueOf(resultSet.getString(COL_CLUSTERING_ORDER));
                builder.addClusteringKey(columnName, clusteringOrder);
                break;
              default:
                throw new AssertionError("Invalid key type: " + keyType);
            }
          }
        }
      }
    } catch (SQLException e) {
      // An exception will be thrown if the namespace table does not exist when executing the select
      // query
      if (rdbEngine.isUndefinedTableError(e)) {
        return null;
      }
      throw e;
    }

    if (!tableExists) {
      return null;
    }

    return builder.build();
  }

  private String getSelectColumnsStatement() {
    return "SELECT "
        + enclose(COL_COLUMN_NAME)
        + ","
        + enclose(COL_DATA_TYPE)
        + ","
        + enclose(COL_KEY_TYPE)
        + ","
        + enclose(COL_CLUSTERING_ORDER)
        + ","
        + enclose(COL_INDEXED)
        + " FROM "
        + encloseFullTableName(metadataSchema, TABLE_NAME)
        + " WHERE "
        + enclose(COL_FULL_TABLE_NAME)
        + "=? ORDER BY "
        + enclose(COL_ORDINAL_POSITION)
        + " ASC";
  }

  void updateTableMetadata(
      Connection connection, String schema, String table, String columnName, boolean indexed)
      throws SQLException {
    String updateStatement =
        "UPDATE "
            + encloseFullTableName(metadataSchema, TABLE_NAME)
            + " SET "
            + enclose(COL_INDEXED)
            + "="
            + computeBooleanValue(indexed)
            + " WHERE "
            + enclose(COL_FULL_TABLE_NAME)
            + "='"
            + getFullTableName(schema, table)
            + "' AND "
            + enclose(COL_COLUMN_NAME)
            + "='"
            + columnName
            + "'";
    execute(connection, updateStatement);
  }

  Set<String> getNamespaceTableNames(Connection connection, String namespace) throws SQLException {
    String selectTablesOfNamespaceStatement =
        "SELECT DISTINCT "
            + enclose(COL_FULL_TABLE_NAME)
            + " FROM "
            + encloseFullTableName(metadataSchema, TABLE_NAME)
            + " WHERE "
            + enclose(COL_FULL_TABLE_NAME)
            + " LIKE ?";
    try {
      try (PreparedStatement preparedStatement =
          connection.prepareStatement(selectTablesOfNamespaceStatement)) {
        String prefix = namespace + ".";
        preparedStatement.setString(1, prefix + "%");
        try (ResultSet results = preparedStatement.executeQuery()) {
          Set<String> tableNames = new HashSet<>();
          while (results.next()) {
            String tableName = results.getString(COL_FULL_TABLE_NAME).substring(prefix.length());
            tableNames.add(tableName);
          }
          return tableNames;
        }
      }

    } catch (SQLException e) {
      // An exception will be thrown if the metadata table does not exist when executing the select
      // query
      if (rdbEngine.isUndefinedTableError(e)) {
        return Collections.emptySet();
      }
      throw e;
    }
  }

  Set<String> getNamespaceNames(Connection connection) throws SQLException {
    String selectAllTableNames =
        "SELECT DISTINCT "
            + enclose(COL_FULL_TABLE_NAME)
            + " FROM "
            + encloseFullTableName(metadataSchema, TABLE_NAME);
    try {
      try (Statement stmt = connection.createStatement();
          ResultSet rs = stmt.executeQuery(selectAllTableNames)) {
        Set<String> namespaceOfExistingTables = new HashSet<>();
        while (rs.next()) {
          String fullTableName = rs.getString(COL_FULL_TABLE_NAME);
          String namespaceName = fullTableName.substring(0, fullTableName.indexOf('.'));
          namespaceOfExistingTables.add(namespaceName);
        }
        namespaceOfExistingTables.add(metadataSchema);

        return namespaceOfExistingTables;
      }
    } catch (SQLException e) {
      // An exception will be thrown if the namespace table does not exist when executing the select
      // query
      if (rdbEngine.isUndefinedTableError(e)) {
        return Collections.singleton(metadataSchema);
      }
      throw e;
    }
  }

  private String computeBooleanValue(boolean value) {
    return rdbEngine.computeBooleanValue(value);
  }

  private String getBooleanType() {
    return rdbEngine.getDataTypeForEngine(DataType.BOOLEAN);
  }

  private String getTextType(int charLength, boolean isKey) {
    return rdbEngine.getTextType(charLength, isKey);
  }

  private void createTable(Connection connection, String createTableStatement, boolean ifNotExists)
      throws SQLException {
    String stmt = createTableStatement;
    if (ifNotExists) {
      stmt = rdbEngine.tryAddIfNotExistsToCreateTableSql(createTableStatement);
    }
    try {
      execute(connection, stmt);
    } catch (SQLException e) {
      // Suppress the exception thrown when the table already exists
      if (!(ifNotExists && rdbEngine.isDuplicateTableError(e))) {
        throw e;
      }
    }
  }

  private void deleteTable(Connection connection, String fullTableName) throws SQLException {
    String dropTableStatement = "DROP TABLE " + fullTableName;

    execute(connection, dropTableStatement);
  }

  private String enclose(String name) {
    return rdbEngine.enclose(name);
  }

  private String encloseFullTableName(String schema, String table) {
    return rdbEngine.encloseFullTableName(schema, table);
  }
}
