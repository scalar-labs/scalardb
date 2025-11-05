package com.scalar.db.storage.jdbc;

import static com.scalar.db.storage.jdbc.JdbcAdmin.execute;
import static com.scalar.db.util.ScalarDbUtils.getFullTableName;

import com.scalar.db.api.VirtualTableInfo;
import com.scalar.db.api.VirtualTableJoinType;
import com.scalar.db.io.DataType;
import edu.umd.cs.findbugs.annotations.Nullable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@SuppressFBWarnings("OBL_UNSATISFIED_OBLIGATION")
public class VirtualTableMetadataService {
  private static final String TABLE_NAME = "virtual_tables";
  private static final String COL_FULL_TABLE_NAME = "full_table_name";
  private static final String COL_LEFT_SOURCE_TABLE_FULL_TABLE_NAME =
      "left_source_table_full_table_name";
  private static final String COL_RIGHT_SOURCE_TABLE_FULL_TABLE_NAME =
      "right_source_table_full_table_name";
  private static final String COL_JOIN_TYPE = "join_type";
  private static final String COL_ATTRIBUTES = "attributes";

  private final String metadataSchema;
  private final RdbEngineStrategy rdbEngine;

  VirtualTableMetadataService(String metadataSchema, RdbEngineStrategy rdbEngine) {
    this.metadataSchema = metadataSchema;
    this.rdbEngine = rdbEngine;
  }

  void createVirtualTablesTableIfNotExists(Connection connection) throws SQLException {
    String createTableStatement =
        "CREATE TABLE "
            + encloseFullTableName(metadataSchema, TABLE_NAME)
            + "("
            + enclose(COL_FULL_TABLE_NAME)
            + " "
            + getTextType(128, true)
            + ", "
            + enclose(COL_LEFT_SOURCE_TABLE_FULL_TABLE_NAME)
            + " "
            + getTextType(128, true)
            + ", "
            + enclose(COL_RIGHT_SOURCE_TABLE_FULL_TABLE_NAME)
            + " "
            + getTextType(128, true)
            + ", "
            + enclose(COL_JOIN_TYPE)
            + " "
            + getTextType(20, false)
            + ", "
            + enclose(COL_ATTRIBUTES)
            + " "
            + rdbEngine.getDataTypeForEngine(DataType.TEXT)
            + ", "
            + "PRIMARY KEY ("
            + enclose(COL_FULL_TABLE_NAME)
            + "))";
    createTable(connection, createTableStatement, true);
  }

  void deleteVirtualTablesTableIfEmpty(Connection connection) throws SQLException {
    if (isVirtualTablesTableEmpty(connection)) {
      deleteTable(connection, encloseFullTableName(metadataSchema, TABLE_NAME));
    }
  }

  private boolean isVirtualTablesTableEmpty(Connection connection) throws SQLException {
    String selectAllTables = "SELECT * FROM " + encloseFullTableName(metadataSchema, TABLE_NAME);
    try (Statement statement = connection.createStatement();
        ResultSet results = statement.executeQuery(selectAllTables)) {
      return !results.next();
    }
  }

  void insertIntoVirtualTablesTable(
      Connection connection,
      String namespace,
      String table,
      String leftSourceNamespace,
      String leftSourceTable,
      String rightSourceNamespace,
      String rightSourceTable,
      VirtualTableJoinType joinType,
      String attributes)
      throws SQLException {
    String insertStatement =
        "INSERT INTO " + encloseFullTableName(metadataSchema, TABLE_NAME) + " VALUES (?,?,?,?,?)";
    try (PreparedStatement preparedStatement = connection.prepareStatement(insertStatement)) {
      preparedStatement.setString(1, getFullTableName(namespace, table));
      preparedStatement.setString(2, getFullTableName(leftSourceNamespace, leftSourceTable));
      preparedStatement.setString(3, getFullTableName(rightSourceNamespace, rightSourceTable));
      preparedStatement.setString(4, joinType.name());
      preparedStatement.setString(5, attributes);
      preparedStatement.execute();
    }
  }

  void deleteFromVirtualTablesTable(Connection connection, String namespace, String table)
      throws SQLException {
    String deleteStatement =
        "DELETE FROM "
            + encloseFullTableName(metadataSchema, TABLE_NAME)
            + " WHERE "
            + enclose(COL_FULL_TABLE_NAME)
            + " = ?";
    try (PreparedStatement preparedStatement = connection.prepareStatement(deleteStatement)) {
      preparedStatement.setString(1, getFullTableName(namespace, table));
      preparedStatement.execute();
    }
  }

  @Nullable
  VirtualTableInfo getVirtualTableInfo(Connection connection, String namespace, String table)
      throws SQLException {
    if (!internalTableExists(connection, metadataSchema, TABLE_NAME)) {
      return null;
    }

    String selectStatement =
        "SELECT * FROM "
            + encloseFullTableName(metadataSchema, TABLE_NAME)
            + " WHERE "
            + enclose(COL_FULL_TABLE_NAME)
            + " = ?";
    try (PreparedStatement preparedStatement = connection.prepareStatement(selectStatement)) {
      preparedStatement.setString(1, getFullTableName(namespace, table));
      try (ResultSet results = preparedStatement.executeQuery()) {
        if (results.next()) {
          return createVirtualTableInfoFromResultSet(results);
        }

        return null;
      }
    }
  }

  List<VirtualTableInfo> getVirtualTableInfosBySourceTable(
      Connection connection, String sourceNamespace, String sourceTable) throws SQLException {
    if (!internalTableExists(connection, metadataSchema, TABLE_NAME)) {
      return new ArrayList<>();
    }

    String selectStatement =
        "SELECT * FROM "
            + encloseFullTableName(metadataSchema, TABLE_NAME)
            + " WHERE "
            + enclose(COL_LEFT_SOURCE_TABLE_FULL_TABLE_NAME)
            + " = ? OR "
            + enclose(COL_RIGHT_SOURCE_TABLE_FULL_TABLE_NAME)
            + " = ?";
    try (PreparedStatement preparedStatement = connection.prepareStatement(selectStatement)) {
      String sourceTableFullTableName = getFullTableName(sourceNamespace, sourceTable);
      preparedStatement.setString(1, sourceTableFullTableName);
      preparedStatement.setString(2, sourceTableFullTableName);
      try (ResultSet results = preparedStatement.executeQuery()) {
        List<VirtualTableInfo> ret = new ArrayList<>();
        while (results.next()) {
          ret.add(createVirtualTableInfoFromResultSet(results));
        }
        return ret;
      }
    }
  }

  private VirtualTableInfo createVirtualTableInfoFromResultSet(ResultSet results)
      throws SQLException {
    String fullTableName = results.getString(COL_FULL_TABLE_NAME);
    String namespace = fullTableName.substring(0, fullTableName.indexOf('.'));
    String table = fullTableName.substring(fullTableName.indexOf('.') + 1);
    String leftSourceTableFullTableName = results.getString(COL_LEFT_SOURCE_TABLE_FULL_TABLE_NAME);
    String rightSourceTableFullTableName =
        results.getString(COL_RIGHT_SOURCE_TABLE_FULL_TABLE_NAME);
    VirtualTableJoinType joinType = VirtualTableJoinType.valueOf(results.getString(COL_JOIN_TYPE));

    String leftSourceNamespace =
        leftSourceTableFullTableName.substring(0, leftSourceTableFullTableName.indexOf('.'));
    String leftSourceTable =
        leftSourceTableFullTableName.substring(leftSourceTableFullTableName.indexOf('.') + 1);
    String rightSourceNamespace =
        rightSourceTableFullTableName.substring(0, rightSourceTableFullTableName.indexOf('.'));
    String rightSourceTable =
        rightSourceTableFullTableName.substring(rightSourceTableFullTableName.indexOf('.') + 1);

    return new VirtualTableInfo() {
      @Override
      public String getNamespaceName() {
        return namespace;
      }

      @Override
      public String getTableName() {
        return table;
      }

      @Override
      public String getLeftSourceNamespaceName() {
        return leftSourceNamespace;
      }

      @Override
      public String getLeftSourceTableName() {
        return leftSourceTable;
      }

      @Override
      public String getRightSourceNamespaceName() {
        return rightSourceNamespace;
      }

      @Override
      public String getRightSourceTableName() {
        return rightSourceTable;
      }

      @Override
      public VirtualTableJoinType getJoinType() {
        return joinType;
      }
    };
  }

  Set<String> getNamespaceTableNames(Connection connection, String namespace) throws SQLException {
    if (!internalTableExists(connection, metadataSchema, TABLE_NAME)) {
      return Collections.emptySet();
    }

    String selectTablesOfNamespaceStatement =
        "SELECT "
            + enclose(COL_FULL_TABLE_NAME)
            + " FROM "
            + encloseFullTableName(metadataSchema, TABLE_NAME)
            + " WHERE "
            + enclose(COL_FULL_TABLE_NAME)
            + " LIKE ?";
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
  }

  Set<String> getNamespaceNamesOfExistingTables(Connection connection) throws SQLException {
    if (!internalTableExists(connection, metadataSchema, TABLE_NAME)) {
      return Collections.emptySet();
    }

    String selectAllTableNames =
        "SELECT "
            + enclose(COL_FULL_TABLE_NAME)
            + " FROM "
            + encloseFullTableName(metadataSchema, TABLE_NAME);
    try (Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(selectAllTableNames)) {
      Set<String> namespaceOfExistingTables = new HashSet<>();
      while (rs.next()) {
        String fullTableName = rs.getString(COL_FULL_TABLE_NAME);
        String namespaceName = fullTableName.substring(0, fullTableName.indexOf('.'));
        namespaceOfExistingTables.add(namespaceName);
      }
      return namespaceOfExistingTables;
    }
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

  private boolean internalTableExists(Connection connection, String namespace, String table)
      throws SQLException {
    String fullTableName = encloseFullTableName(namespace, table);
    String sql = rdbEngine.internalTableExistsCheckSql(fullTableName);
    try {
      execute(connection, sql);
      return true;
    } catch (SQLException e) {
      // An exception will be thrown if the table does not exist when executing the select
      // query
      if (rdbEngine.isUndefinedTableError(e)) {
        return false;
      }
      throw e;
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
