package com.scalar.db.storage.jdbc;

import static com.scalar.db.storage.jdbc.JdbcAdmin.execute;

import com.google.common.annotations.VisibleForTesting;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

@SuppressFBWarnings("OBL_UNSATISFIED_OBLIGATION")
public class NamespaceMetadataService {
  @VisibleForTesting public static final String TABLE_NAME = "namespaces";
  @VisibleForTesting static final String COL_NAMESPACE_NAME = "namespace_name";

  private final String metadataSchema;
  private final RdbEngineStrategy rdbEngine;

  NamespaceMetadataService(String metadataSchema, RdbEngineStrategy rdbEngine) {
    this.metadataSchema = metadataSchema;
    this.rdbEngine = rdbEngine;
  }

  void createNamespacesTableIfNotExists(Connection connection) throws SQLException {
    if (tableExistsInternal(connection, metadataSchema, TABLE_NAME)) {
      return;
    }

    createSchemaIfNotExists(connection, metadataSchema);
    String createTableStatement =
        "CREATE TABLE "
            + encloseFullTableName(metadataSchema, TABLE_NAME)
            + "("
            + enclose(COL_NAMESPACE_NAME)
            + " "
            + getTextType(128, true)
            + ", "
            + "PRIMARY KEY ("
            + enclose(COL_NAMESPACE_NAME)
            + "))";
    createTable(connection, createTableStatement, true);

    // Insert the system namespace to the namespaces table
    insertIntoNamespacesTable(connection, metadataSchema);
  }

  void deleteNamespacesTableIfEmpty(Connection connection) throws SQLException {
    if (isNamespacesTableEmpty(connection)) {
      deleteTable(connection, encloseFullTableName(metadataSchema, TABLE_NAME));
      deleteMetadataSchema(connection);
    }
  }

  private boolean isNamespacesTableEmpty(Connection connection) throws SQLException {
    String selectAllTables = "SELECT * FROM " + encloseFullTableName(metadataSchema, TABLE_NAME);

    Set<String> namespaces = new HashSet<>();
    try (Statement statement = connection.createStatement();
        ResultSet results = statement.executeQuery(selectAllTables)) {
      int count = 0;
      while (results.next()) {
        namespaces.add(results.getString(COL_NAMESPACE_NAME));
        // Only need to fetch the first two rows
        if (count++ == 2) {
          break;
        }
      }
    }

    return namespaces.size() == 1 && namespaces.contains(metadataSchema);
  }

  private void deleteMetadataSchema(Connection connection) throws SQLException {
    String sql = rdbEngine.deleteMetadataSchemaSql(metadataSchema);
    execute(connection, sql);
  }

  void insertIntoNamespacesTable(Connection connection, String namespaceName) throws SQLException {
    String insertStatement =
        "INSERT INTO " + encloseFullTableName(metadataSchema, TABLE_NAME) + " VALUES (?)";
    try (PreparedStatement preparedStatement = connection.prepareStatement(insertStatement)) {
      preparedStatement.setString(1, namespaceName);
      preparedStatement.execute();
    }
  }

  void upsertIntoNamespacesTable(Connection connection, String namespace) throws SQLException {
    try {
      insertIntoNamespacesTable(connection, namespace);
    } catch (SQLException e) {
      // ignore if the schema already exists
      if (!rdbEngine.isDuplicateKeyError(e)) {
        throw e;
      }
    }
  }

  void deleteFromNamespacesTable(Connection connection, String namespaceName) throws SQLException {
    String deleteStatement =
        "DELETE FROM "
            + encloseFullTableName(metadataSchema, TABLE_NAME)
            + " WHERE "
            + enclose(COL_NAMESPACE_NAME)
            + " = ?";
    try (PreparedStatement preparedStatement = connection.prepareStatement(deleteStatement)) {
      preparedStatement.setString(1, namespaceName);
      preparedStatement.execute();
    }
  }

  boolean namespaceExists(Connection connection, String namespace) throws SQLException {
    String selectQuery =
        "SELECT 1 FROM "
            + encloseFullTableName(metadataSchema, TABLE_NAME)
            + " WHERE "
            + enclose(COL_NAMESPACE_NAME)
            + " = ?";
    try {
      try (PreparedStatement statement = connection.prepareStatement(selectQuery)) {
        statement.setString(1, namespace);
        try (ResultSet resultSet = statement.executeQuery()) {
          return resultSet.next();
        }
      }
    } catch (SQLException e) {
      // An exception will be thrown if the namespaces table does not exist when executing the
      // select query
      if (rdbEngine.isUndefinedTableError(e)) {
        return false;
      }
      throw e;
    }
  }

  Set<String> getNamespaceNames(Connection connection) throws SQLException {
    try {
      String selectQuery = "SELECT * FROM " + encloseFullTableName(metadataSchema, TABLE_NAME);
      Set<String> namespaces = new HashSet<>();
      try (PreparedStatement preparedStatement = connection.prepareStatement(selectQuery);
          ResultSet resultSet = preparedStatement.executeQuery()) {
        while (resultSet.next()) {
          namespaces.add(resultSet.getString(COL_NAMESPACE_NAME));
        }
        return namespaces;
      }
    } catch (SQLException e) {
      // An exception will be thrown if the namespace table does not exist when executing the select
      // query
      if (rdbEngine.isUndefinedTableError(e)) {
        return Collections.emptySet();
      }
      throw e;
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

  private boolean tableExistsInternal(Connection connection, String namespace, String table)
      throws SQLException {
    String fullTableName = encloseFullTableName(namespace, table);
    String sql = rdbEngine.tableExistsInternalTableCheckSql(fullTableName);
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

  private void createSchemaIfNotExists(Connection connection, String schema) throws SQLException {
    String[] sqls = rdbEngine.createSchemaIfNotExistsSqls(schema);
    try {
      execute(connection, sqls);
    } catch (SQLException e) {
      // Suppress exceptions indicating the duplicate metadata schema
      if (!rdbEngine.isCreateMetadataSchemaDuplicateSchemaError(e)) {
        throw e;
      }
    }
  }

  private String enclose(String name) {
    return rdbEngine.enclose(name);
  }

  private String encloseFullTableName(String schema, String table) {
    return rdbEngine.encloseFullTableName(schema, table);
  }
}
