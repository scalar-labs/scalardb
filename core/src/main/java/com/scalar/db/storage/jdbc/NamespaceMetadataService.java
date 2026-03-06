package com.scalar.db.storage.jdbc;

import static com.scalar.db.storage.jdbc.JdbcAdmin.execute;
import static com.scalar.db.storage.jdbc.JdbcAdmin.executeQuery;
import static com.scalar.db.storage.jdbc.JdbcAdmin.executeUpdate;

import com.google.common.annotations.VisibleForTesting;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

@SuppressFBWarnings("OBL_UNSATISFIED_OBLIGATION")
public class NamespaceMetadataService {
  @VisibleForTesting public static final String TABLE_NAME = "namespaces";
  @VisibleForTesting static final String COL_NAMESPACE_NAME = "namespace_name";

  private final String metadataSchema;
  private final RdbEngineStrategy rdbEngine;
  private final boolean requiresExplicitCommit;

  NamespaceMetadataService(
      String metadataSchema, RdbEngineStrategy rdbEngine, boolean requiresExplicitCommit) {
    this.metadataSchema = metadataSchema;
    this.rdbEngine = rdbEngine;
    this.requiresExplicitCommit = requiresExplicitCommit;
  }

  void createNamespacesTableIfNotExists(Connection connection) throws SQLException {
    if (internalTableExists(connection, metadataSchema, TABLE_NAME)) {
      return;
    }

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
    }
  }

  private boolean isNamespacesTableEmpty(Connection connection) throws SQLException {
    String selectAllTables = "SELECT * FROM " + encloseFullTableName(metadataSchema, TABLE_NAME);

    Set<String> namespaces =
        executeQuery(
            connection,
            selectAllTables,
            requiresExplicitCommit,
            rs -> {
              Set<String> result = new HashSet<>();
              int count = 0;
              while (rs.next()) {
                result.add(rs.getString(COL_NAMESPACE_NAME));
                // Only need to fetch the first two rows
                if (count++ == 2) {
                  break;
                }
              }
              return result;
            });

    return namespaces.size() == 1 && namespaces.contains(metadataSchema);
  }

  void insertIntoNamespacesTable(Connection connection, String namespaceName) throws SQLException {
    String insertStatement =
        "INSERT INTO " + encloseFullTableName(metadataSchema, TABLE_NAME) + " VALUES (?)";
    executeUpdate(
        connection, insertStatement, requiresExplicitCommit, ps -> ps.setString(1, namespaceName));
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
    executeUpdate(
        connection, deleteStatement, requiresExplicitCommit, ps -> ps.setString(1, namespaceName));
  }

  boolean namespaceExists(Connection connection, String namespace) throws SQLException {
    String selectQuery =
        "SELECT 1 FROM "
            + encloseFullTableName(metadataSchema, TABLE_NAME)
            + " WHERE "
            + enclose(COL_NAMESPACE_NAME)
            + " = ?";
    try {
      return executeQuery(
          connection,
          selectQuery,
          requiresExplicitCommit,
          ps -> ps.setString(1, namespace),
          ResultSet::next);
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
      return executeQuery(
          connection,
          selectQuery,
          requiresExplicitCommit,
          rs -> {
            Set<String> namespaces = new HashSet<>();
            while (rs.next()) {
              namespaces.add(rs.getString(COL_NAMESPACE_NAME));
            }
            return namespaces;
          });
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
      execute(connection, stmt, requiresExplicitCommit);
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
      execute(connection, sql, requiresExplicitCommit);
      return true;
    } catch (SQLException e) {
      // An exception will be thrown if the table does not exist when executing the select query
      if (rdbEngine.isUndefinedTableError(e)) {
        return false;
      }
      throw e;
    }
  }

  private void deleteTable(Connection connection, String fullTableName) throws SQLException {
    String dropTableStatement = "DROP TABLE " + fullTableName;
    execute(connection, dropTableStatement, requiresExplicitCommit);
  }

  private String enclose(String name) {
    return rdbEngine.enclose(name);
  }

  private String encloseFullTableName(String schema, String table) {
    return rdbEngine.encloseFullTableName(schema, table);
  }
}
