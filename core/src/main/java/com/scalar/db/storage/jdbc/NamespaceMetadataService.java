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
  private final String createdTextCollationClause;
  private final boolean requiresExplicitCommit;

  NamespaceMetadataService(
      String metadataSchema,
      RdbEngineStrategy rdbEngine,
      String createdTextCollationClause,
      boolean requiresExplicitCommit) {
    this.metadataSchema = metadataSchema;
    this.rdbEngine = rdbEngine;
    this.createdTextCollationClause = createdTextCollationClause;
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
    JdbcAdmin.createTable(
        connection,
        rdbEngine,
        createTableStatement,
        true,
        metadataSchema,
        TABLE_NAME,
        requiresExplicitCommit);

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
            rdbEngine,
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
        connection,
        rdbEngine,
        insertStatement,
        requiresExplicitCommit,
        ps -> ps.setString(1, namespaceName));
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
        connection,
        rdbEngine,
        deleteStatement,
        requiresExplicitCommit,
        ps -> ps.setString(1, namespaceName));
  }

  boolean namespaceExists(Connection connection, String namespace) throws SQLException {
    if (!internalTableExists(connection, metadataSchema, TABLE_NAME)) {
      return false;
    }

    String selectQuery =
        "SELECT 1 FROM "
            + encloseFullTableName(metadataSchema, TABLE_NAME)
            + " WHERE "
            + enclose(COL_NAMESPACE_NAME)
            + " = ?";
    return executeQuery(
        connection,
        rdbEngine,
        selectQuery,
        requiresExplicitCommit,
        ps -> ps.setString(1, namespace),
        ResultSet::next);
  }

  Set<String> getNamespaceNames(Connection connection) throws SQLException {
    if (!internalTableExists(connection, metadataSchema, TABLE_NAME)) {
      return Collections.emptySet();
    }

    String selectQuery = "SELECT * FROM " + encloseFullTableName(metadataSchema, TABLE_NAME);
    return executeQuery(
        connection,
        rdbEngine,
        selectQuery,
        requiresExplicitCommit,
        rs -> {
          Set<String> namespaces = new HashSet<>();
          while (rs.next()) {
            namespaces.add(rs.getString(COL_NAMESPACE_NAME));
          }
          return namespaces;
        });
  }

  private String getTextType(int charLength, boolean isKey) {
    return rdbEngine.appendCollationClause(
        rdbEngine.getTextType(charLength, isKey), createdTextCollationClause);
  }

  private boolean internalTableExists(Connection connection, String namespace, String table)
      throws SQLException {
    return JdbcAdmin.internalTableExists(
        connection, rdbEngine, namespace, table, requiresExplicitCommit);
  }

  private void deleteTable(Connection connection, String fullTableName) throws SQLException {
    String dropTableStatement = "DROP TABLE " + fullTableName;
    execute(connection, rdbEngine, dropTableStatement, requiresExplicitCommit);
  }

  private String enclose(String name) {
    return rdbEngine.enclose(name);
  }

  private String encloseFullTableName(String schema, String table) {
    return rdbEngine.encloseFullTableName(schema, table);
  }
}
