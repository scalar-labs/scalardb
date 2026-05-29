package com.scalar.db.storage.jdbc;

import static com.scalar.db.storage.jdbc.JdbcAdmin.execute;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.util.ThrowableConsumer;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.DisabledIf;
import org.junit.jupiter.api.condition.EnabledIf;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RdbEngineStrategyExceptionIntegrationTest {

  private static final String TEST_SCHEMA = "rdb_engine_exc_test";
  private static final String DUP_TABLE = "dup_table_test";
  private static final String DUP_KEY_TABLE = "dup_key_test";
  private static final String DUP_INDEX_TABLE = "dup_idx_test";
  private static final String UNDEF_INDEX_TABLE = "undef_idx_test";
  private static final String DUP_SCHEMA = "rdb_engine_dup_schema";
  private static final String INDEX_NAME = "rdb_engine_test_idx";
  private static final String INDEX_COL = "val";

  private RdbEngineStrategy rdbEngine;
  private HikariDataSource dataSource;
  private boolean requiresExplicitCommit;

  @BeforeAll
  public void setUpAll() throws SQLException {
    Properties properties = JdbcEnv.getProperties("rdb_engine_exc_test");
    JdbcConfig config = new JdbcConfig(new DatabaseConfig(properties));
    rdbEngine = RdbEngineFactory.create(config);
    dataSource = JdbcUtils.initDataSourceForAdmin(config, rdbEngine);
    requiresExplicitCommit = JdbcUtils.requiresExplicitCommit(dataSource, rdbEngine);
    createSchemaIfNotExists(TEST_SCHEMA);
  }

  @AfterAll
  public void tearDownAll() throws SQLException {
    executeSql(rdbEngine.dropNamespaceSql(TEST_SCHEMA));

    if (dataSource != null) {
      dataSource.close();
    }
  }

  @Test
  @DisabledIf("IsCreateTableIfNotExistsSyntaxSupported")
  public void isDuplicateTableError_WhenDuplicateTable_ShouldReturnTrue() throws SQLException {
    // Arrange: create the table
    String createSql =
        "CREATE TABLE "
            + rdbEngine.encloseFullTableName(TEST_SCHEMA, DUP_TABLE)
            + " ("
            + rdbEngine.enclose("id")
            + " INT NOT NULL, PRIMARY KEY ("
            + rdbEngine.enclose("id")
            + "))";
    assumeFalse(rdbEngine.tryAddIfNotExistsToCreateTableSql(createSql).equals(createSql));
    executeSql(createSql);
    try {
      // Act
      // Create the same table again to provoke the error
      SQLException caught = null;
      try {
        executeSql(createSql);
      } catch (SQLException e) {
        caught = e;
      }

      // Assert
      assertThat((Object) caught).isNotNull();
      assertThat(rdbEngine.isDuplicateTableError(caught)).isTrue();
    } finally {
      try {
        executeSql("DROP TABLE " + rdbEngine.encloseFullTableName(TEST_SCHEMA, DUP_TABLE));
      } catch (Exception e) {
        // Ignore
      }
    }
  }

  private boolean IsCreateTableIfNotExistsSyntaxSupported() {
    return JdbcTestUtils.isMysql(rdbEngine) || JdbcTestUtils.isPostgresql(rdbEngine);
  }

  @Test
  public void isDuplicateKeyError_WhenDuplicateKey_ShouldReturnTrue() throws SQLException {
    // Arrange: create a table with a primary key
    String createSql =
        "CREATE TABLE "
            + rdbEngine.encloseFullTableName(TEST_SCHEMA, DUP_KEY_TABLE)
            + " ("
            + rdbEngine.enclose("id")
            + " INT NOT NULL, PRIMARY KEY ("
            + rdbEngine.enclose("id")
            + "))";
    executeSql(createSql);
    try {
      String insertSql =
          "INSERT INTO "
              + rdbEngine.encloseFullTableName(TEST_SCHEMA, DUP_KEY_TABLE)
              + " ("
              + rdbEngine.enclose("id")
              + ") VALUES (1)";
      executeSql(insertSql);

      // Act
      // Insert the same key again
      SQLException caught = null;
      try {
        executeSql(insertSql);
      } catch (SQLException e) {
        caught = e;
      }

      // Assert
      assertThat((Object) caught).isNotNull();
      assertThat(rdbEngine.isDuplicateKeyError(caught)).isTrue();
    } finally {
      try {
        executeSql("DROP TABLE " + rdbEngine.encloseFullTableName(TEST_SCHEMA, DUP_KEY_TABLE));
      } catch (Exception e) {
        // Ignore
      }
    }
  }

  @Test
  public void isUndefinedTableError_WhenUndefinedTable_ShouldReturnTrue() {
    // Act
    // Query a non-existent table
    SQLException caught = null;
    try {
      executeSql(
          "SELECT 1 FROM "
              + rdbEngine.encloseFullTableName(TEST_SCHEMA, "nonexistent_table_xyz_abc"));
    } catch (SQLException e) {
      caught = e;
    }

    // Assert
    assertThat((Object) caught).isNotNull();
    assertThat(rdbEngine.isUndefinedTableError(caught)).isTrue();
  }

  @Test
  public void isUndefinedTableError_WhenUndefinedSchema_ShouldReturnTrue() {
    // Act
    // Query a non-existent table
    SQLException caught = null;
    try {
      executeSql(
          "SELECT 1 FROM "
              + rdbEngine.encloseFullTableName("nonexistent_schema", "nonexistent_table"));
    } catch (SQLException e) {
      caught = e;
    }

    // Assert
    assertThat((Object) caught).isNotNull();
    assertThat(rdbEngine.isUndefinedTableError(caught)).isTrue();
  }

  @Test
  @DisabledIf("isCreateSchemaIfNotExistsSyntaxSupported")
  public void isCreateMetadataSchemaDuplicateSchemaError_WhenDuplicateSchema_ShouldReturnTrue()
      throws SQLException {
    // Arrange: create the schema once
    executeSqls(rdbEngine.createSchemaSqls(DUP_SCHEMA));
    try {
      // Act: try to create it again
      SQLException caught = null;
      try {
        executeSqls(rdbEngine.createSchemaSqls(DUP_SCHEMA));
      } catch (SQLException e) {
        caught = e;
      }

      // Assert
      assertThat((Object) caught).isNotNull();
      assertThat(rdbEngine.isCreateMetadataSchemaDuplicateSchemaError(caught)).isTrue();
    } finally {
      try {
        executeSql(rdbEngine.dropNamespaceSql(DUP_SCHEMA));
      } catch (Exception e) {
        // Ignore
      }
    }
  }

  private boolean isCreateSchemaIfNotExistsSyntaxSupported() {
    return JdbcTestUtils.isMysql(rdbEngine)
        || JdbcTestUtils.isPostgresql(rdbEngine)
        || JdbcTestUtils.isSqlite(rdbEngine);
  }

  @Test
  @DisabledIf("isCreateIndexIfNotExistsSyntaxSupported")
  public void isDuplicateIndexError_WhenDuplicateIndex_ShouldReturnTrue() throws SQLException {
    // Arrange: create the table and an index
    String createTableSql =
        "CREATE TABLE "
            + rdbEngine.encloseFullTableName(TEST_SCHEMA, DUP_INDEX_TABLE)
            + " ("
            + rdbEngine.enclose("id")
            + " INT NOT NULL, "
            + rdbEngine.enclose(INDEX_COL)
            + " INT, PRIMARY KEY ("
            + rdbEngine.enclose("id")
            + "))";
    executeSql(createTableSql);
    try {
      String createIndexSql =
          rdbEngine.createIndexSql(TEST_SCHEMA, DUP_INDEX_TABLE, INDEX_NAME, INDEX_COL);
      executeSql(createIndexSql);

      // Act: create the same index again
      SQLException caught = null;
      try {
        executeSql(createIndexSql);
      } catch (SQLException e) {
        caught = e;
      }

      // Assert
      assertThat((Object) caught).isNotNull();
      assertThat(rdbEngine.isDuplicateIndexError(caught)).isTrue();
    } finally {
      try {
        executeSql("DROP TABLE " + rdbEngine.encloseFullTableName(TEST_SCHEMA, DUP_INDEX_TABLE));
      } catch (Exception e) {
        // Ignore
      }
    }
  }

  private boolean isCreateIndexIfNotExistsSyntaxSupported() {
    return JdbcTestUtils.isDb2(rdbEngine)
        || JdbcTestUtils.isSqlite(rdbEngine)
        || JdbcTestUtils.isPostgresql(rdbEngine);
  }

  @Test
  @EnabledIf("isDb2")
  public void throwIfDuplicatedIndexWarning_WhenDb2DuplicateIndex_ShouldThrow() throws Exception {
    assumeTrue(
        JdbcTestUtils.isDb2(rdbEngine), "Only DB2 has warning-based duplicate index detection");

    // Create table and index
    String fullTable = rdbEngine.encloseFullTableName(TEST_SCHEMA, DUP_INDEX_TABLE);
    String idCol = rdbEngine.enclose("id");
    String valCol = rdbEngine.enclose(INDEX_COL);
    executeSql(
        "CREATE TABLE "
            + fullTable
            + " ("
            + idCol
            + " INT NOT NULL, "
            + valCol
            + " INT, PRIMARY KEY ("
            + idCol
            + "))");
    try {
      String createIndexSql =
          rdbEngine.createIndexSql(TEST_SCHEMA, DUP_INDEX_TABLE, INDEX_NAME, INDEX_COL);
      executeSql(createIndexSql);

      // Execute duplicate CREATE INDEX -- DB2 raises warning, not exception
      AtomicReference<SQLException> thrown = new AtomicReference<>();
      JdbcAdmin.withConnection(
          dataSource,
          requiresExplicitCommit,
          (ThrowableConsumer<Connection, SQLException>)
              conn -> {
                try (Statement stmt = conn.createStatement()) {
                  stmt.execute(createIndexSql);
                  SQLWarning warning = stmt.getWarnings();
                  while (warning != null) {
                    try {
                      rdbEngine.throwIfDuplicatedIndexWarning(warning);
                    } catch (SQLException e) {
                      thrown.set(e);
                    }
                    warning = warning.getNextWarning();
                  }
                }
              });

      assertThat((Object) thrown.get())
          .as("Expected throwIfDuplicatedIndexWarning to throw for DB2 warning code 605")
          .isNotNull();
    } finally {
      try {
        executeSql("DROP TABLE " + rdbEngine.encloseFullTableName(TEST_SCHEMA, DUP_INDEX_TABLE));
      } catch (Exception e) {
        // Ignore
      }
    }
  }

  @Test
  public void isUndefinedIndexError_WhenUndefinedIndex_ShouldReturnTrue() throws SQLException {
    // Arrange: create a table without the index we'll try to drop
    String createTableSql =
        "CREATE TABLE "
            + rdbEngine.encloseFullTableName(TEST_SCHEMA, UNDEF_INDEX_TABLE)
            + " ("
            + rdbEngine.enclose("id")
            + " INT NOT NULL, "
            + rdbEngine.enclose(INDEX_COL)
            + " INT, PRIMARY KEY ("
            + rdbEngine.enclose("id")
            + "))";
    executeSql(createTableSql);
    try {
      // Act: drop a non-existent index
      SQLException caught = null;
      try {
        executeSql(
            rdbEngine.dropIndexSql(TEST_SCHEMA, UNDEF_INDEX_TABLE, "nonexistent_index_xyz_abc"));
      } catch (SQLException e) {
        caught = e;
      }

      // Assert
      assertThat((Object) caught).isNotNull();
      assertThat(rdbEngine.isUndefinedIndexError(caught)).isTrue();
    } finally {
      try {
        executeSql("DROP TABLE " + rdbEngine.encloseFullTableName(TEST_SCHEMA, UNDEF_INDEX_TABLE));
      } catch (Exception e) {
        // Ignore
      }
    }
  }



  private void executeSql(String sql) throws SQLException {
    JdbcAdmin.withConnection(
        dataSource,
        requiresExplicitCommit,
        (ThrowableConsumer<Connection, SQLException>)
            conn -> execute(conn, sql, requiresExplicitCommit));
  }

  private void executeSqls(String[] sqls) throws SQLException {
    JdbcAdmin.withConnection(
        dataSource,
        requiresExplicitCommit,
        (ThrowableConsumer<Connection, SQLException>)
            conn -> {
              for (String sql : sqls) {
                execute(conn, sql, requiresExplicitCommit);
              }
            });
  }

  private void createSchemaIfNotExists(String schema) throws SQLException {
    String[] sqls = rdbEngine.createSchemaIfNotExistsSqls(schema);
    try {
      try (Connection connection = dataSource.getConnection(); ) {
        execute(connection, sqls, requiresExplicitCommit);
      }

    } catch (SQLException e) {
      // Suppress exceptions indicating the duplicate metadata schema
      if (!rdbEngine.isCreateMetadataSchemaDuplicateSchemaError(e)) {
        throw e;
      }
    }
  }

  private boolean isDb2() {
    return JdbcTestUtils.isDb2(rdbEngine);
  }
}
