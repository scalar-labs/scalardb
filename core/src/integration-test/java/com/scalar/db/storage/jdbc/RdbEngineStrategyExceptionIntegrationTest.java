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
  private static final String CONFLICT_TABLE = "conflict_test";
  private static final String DUP_SCHEMA = "rdb_engine_dup_schema";
  private static final String INDEX_NAME = "rdb_engine_test_idx";
  private static final String INDEX_COL = "val";

  private RdbEngineStrategy rdbEngine;
  private HikariDataSource dataSource;
  private boolean requiresExplicitCommit;
  private String jdbcUrl;

  @BeforeAll
  public void setUpAll() throws SQLException {
    Properties properties = JdbcEnv.getProperties("rdb_engine_exc_test");
    JdbcConfig config = new JdbcConfig(new DatabaseConfig(properties));
    rdbEngine = RdbEngineFactory.create(config);
    dataSource = JdbcUtils.initDataSourceForAdmin(config, rdbEngine);
    requiresExplicitCommit = JdbcUtils.requiresExplicitCommit(dataSource, rdbEngine);
    jdbcUrl = properties.getProperty(DatabaseConfig.CONTACT_POINTS);
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

  @Test
  public void isConflict_WhenDeadlock_ShouldReturnTrue() throws Exception {
    // Create conflict table with two rows
    String fullTable = rdbEngine.encloseFullTableName(TEST_SCHEMA, CONFLICT_TABLE);
    String idCol = rdbEngine.enclose("id");
    String valCol = rdbEngine.enclose("val");
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
      executeSql("INSERT INTO " + fullTable + " (" + idCol + ", " + valCol + ") VALUES (1, 0)");
      executeSql("INSERT INTO " + fullTable + " (" + idCol + ", " + valCol + ") VALUES (2, 0)");

      // Two-thread deadlock
      CountDownLatch thread1HoldsLock = new CountDownLatch(1);
      CountDownLatch thread2HoldsLock = new CountDownLatch(1);
      AtomicReference<SQLException> conflictException = new AtomicReference<>();

      String updateRow1 = "UPDATE " + fullTable + " SET " + valCol + " = 1 WHERE " + idCol + " = 1";
      String updateRow2 = "UPDATE " + fullTable + " SET " + valCol + " = 1 WHERE " + idCol + " = 2";

      // For SQLite, use DriverManager with busy_timeout=0 so the second writer
      // gets SQLITE_BUSY immediately instead of waiting (the HikariDataSource
      // is configured with busy_timeout=50000 which causes a 50s hang).
      boolean isSqlite = JdbcTestUtils.isSqlite(rdbEngine);

      if (isSqlite) {
        String sqliteNoBusyUrl = jdbcUrl.replaceAll("busy_timeout=\\d+", "busy_timeout=0");
        // SQLite uses file-level locking, not row-level. The first write transaction
        // locks the entire file. With busy_timeout=0, the second writer gets SQLITE_BUSY
        // immediately. No traditional deadlock is possible — just contention.
        try (Connection conn1 = DriverManager.getConnection(sqliteNoBusyUrl)) {
          conn1.setAutoCommit(false);
          try (Statement stmt1 = conn1.createStatement()) {
            stmt1.execute(updateRow1); // Acquires RESERVED lock on the file
            // Now try a second connection while conn1 holds the lock
            try (Connection conn2 = DriverManager.getConnection(sqliteNoBusyUrl)) {
              conn2.setAutoCommit(false);
              try (Statement stmt2 = conn2.createStatement()) {
                stmt2.execute(updateRow2); // Should get SQLITE_BUSY
              }
            } catch (SQLException e) {
              conflictException.set(e);
            }
          } finally {
            conn1.rollback();
          }
        }
      } else {
        // Standard row-level deadlock: two threads lock rows in opposite order
        Thread thread1 =
            new Thread(
                () -> {
                  try (Connection conn = dataSource.getConnection()) {
                    conn.setAutoCommit(false);
                    // TODO changing isolation level might trigger other errors
                    conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
                    try (Statement stmt = conn.createStatement()) {
                      stmt.execute(updateRow1); // Lock row 1
                      thread1HoldsLock.countDown();
                      if (!thread2HoldsLock.await(30, TimeUnit.SECONDS)) {
                        conn.rollback();
                        return;
                      }
                      stmt.execute(updateRow2); // Try to lock row 2 -> blocked
                      conn.commit();
                    } catch (SQLException e) {
                      conflictException.compareAndSet(null, e);
                      try {
                        conn.rollback();
                      } catch (SQLException rollbackEx) {
                        // ignore rollback error
                      }
                    }
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                  } catch (Exception e) {
                    if (e instanceof SQLException) {
                      conflictException.compareAndSet(null, (SQLException) e);
                    }
                  }
                });

        Thread thread2 =
            new Thread(
                () -> {
                  try (Connection conn = dataSource.getConnection()) {
                    conn.setAutoCommit(false);
                    // TODO changing isolation level might trigger other errors
                    conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
                    try (Statement stmt = conn.createStatement()) {
                      if (!thread1HoldsLock.await(30, TimeUnit.SECONDS)) {
                        conn.rollback();
                        return;
                      }
                      stmt.execute(updateRow2); // Lock row 2
                      thread2HoldsLock.countDown();
                      stmt.execute(updateRow1); // Try to lock row 1 -> DEADLOCK
                      conn.commit();
                    } catch (SQLException e) {
                      conflictException.compareAndSet(null, e);
                      try {
                        conn.rollback();
                      } catch (SQLException rollbackEx) {
                        // ignore rollback error
                      }
                    }
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                  } catch (Exception e) {
                    if (e instanceof SQLException) {
                      conflictException.compareAndSet(null, (SQLException) e);
                    }
                  }
                });

        thread1.start();
        thread2.start();
        thread1.join(60_000);
        thread2.join(60_000);
      }

      assertThat((Object) conflictException.get())
          .as("Expected a deadlock/conflict exception but none was thrown")
          .isNotNull();
      assertThat(rdbEngine.isConflict(conflictException.get())).isTrue();
    } finally {
      try {
        executeSql("DROP TABLE " + rdbEngine.encloseFullTableName(TEST_SCHEMA, CONFLICT_TABLE));
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
