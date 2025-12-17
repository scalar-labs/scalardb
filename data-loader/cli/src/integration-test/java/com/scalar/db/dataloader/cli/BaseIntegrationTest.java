package com.scalar.db.dataloader.cli;

import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.service.StorageFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.MountableFile;

/**
 * Base class for integration tests that provides shared MySQL container setup and configuration.
 *
 * <p>This class eliminates code duplication across test classes by providing:
 *
 * <ul>
 *   <li>Shared MySQL container instance
 *   <li>Config file creation helper
 *   <li>Test constants (database names, credentials, etc.)
 *   <li>Test isolation support
 * </ul>
 */
public abstract class BaseIntegrationTest {

  // Database configuration constants
  protected static final String MYSQL_IMAGE = "mysql:8.0";
  protected static final String DATABASE_NAME = "test";
  protected static final String USERNAME = "root";
  protected static final String PASSWORD = "12345678";
  protected static final String NAMESPACE = "test";

  // Table name constants
  protected static final String TABLE_EMPLOYEE = "employee";
  protected static final String TABLE_EMPLOYEE_TRN = "employee_trn";
  protected static final String TABLE_ALL_COLUMNS = "all_columns";
  protected static final String TABLE_EMP_DEPARTMENT = "emp_department";

  // Shared MySQL container - initialized once for all tests
  protected static MySQLContainer<?> mysql;

  // Temporary directory for test files (created per test method)
  @TempDir protected Path tempDir;

  // Config file path (created in setup)
  protected Path configFilePath;

  /**
   * Returns the name of the initialization SQL file to use. Subclasses can override to use a
   * different init file. This must be called before the container starts.
   *
   * @return the name of the init SQL file (default: "init_mysql_import.sql")
   */
  protected static String getInitSqlFileName() {
    return "init_mysql_import.sql";
  }

  /**
   * Starts the MySQL container before all tests. This method is called once per test class.
   *
   * <p>The container is configured with:
   *
   * <ul>
   *   <li>Database initialization script from classpath (default: "init_mysql_import.sql")
   *   <li>Database name, username, and password from constants
   * </ul>
   *
   * <p>Subclasses can override {@link #getInitSqlFileName()} to use a different init file, but must
   * do so before this method is called.
   */
  @BeforeAll
  static void startContainers() {
    if (mysql != null && mysql.isRunning()) {
      return; // Container already started by another test class
    }

    mysql =
        new MySQLContainer<>(MYSQL_IMAGE)
            .withDatabaseName(DATABASE_NAME)
            .withUsername(USERNAME)
            .withPassword(PASSWORD);

    // Use init_mysql.sql which has both tables and data
    // This ensures export tests have data to export
    // Import tests can clean up data as needed
    mysql.withCopyFileToContainer(
        MountableFile.forClasspathResource("init_mysql.sql"),
        "/docker-entrypoint-initdb.d/init_mysql.sql");

    mysql.start();
  }

  /**
   * Stops the MySQL container after all tests are complete. This method is called once per test
   * class.
   */
  @AfterAll
  static void stopContainers() {
    if (mysql != null && mysql.isRunning()) {
      mysql.stop();
    }
  }

  /**
   * Sets up test-specific resources before each test method. Creates the ScalarDB config file in
   * the temporary directory.
   *
   * <p>Subclasses can override this method to add additional setup, but should call {@code
   * super.setup()} first.
   *
   * @throws Exception if setup fails
   */
  @BeforeEach
  void setup() throws Exception {
    // Create ScalarDB config file
    configFilePath = tempDir.resolve("scalardb.properties");
    String configContent = getScalarDbConfig();
    Files.write(configFilePath, configContent.getBytes(StandardCharsets.UTF_8));

    // Clean up tables before each test to ensure clean state
    // This ensures import tests start with empty tables
    if (shouldCleanupTables()) {
      cleanupTablesInternal();
    }
  }

  // Transaction manager constants
  protected static final String TRANSACTION_MANAGER_CONSENSUS_COMMIT = "consensus-commit";
  protected static final String TRANSACTION_MANAGER_SINGLE_CRUD = "single-crud-operation";
  protected static final String TRANSACTION_MANAGER_JDBC = "jdbc";

  // System namespace constant
  protected static final String SYSTEM_NAMESPACE = "scalardb";

  /**
   * Returns the transaction manager type for test configuration. Subclasses can override to use a
   * different transaction manager (e.g., "single-crud-operation" for storage-only tests).
   *
   * <p>Valid values:
   *
   * <ul>
   *   <li>{@code jdbc} - Native JDBC transactions (default for JDBC storage)
   *   <li>{@code consensus-commit} - Full transactional support with ACID guarantees
   *   <li>{@code single-crud-operation} - Storage-only mode without transactions
   * </ul>
   *
   * <p>Note: When using JDBC storage, use "jdbc" transaction manager for proper metadata access.
   * The "consensus-commit" manager requires additional coordinator table setup.
   *
   * @return the transaction manager type (default: "jdbc" for JDBC storage compatibility)
   */
  protected String getTransactionManagerType() {
    return TRANSACTION_MANAGER_JDBC;
  }

  /**
   * Generates the ScalarDB configuration content as a string.
   *
   * <p>The configuration includes:
   *
   * <ul>
   *   <li>Storage type (jdbc)
   *   <li>Database connection details
   *   <li>Transaction manager type
   *   <li>System namespace for metadata tables
   *   <li>Cross-partition scan support
   * </ul>
   *
   * @return ScalarDB configuration properties as a string
   */
  protected String getScalarDbConfig() {
    return "scalar.db.storage=jdbc\n"
        + "scalar.db.contact_points="
        + mysql.getJdbcUrl()
        + "\n"
        + "scalar.db.username="
        + USERNAME
        + "\n"
        + "scalar.db.password="
        + PASSWORD
        + "\n"
        + "scalar.db.transaction_manager="
        + getTransactionManagerType()
        + "\n"
        + "scalar.db.system_namespace_name="
        + SYSTEM_NAMESPACE
        + "\n"
        + "scalar.db.cross_partition_scan.enabled=true\n";
  }

  /**
   * Determines whether tables should be cleaned up after each test. Export tests may need to
   * preserve data, so they can override this method to return false.
   *
   * @return true if tables should be cleaned up, false otherwise
   */
  protected boolean shouldCleanupTables() {
    return true;
  }

  /**
   * Internal method to perform table cleanup. Called both before tests (to ensure clean state) and
   * after tests (for isolation).
   *
   * @throws Exception if cleanup fails
   */
  private void cleanupTablesInternal() throws Exception {
    if (configFilePath == null || !Files.exists(configFilePath)) {
      return; // Skip cleanup if config file doesn't exist
    }

    try {
      Properties props = new Properties();
      props.load(Files.newInputStream(configFilePath));
      StorageFactory factory = StorageFactory.create(props);
      DistributedStorageAdmin admin = factory.getStorageAdmin();

      // Truncate common test tables to ensure test isolation
      // Catch and ignore exceptions - tables might not exist or cleanup might fail
      truncateTableSafely(admin, NAMESPACE, TABLE_EMPLOYEE);
      truncateTableSafely(admin, NAMESPACE, TABLE_EMPLOYEE_TRN);
      truncateTableSafely(admin, NAMESPACE, TABLE_ALL_COLUMNS);
      truncateTableSafely(admin, NAMESPACE, TABLE_EMP_DEPARTMENT);

      admin.close();
    } catch (Exception e) {
      // Log but don't fail tests if cleanup fails
      // This is expected if tables don't exist or admin can't be created
    }
  }

  /**
   * Cleans up test data from tables after each test to ensure test isolation. This method is called
   * automatically after each test method via {@code @AfterEach}.
   *
   * <p>By default, this method truncates common test tables. Subclasses can override {@link
   * #shouldCleanupTables()} to skip cleanup if needed (e.g., export tests that need data).
   *
   * @throws Exception if cleanup fails
   */
  @AfterEach
  void cleanupTables() throws Exception {
    if (!shouldCleanupTables()) {
      return; // Skip cleanup for tests that need to preserve data
    }
    cleanupTablesInternal();
  }

  /**
   * Safely truncates a table, ignoring any exceptions. Used during test cleanup.
   *
   * @param admin the storage admin instance
   * @param namespace the namespace name
   * @param table the table name
   */
  private void truncateTableSafely(DistributedStorageAdmin admin, String namespace, String table) {
    try {
      admin.truncateTable(namespace, table);
    } catch (ExecutionException e) {
      // Table might not exist, already empty, or other error - ignore during cleanup
      // This is expected and should not fail tests
    }
  }
}
