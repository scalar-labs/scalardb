package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.service.TransactionFactory;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/**
 * Runs ScalarDB against a live SAP ASE. Unlike the rest of the JDBC integration tests, this one is
 * self-contained: it makes its own properties and cleans up after itself, so it can be pointed at
 * an ASE without the surrounding harness.
 *
 * <pre>
 * ./gradlew integrationTestJdbc --tests "*AseSmokeTest" \
 *     -PjconnectJar=/path/to/jConnect40.jar \
 *     -Dscalardb.jdbc.url=jdbc:sybase:Tds:localhost:5557/scalardb \
 *     -Dscalardb.jdbc.username=sa -Dscalardb.jdbc.password=sybase \
 *     -Dscalardb.ase.namespace=n1
 * </pre>
 *
 * <p>The namespace and the system namespace must already exist as ASE users, since the engine never
 * creates one. See docs/sap-ase-poc.md.
 */
public class AseSmokeTest {

  private static final String TABLE = "smoke";
  private static final String LEGACY_TABLE = "legacy";

  private final String namespace = System.getProperty("scalardb.ase.namespace", "n1");
  private final String jdbcUrl =
      System.getProperty("scalardb.jdbc.url", "jdbc:sybase:Tds:localhost:5557/scalardb");
  private final String username = System.getProperty("scalardb.jdbc.username", "sa");
  private final String password = System.getProperty("scalardb.jdbc.password", "sybase");

  private Properties properties() {
    Properties properties = new Properties();
    properties.setProperty(DatabaseConfig.STORAGE, "jdbc");
    properties.setProperty(DatabaseConfig.CONTACT_POINTS, jdbcUrl);
    properties.setProperty(DatabaseConfig.USERNAME, username);
    properties.setProperty(DatabaseConfig.PASSWORD, password);
    // Must name an existing ASE user
    properties.setProperty(DatabaseConfig.SYSTEM_NAMESPACE_NAME, "scalardb");
    properties.setProperty(DatabaseConfig.CROSS_PARTITION_SCAN, "true");
    properties.setProperty(JdbcConfig.CONNECTION_POOL_MIN_IDLE, "0");
    properties.setProperty(JdbcConfig.TABLE_METADATA_CONNECTION_POOL_MIN_IDLE, "0");
    properties.setProperty(JdbcConfig.ADMIN_CONNECTION_POOL_MIN_IDLE, "0");
    return properties;
  }

  private static final TableMetadata METADATA =
      TableMetadata.newBuilder()
          .addColumn("pk", DataType.TEXT)
          .addColumn("ck", DataType.INT)
          .addColumn("text_col", DataType.TEXT)
          .addColumn("bool_col", DataType.BOOLEAN)
          .addColumn("ts_col", DataType.TIMESTAMP)
          .addColumn("indexed_col", DataType.TEXT)
          .addPartitionKey("pk")
          .addClusteringKey("ck")
          .addSecondaryIndex("indexed_col")
          .build();

  @Test
  public void scalarDbRunsOnSapAse() throws Exception {
    Properties properties = properties();

    try (DistributedStorageAdmin admin = StorageFactory.create(properties).getStorageAdmin();
        DistributedStorage storage = StorageFactory.create(properties).getStorage()) {

      dropIfPresent(admin);

      // 1. DDL: the namespace check, CREATE TABLE with LOCK DATAROWS, and a secondary index
      admin.createNamespace(namespace, true);
      admin.createTable(namespace, TABLE, METADATA, true);
      assertThat(admin.tableExists(namespace, TABLE)).isTrue();
      assertThat(admin.getTableMetadata(namespace, TABLE)).isEqualTo(METADATA);
      System.out.println("[ase] created " + namespace + "." + TABLE + " with a secondary index");

      // 2. Upsert through MERGE, then read back
      storage.put(
          Put.newBuilder()
              .namespace(namespace)
              .table(TABLE)
              .partitionKey(Key.ofText("pk", "p1"))
              .clusteringKey(Key.ofInt("ck", 1))
              .textValue("text_col", "hello ase")
              .booleanValue("bool_col", true)
              .textValue("indexed_col", "idx1")
              .build());

      Optional<Result> result =
          storage.get(
              Get.newBuilder()
                  .namespace(namespace)
                  .table(TABLE)
                  .partitionKey(Key.ofText("pk", "p1"))
                  .clusteringKey(Key.ofInt("ck", 1))
                  .build());
      assertThat(result).isPresent();
      assertThat(result.get().getText("text_col")).isEqualTo("hello ase");
      assertThat(result.get().getBoolean("bool_col")).isTrue();
      System.out.println("[ase] put and get round-tripped, including BOOLEAN in a tinyint");

      // The same key again: MERGE has to update rather than fail on the duplicate key
      storage.put(
          Put.newBuilder()
              .namespace(namespace)
              .table(TABLE)
              .partitionKey(Key.ofText("pk", "p1"))
              .clusteringKey(Key.ofInt("ck", 1))
              .textValue("text_col", "hello again")
              .build());
      assertThat(
              storage
                  .get(
                      Get.newBuilder()
                          .namespace(namespace)
                          .table(TABLE)
                          .partitionKey(Key.ofText("pk", "p1"))
                          .clusteringKey(Key.ofInt("ck", 1))
                          .build())
                  .get()
                  .getText("text_col"))
          .isEqualTo("hello again");
      System.out.println("[ase] a second put on the same key merged instead of failing");

      // 3. Scan, which exercises SELECT with the clustering key ordering
      storage.put(
          Put.newBuilder()
              .namespace(namespace)
              .table(TABLE)
              .partitionKey(Key.ofText("pk", "p1"))
              .clusteringKey(Key.ofInt("ck", 2))
              .textValue("text_col", "second")
              .build());
      try (Scanner scanner =
          storage.scan(
              Scan.newBuilder()
                  .namespace(namespace)
                  .table(TABLE)
                  .partitionKey(Key.ofText("pk", "p1"))
                  .build())) {
        List<Result> results = scanner.all();
        assertThat(results).hasSize(2);
        System.out.println("[ase] scan returned " + results.size() + " rows");
      }

      // 4. The secondary index lifecycle, which is where the index name quoting bites
      admin.dropIndex(namespace, TABLE, "indexed_col");
      admin.createIndex(namespace, TABLE, "indexed_col", Collections.emptyMap());
      assertThat(admin.indexExists(namespace, TABLE, "indexed_col")).isTrue();
      System.out.println("[ase] dropped and recreated the secondary index");

      // 5. Delete
      storage.delete(
          Delete.newBuilder()
              .namespace(namespace)
              .table(TABLE)
              .partitionKey(Key.ofText("pk", "p1"))
              .clusteringKey(Key.ofInt("ck", 2))
              .build());
      System.out.println("[ase] delete applied");
    }

    // 5. A Consensus Commit transaction over the same storage
    Properties transactionProperties = properties();
    transactionProperties.setProperty(DatabaseConfig.TRANSACTION_MANAGER, "consensus-commit");
    try (DistributedTransactionAdmin transactionAdmin =
            TransactionFactory.create(transactionProperties).getTransactionAdmin();
        DistributedTransactionManager manager =
            TransactionFactory.create(transactionProperties).getTransactionManager()) {

      transactionAdmin.createNamespace(namespace, true);
      transactionAdmin.createTable(namespace, TABLE + "_tx", METADATA, true);
      transactionAdmin.createCoordinatorTables(true);

      DistributedTransaction transaction = manager.start();
      transaction.put(
          Put.newBuilder()
              .namespace(namespace)
              .table(TABLE + "_tx")
              .partitionKey(Key.ofText("pk", "tx1"))
              .clusteringKey(Key.ofInt("ck", 1))
              .textValue("text_col", "committed")
              .build());
      transaction.commit();

      DistributedTransaction reader = manager.start();
      Optional<Result> committed =
          reader.get(
              Get.newBuilder()
                  .namespace(namespace)
                  .table(TABLE + "_tx")
                  .partitionKey(Key.ofText("pk", "tx1"))
                  .clusteringKey(Key.ofInt("ck", 1))
                  .build());
      reader.commit();
      assertThat(committed).isPresent();
      assertThat(committed.get().getText("text_col")).isEqualTo("committed");
      System.out.println("[ase] a Consensus Commit transaction committed and was read back");

      transactionAdmin.dropTable(namespace, TABLE + "_tx", true);
      transactionAdmin.dropCoordinatorTables(true);
    }

    // 6. Import: put ScalarDB in front of a table that already exists
    createLegacyTable();
    try (DistributedStorageAdmin admin = StorageFactory.create(properties).getStorageAdmin();
        DistributedStorage storage = StorageFactory.create(properties).getStorage()) {
      admin.importTable(namespace, LEGACY_TABLE, Collections.emptyMap(), Collections.emptyMap());
      TableMetadata imported = admin.getTableMetadata(namespace, LEGACY_TABLE);
      assertThat(imported).isNotNull();
      System.out.println("[ase] imported the existing table as " + imported.getColumnNames());

      Optional<Result> legacyRow =
          storage.get(
              Get.newBuilder()
                  .namespace(namespace)
                  .table(LEGACY_TABLE)
                  .partitionKey(Key.ofInt("id", 1))
                  .build());
      assertThat(legacyRow).isPresent();
      assertThat(legacyRow.get().getText("name")).isEqualTo("existing row");
      System.out.println("[ase] read a pre-existing row through ScalarDB");

      admin.dropTable(namespace, LEGACY_TABLE, true);
      admin.dropTable(namespace, TABLE, true);
    }
  }

  private void dropIfPresent(DistributedStorageAdmin admin) throws Exception {
    for (String table : new String[] {TABLE, TABLE + "_tx", LEGACY_TABLE}) {
      if (admin.namespaceExists(namespace) && admin.tableExists(namespace, table)) {
        admin.dropTable(namespace, table);
      }
    }
  }

  /** Creates a table the way an existing application would have, with no ScalarDB involvement. */
  private void createLegacyTable() throws Exception {
    try (Connection connection = connect();
        Statement statement = connection.createStatement()) {
      statement.execute("SET QUOTED_IDENTIFIER ON");
      try {
        statement.execute("DROP TABLE " + namespace + "." + LEGACY_TABLE);
      } catch (SQLException e) {
        // not there
      }
      statement.execute(
          "CREATE TABLE "
              + namespace
              + "."
              + LEGACY_TABLE
              + " (id INT NOT NULL, name VARCHAR(64) NULL, created BIGDATETIME NULL,"
              + " PRIMARY KEY (id)) LOCK DATAROWS");
      statement.execute(
          "INSERT INTO "
              + namespace
              + "."
              + LEGACY_TABLE
              + " (id, name, created) VALUES (1, 'existing row', '20200102 03:04:05.123456')");
    }
  }

  private Connection connect() throws Exception {
    Driver driver =
        (Driver)
            Class.forName("com.sybase.jdbc4.jdbc.SybDriver").getDeclaredConstructor().newInstance();
    Properties credentials = new Properties();
    credentials.setProperty("user", username);
    credentials.setProperty("password", password);
    return driver.connect(jdbcUrl, credentials);
  }
}
