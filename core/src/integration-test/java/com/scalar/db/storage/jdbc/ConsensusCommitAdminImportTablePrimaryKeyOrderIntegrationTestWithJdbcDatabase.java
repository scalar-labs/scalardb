package com.scalar.db.storage.jdbc;

import static com.scalar.db.storage.jdbc.JdbcImportPrimaryKeyOrderTestUtils.COL;
import static com.scalar.db.storage.jdbc.JdbcImportPrimaryKeyOrderTestUtils.PK_A;
import static com.scalar.db.storage.jdbc.JdbcImportPrimaryKeyOrderTestUtils.PK_B;
import static com.scalar.db.storage.jdbc.JdbcImportPrimaryKeyOrderTestUtils.PK_C;
import static com.scalar.db.storage.jdbc.JdbcImportPrimaryKeyOrderTestUtils.PRIMARY_KEY_ORDER;
import static com.scalar.db.storage.jdbc.JdbcImportPrimaryKeyOrderTestUtils.TABLE;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Get;
import com.scalar.db.api.Insert;
import com.scalar.db.api.Result;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.io.Key;
import com.scalar.db.service.TransactionFactory;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Optional;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.DisabledIf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DisabledIf("isSqlite")
class ConsensusCommitAdminImportTablePrimaryKeyOrderIntegrationTestWithJdbcDatabase {
  private static final Logger logger =
      LoggerFactory.getLogger(
          ConsensusCommitAdminImportTablePrimaryKeyOrderIntegrationTestWithJdbcDatabase.class);

  private static final String TEST_NAME = "tx_import_pk_order";
  private static final String NAMESPACE = "int_test_" + TEST_NAME;
  private static final String IMPORTED_TABLE = TABLE + "_scalardb";
  private static final String TRANSACTION_METADATA_TABLE = TABLE + "_tx_metadata";
  private static final ImmutableMap<String, String> CREATION_OPTIONS =
      ImmutableMap.of("transaction-metadata-decoupling", "true");

  private JdbcImportPrimaryKeyOrderTestUtils testUtils;
  private DistributedTransactionAdmin admin;
  private DistributedTransactionManager manager;

  @BeforeAll
  void beforeAll() {
    Properties properties = ConsensusCommitJdbcEnv.getProperties(TEST_NAME);

    // Set the isolation level for consistency reads for virtual tables
    RdbEngineStrategy rdbEngine =
        RdbEngineFactory.create(new JdbcConfig(new DatabaseConfig(properties)));
    properties.setProperty(
        JdbcConfig.ISOLATION_LEVEL,
        JdbcTestUtils.getIsolationLevel(
                rdbEngine.getMinimumIsolationLevelForConsistentVirtualTableRead())
            .name());

    testUtils = new JdbcImportPrimaryKeyOrderTestUtils(properties);
    TransactionFactory factory = TransactionFactory.create(properties);
    admin = factory.getTransactionAdmin();
    manager = factory.getTransactionManager();
  }

  @BeforeEach
  void setUp() throws Exception {
    admin.createCoordinatorTables(true);
    testUtils.createSchemaAndTable(NAMESPACE);
  }

  @AfterEach
  void afterEach() {
    try {
      if (admin.tableExists(NAMESPACE, IMPORTED_TABLE)) {
        // This also drops the data table and the transaction metadata table
        admin.dropTable(NAMESPACE, IMPORTED_TABLE);
      } else {
        testUtils.dropTable(NAMESPACE, TABLE);
      }
      admin.dropNamespace(NAMESPACE);
      admin.dropCoordinatorTables();
    } catch (Exception e) {
      logger.warn("Failed to clean up the namespace", e);
    }
  }

  @AfterAll
  void afterAll() {
    try {
      if (manager != null) {
        manager.close();
      }
    } catch (Exception e) {
      logger.warn("Failed to close manager", e);
    }

    try {
      if (admin != null) {
        admin.close();
      }
    } catch (Exception e) {
      logger.warn("Failed to close admin", e);
    }

    try {
      if (testUtils != null) {
        testUtils.close();
      }
    } catch (Exception e) {
      logger.warn("Failed to close test utils", e);
    }
  }

  @SuppressWarnings("unused")
  static boolean isSqlite() {
    return JdbcEnv.isSqlite();
  }

  @Test
  void importTable_ShouldRegisterPartitionKeyInPrimaryKeyOrder() throws ExecutionException {
    // Act
    admin.importTable(NAMESPACE, TABLE, CREATION_OPTIONS);

    // Assert
    // The partition key of the imported table, with which applications build keys, comes from the
    // metadata of the data table
    TableMetadata metadata = admin.getTableMetadata(NAMESPACE, IMPORTED_TABLE);
    assertThat(metadata).isNotNull();
    assertThat(new ArrayList<>(metadata.getPartitionKeyNames()))
        .containsExactlyElementsOf(PRIMARY_KEY_ORDER);
  }

  @Test
  void importTable_ShouldCreateTransactionMetadataTableWithSamePrimaryKeyOrderAsDataTable()
      throws ExecutionException, SQLException {
    // Act
    admin.importTable(NAMESPACE, TABLE, CREATION_OPTIONS);

    // Assert
    // The data table is created by this test, so this verifies that the KEY_SEQ values returned by
    // the JDBC driver reflect the primary key order. The check of the transaction metadata table
    // below relies on it
    assertThat(testUtils.getPhysicalPrimaryKeyNames(NAMESPACE, TABLE))
        .containsExactlyElementsOf(PRIMARY_KEY_ORDER);
    assertThat(testUtils.getPhysicalPrimaryKeyNames(NAMESPACE, TRANSACTION_METADATA_TABLE))
        .containsExactlyElementsOf(PRIMARY_KEY_ORDER);
  }

  @Test
  void importTable_ForImportedTable_ShouldInsertThenGetWithKeyInPrimaryKeyOrder()
      throws ExecutionException, TransactionException {
    // Arrange
    admin.importTable(NAMESPACE, TABLE, CREATION_OPTIONS);
    Key partitionKey =
        Key.newBuilder().addText(PK_C, "c").addText(PK_A, "a").addText(PK_B, "b").build();

    // Act
    DistributedTransaction transaction = manager.begin();
    transaction.insert(
        Insert.newBuilder()
            .namespace(NAMESPACE)
            .table(IMPORTED_TABLE)
            .partitionKey(partitionKey)
            .textValue(COL, "value")
            .build());
    transaction.commit();

    transaction = manager.begin();
    Optional<Result> result =
        transaction.get(
            Get.newBuilder()
                .namespace(NAMESPACE)
                .table(IMPORTED_TABLE)
                .partitionKey(partitionKey)
                .build());
    transaction.commit();

    // Assert
    assertThat(result).isPresent();
    assertThat(result.get().getText(COL)).isEqualTo("value");
  }
}
