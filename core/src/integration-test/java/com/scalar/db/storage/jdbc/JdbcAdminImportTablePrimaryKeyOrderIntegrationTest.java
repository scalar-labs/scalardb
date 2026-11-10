package com.scalar.db.storage.jdbc;

import static com.scalar.db.storage.jdbc.JdbcImportPrimaryKeyOrderTestUtils.COL;
import static com.scalar.db.storage.jdbc.JdbcImportPrimaryKeyOrderTestUtils.PK_A;
import static com.scalar.db.storage.jdbc.JdbcImportPrimaryKeyOrderTestUtils.PK_B;
import static com.scalar.db.storage.jdbc.JdbcImportPrimaryKeyOrderTestUtils.PK_C;
import static com.scalar.db.storage.jdbc.JdbcImportPrimaryKeyOrderTestUtils.PRIMARY_KEY_ORDER;
import static com.scalar.db.storage.jdbc.JdbcImportPrimaryKeyOrderTestUtils.TABLE;
import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Key;
import com.scalar.db.service.StorageFactory;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
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
class JdbcAdminImportTablePrimaryKeyOrderIntegrationTest {
  private static final Logger logger =
      LoggerFactory.getLogger(JdbcAdminImportTablePrimaryKeyOrderIntegrationTest.class);

  private static final String TEST_NAME = "import_pk_order";
  private static final String NAMESPACE = "int_test_" + TEST_NAME;

  private JdbcImportPrimaryKeyOrderTestUtils testUtils;
  private DistributedStorageAdmin admin;
  private DistributedStorage storage;

  @BeforeAll
  void beforeAll() {
    Properties properties = JdbcEnv.getProperties(TEST_NAME);
    testUtils = new JdbcImportPrimaryKeyOrderTestUtils(properties);
    StorageFactory factory = StorageFactory.create(properties);
    admin = factory.getStorageAdmin();
    storage = factory.getStorage();
  }

  @BeforeEach
  void setUp() throws SQLException {
    testUtils.createSchemaAndTable(NAMESPACE);
  }

  @AfterEach
  void afterEach() {
    try {
      if (admin.tableExists(NAMESPACE, TABLE)) {
        admin.dropTable(NAMESPACE, TABLE);
      } else {
        testUtils.dropTable(NAMESPACE, TABLE);
      }
      admin.dropNamespace(NAMESPACE);
    } catch (Exception e) {
      logger.warn("Failed to clean up the namespace", e);
    }
  }

  @AfterAll
  void afterAll() {
    try {
      if (admin != null) {
        admin.close();
      }
    } catch (Exception e) {
      logger.warn("Failed to close admin", e);
    }

    try {
      if (storage != null) {
        storage.close();
      }
    } catch (Exception e) {
      logger.warn("Failed to close storage", e);
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
    admin.importTable(NAMESPACE, TABLE, Collections.emptyMap());

    // Assert
    TableMetadata metadata = admin.getTableMetadata(NAMESPACE, TABLE);
    assertThat(metadata).isNotNull();
    assertThat(new ArrayList<>(metadata.getPartitionKeyNames()))
        .containsExactlyElementsOf(PRIMARY_KEY_ORDER);
  }

  @Test
  void importTable_ForImportedTable_ShouldPutThenGetWithKeyInPrimaryKeyOrder()
      throws ExecutionException {
    // Arrange
    admin.importTable(NAMESPACE, TABLE, Collections.emptyMap());
    Key partitionKey =
        Key.newBuilder().addText(PK_C, "c").addText(PK_A, "a").addText(PK_B, "b").build();

    // Act
    storage.put(
        Put.newBuilder()
            .namespace(NAMESPACE)
            .table(TABLE)
            .partitionKey(partitionKey)
            .textValue(COL, "value")
            .build());
    Optional<Result> result =
        storage.get(
            Get.newBuilder().namespace(NAMESPACE).table(TABLE).partitionKey(partitionKey).build());

    // Assert
    assertThat(result).isPresent();
    assertThat(result.get().getText(COL)).isEqualTo("value");
  }
}
