package com.scalar.db.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.service.TransactionFactory;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class DistributedTransactionAdminImportTableIntegrationTestBase {

  private static final String TEST_NAME = "tx_admin_import_table";
  private static final String NAMESPACE = "int_test_" + TEST_NAME;
  private final Map<String, TableMetadata> tables = new HashMap<>();

  protected DistributedTransactionAdmin admin;

  @BeforeAll
  public void beforeAll() throws Exception {
    initialize(TEST_NAME);
  }

  protected void initialize(String testName) throws Exception {}

  protected abstract Properties getProperties(String testName);

  protected String getNamespace() {
    return NAMESPACE;
  }

  protected Map<String, String> getCreationOptions() {
    return Collections.emptyMap();
  }

  private void dropTable() throws Exception {
    for (Entry<String, TableMetadata> entry : tables.entrySet()) {
      String table = entry.getKey();
      TableMetadata metadata = entry.getValue();
      if (metadata == null) {
        dropNonImportableTable(table);
      } else {
        admin.dropTable(getNamespace(), table);
      }
    }
    admin.dropNamespace(getNamespace());
  }

  @BeforeEach
  protected void setUp() throws Exception {
    TransactionFactory factory = TransactionFactory.create(getProperties(TEST_NAME));
    admin = factory.getTransactionAdmin();
  }

  @AfterEach
  protected void afterEach() throws Exception {
    dropTable();
    admin.close();
  }

  @AfterAll
  protected void afterAll() throws Exception {}

  protected abstract Map<String, TableMetadata> createExistingDatabaseWithAllDataTypes()
      throws Exception;

  protected abstract void dropNonImportableTable(String table) throws Exception;

  @Test
  public void importTable_ShouldWorkProperly() throws Exception {
    // Arrange
    admin.createNamespace(getNamespace(), getCreationOptions());
    tables.putAll(createExistingDatabaseWithAllDataTypes());

    // Act Assert
    for (Entry<String, TableMetadata> entry : tables.entrySet()) {
      String table = entry.getKey();
      TableMetadata metadata = entry.getValue();
      if (metadata == null) {
        importTable_ForNonImportableTable_ShouldThrowIllegalArgumentException(table);
      } else {
        importTable_ForImportableTable_ShouldImportProperly(table, metadata);
      }
    }
    importTable_ForNonExistingTable_ShouldThrowIllegalArgumentException();
  }

  @Test
  public void importTable_ForUnsupportedDatabase_ShouldThrowUnsupportedOperationException()
      throws ExecutionException {
    // Arrange
    admin.createNamespace(getNamespace(), getCreationOptions());

    // Act Assert
    assertThatThrownBy(() -> admin.importTable(getNamespace(), "unsupported_db"))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  private void importTable_ForImportableTable_ShouldImportProperly(
      String table, TableMetadata metadata) throws ExecutionException {
    // Act
    admin.importTable(getNamespace(), table);

    // Assert
    assertThat(admin.tableExists(getNamespace(), table)).isTrue();
    assertThat(admin.getTableMetadata(getNamespace(), table)).isEqualTo(metadata);
  }

  private void importTable_ForNonImportableTable_ShouldThrowIllegalArgumentException(String table) {
    // Act Assert
    assertThatThrownBy(() -> admin.importTable(getNamespace(), table))
        .isInstanceOf(IllegalArgumentException.class);
  }

  private void importTable_ForNonExistingTable_ShouldThrowIllegalArgumentException() {
    // Act Assert
    assertThatThrownBy(() -> admin.importTable(getNamespace(), "non-existing-table"))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
