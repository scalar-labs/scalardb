package com.scalar.db.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.service.StorageFactory;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class DistributedStorageAdminImportTableIntegrationTestBase {
  private static final Logger logger =
      LoggerFactory.getLogger(DistributedStorageAdminImportTableIntegrationTestBase.class);

  private static final String TEST_NAME = "storage_admin_import_table";
  private static final String NAMESPACE = "int_test_" + TEST_NAME;
  private final List<TestData> testDataList = new ArrayList<>();
  protected DistributedStorageAdmin admin;
  protected DistributedStorage storage;

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
    for (TestData testData : testDataList) {
      if (!testData.isImportableTable()) {
        dropNonImportableTable(testData.getTableName());
      } else {
        admin.dropTable(getNamespace(), testData.getTableName());
      }
    }
    if (!admin.namespaceExists(getNamespace())) {
      // Create metadata to be able to delete the namespace using the Admin
      admin.repairNamespace(getNamespace(), getCreationOptions());
    }
    admin.dropNamespace(getNamespace());
  }

  @BeforeEach
  protected void setUp() throws Exception {
    StorageFactory factory = StorageFactory.create(getProperties(TEST_NAME));
    admin = factory.getStorageAdmin();
    storage = factory.getStorage();
  }

  @AfterEach
  protected void afterEach() {
    try {
      dropTable();
    } catch (Exception e) {
      logger.warn("Failed to drop tables", e);
    }

    try {
      if (admin != null) {
        admin.close();
      }
    } catch (Exception e) {
      logger.warn("Failed to close admin", e);
    }
  }

  @AfterAll
  protected void afterAll() throws Exception {}

  protected abstract List<TestData> createExistingDatabaseWithAllDataTypes() throws SQLException;

  protected abstract void dropNonImportableTable(String table) throws Exception;

  @Test
  public void importTable_ShouldWorkProperly() throws Exception {
    // Arrange
    testDataList.addAll(createExistingDatabaseWithAllDataTypes());

    // Act Assert
    for (TestData testData : testDataList) {
      if (!testData.isImportableTable()) {
        importTable_ForNonImportableTable_ShouldThrowIllegalArgumentException(
            testData.getTableName());
      } else {
        importTable_ForImportableTable_ShouldImportProperly(
            testData.getTableName(),
            testData.getOverrideColumnsType(),
            testData.getTableMetadata());
      }
    }
    importTable_ForNonExistingTable_ShouldThrowIllegalArgumentException();
    importTable_ForImportedTable_ShouldPutThenGetCorrectly();
  }

  @Test
  public void importTable_ForUnsupportedDatabase_ShouldThrowUnsupportedOperationException()
      throws ExecutionException {
    // Act Assert
    assertThatThrownBy(
            () -> admin.importTable(getNamespace(), "unsupported_db", Collections.emptyMap()))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  private void importTable_ForImportableTable_ShouldImportProperly(
      String table, Map<String, DataType> overrideColumnsType, TableMetadata metadata)
      throws ExecutionException {
    // Act
    admin.importTable(getNamespace(), table, Collections.emptyMap(), overrideColumnsType);

    // Assert
    assertThat(admin.namespaceExists(getNamespace())).isTrue();
    assertThat(admin.tableExists(getNamespace(), table)).isTrue();
    assertThat(admin.getTableMetadata(getNamespace(), table)).isEqualTo(metadata);
  }

  private void importTable_ForNonImportableTable_ShouldThrowIllegalArgumentException(String table) {
    // Act Assert
    assertThatThrownBy(
            () -> admin.importTable(getNamespace(), table, Collections.emptyMap()),
            "non-importable data type test failed: " + table)
        .isInstanceOf(IllegalArgumentException.class);
  }

  private void importTable_ForNonExistingTable_ShouldThrowIllegalArgumentException() {
    // Act Assert
    assertThatThrownBy(
            () -> admin.importTable(getNamespace(), "non-existing-table", Collections.emptyMap()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  public void importTable_ForImportedTable_ShouldPutThenGetCorrectly() throws ExecutionException {
    // Arrange
    List<Put> puts =
        testDataList.stream()
            .filter(TestData::isImportableTable)
            .map(td -> td.getPut(getNamespace(), td.getTableName()))
            .collect(Collectors.toList());
    List<Get> gets =
        testDataList.stream()
            .filter(TestData::isImportableTable)
            .map(td -> td.getGet(getNamespace(), td.getTableName()))
            .collect(Collectors.toList());

    // Act
    for (Put put : puts) {
      storage.put(put);
    }
    List<Optional<Result>> results = new ArrayList<>();
    for (Get get : gets) {
      results.add(storage.get(get));
    }

    // Assert
    for (int i = 0; i < results.size(); i++) {
      Put put = puts.get(i);
      Optional<Result> optResult = results.get(i);

      assertThat(optResult).isPresent();
      Result result = optResult.get();
      Set<String> actualColumnNamesWithoutKeys = new HashSet<>(result.getContainedColumnNames());
      actualColumnNamesWithoutKeys.removeAll(
          put.getPartitionKey().getColumns().stream()
              .map(Column::getName)
              .collect(Collectors.toSet()));

      assertThat(actualColumnNamesWithoutKeys)
          .containsExactlyInAnyOrderElementsOf(put.getContainedColumnNames());
      result.getColumns().entrySet().stream()
          .filter(
              e -> {
                // Filter partition key columns
                return !put.getPartitionKey().getColumns().contains(e.getValue());
              })
          .forEach(
              entry ->
                  // Assert each result column is equal to the column inserted with the put
                  assertThat(entry.getValue()).isEqualTo(put.getColumns().get(entry.getKey())));
    }
  }

  /** This interface defines test data for running import table related integration tests. */
  public interface TestData {

    /** @return true if the table is supported for import, false otherwise */
    boolean isImportableTable();

    /** @return the table name */
    String getTableName();

    /** @return the columns for which the data type should be overridden when importing the table */
    Map<String, DataType> getOverrideColumnsType();

    /*
     * @return the expected table metadata of the imported table
     */
    TableMetadata getTableMetadata();

    /** @return a sample Insert operation for the table */
    Insert getInsert(String namespace, String table);

    /** @return a sample Put operation for the table */
    Put getPut(String namespace, String table);

    /**
     * @return a Get operation to retrieve the record inserted with {@link #getPut(String, String)}
     *     or {@link #getInsert(String, String)}
     */
    Get getGet(String namespace, String table);
  }
}
