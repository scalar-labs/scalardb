package com.scalar.db.api;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.service.StorageFactory;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class DistributedStorageJapaneseIntegrationTestBase {

  private static final String TEST_NAME = "storage_jp";
  private static final String NAMESPACE = "int_test_" + TEST_NAME;
  private static final String TABLE = "test_table";
  private static final String COL_NAME1 = "c1";
  private static final String COL_NAME2 = "c2";
  private static final String COL_NAME3 = "c3";

  private DistributedStorage storage;
  private DistributedStorageAdmin admin;
  private String namespace;

  @BeforeAll
  public void beforeAll() throws Exception {
    StorageFactory factory = StorageFactory.create(getProperties(TEST_NAME));
    admin = factory.getStorageAdmin();
    namespace = getNamespace();
    createTable();
    storage = factory.getStorage();
  }

  protected abstract Properties getProperties(String testName);

  protected String getNamespace() {
    return NAMESPACE;
  }

  private void createTable() throws ExecutionException {
    Map<String, String> options = getCreationOptions();
    admin.createNamespace(namespace, true, options);
    admin.createTable(
        namespace,
        TABLE,
        TableMetadata.newBuilder()
            .addColumn(COL_NAME1, DataType.TEXT)
            .addColumn(COL_NAME2, DataType.TEXT)
            .addColumn(COL_NAME3, DataType.TEXT)
            .addPartitionKey(COL_NAME1)
            .addClusteringKey(COL_NAME2)
            .build(),
        true,
        options);
  }

  protected Map<String, String> getCreationOptions() {
    return Collections.emptyMap();
  }

  @BeforeEach
  public void setUp() throws Exception {
    truncateTable();
  }

  private void truncateTable() throws ExecutionException {
    admin.truncateTable(namespace, TABLE);
  }

  @AfterAll
  public void afterAll() throws Exception {
    dropTable();
    admin.close();
    storage.close();
  }

  private void dropTable() throws ExecutionException {
    admin.dropTable(namespace, TABLE);
    admin.dropNamespace(namespace);
  }

  @Test
  public void operation_ShouldWorkProperly() throws ExecutionException, IOException {
    // Arrange
    storage.put(
        Put.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(Key.ofText(COL_NAME1, "あああ"))
            .clusteringKey(Key.ofText(COL_NAME2, "あああ"))
            .textValue(COL_NAME3, "アアア")
            .build());
    storage.put(
        Put.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(Key.ofText(COL_NAME1, "あああ"))
            .clusteringKey(Key.ofText(COL_NAME2, "いいい"))
            .textValue(COL_NAME3, "イイイ")
            .build());
    storage.put(
        Put.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(Key.ofText(COL_NAME1, "あああ"))
            .clusteringKey(Key.ofText(COL_NAME2, "ううう"))
            .textValue(COL_NAME3, "ウウウ")
            .build());
    storage.put(
        Put.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(Key.ofText(COL_NAME1, "いいい"))
            .clusteringKey(Key.ofText(COL_NAME2, "あああ"))
            .textValue(COL_NAME3, "アアア")
            .build());

    // Act Assert
    Optional<Result> result =
        storage.get(
            Get.newBuilder()
                .namespace(namespace)
                .table(TABLE)
                .partitionKey(Key.ofText(COL_NAME1, "あああ"))
                .clusteringKey(Key.ofText(COL_NAME2, "あああ"))
                .build());
    assertThat(result).isPresent();
    assertThat(result.get().getText(COL_NAME1)).isEqualTo("あああ");
    assertThat(result.get().getText(COL_NAME2)).isEqualTo("あああ");
    assertThat(result.get().getText(COL_NAME3)).isEqualTo("アアア");

    Scanner scanner =
        storage.scan(
            Scan.newBuilder()
                .namespace(namespace)
                .table(TABLE)
                .partitionKey(Key.ofText(COL_NAME1, "あああ"))
                .build());
    List<Result> results = scanner.all();
    assertThat(results).hasSize(3);
    assertThat(results.get(0).getText(COL_NAME1)).isEqualTo("あああ");
    assertThat(results.get(0).getText(COL_NAME2)).isEqualTo("あああ");
    assertThat(results.get(0).getText(COL_NAME3)).isEqualTo("アアア");
    assertThat(results.get(1).getText(COL_NAME1)).isEqualTo("あああ");
    assertThat(results.get(1).getText(COL_NAME2)).isEqualTo("いいい");
    assertThat(results.get(1).getText(COL_NAME3)).isEqualTo("イイイ");
    assertThat(results.get(2).getText(COL_NAME1)).isEqualTo("あああ");
    assertThat(results.get(2).getText(COL_NAME2)).isEqualTo("ううう");
    assertThat(results.get(2).getText(COL_NAME3)).isEqualTo("ウウウ");
    scanner.close();

    Delete delete =
        Delete.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(Key.ofText(COL_NAME1, "あああ"))
            .clusteringKey(Key.ofText(COL_NAME2, "あああ"))
            .build();
    storage.delete(delete);
    result =
        storage.get(
            Get.newBuilder()
                .namespace(namespace)
                .table(TABLE)
                .partitionKey(Key.ofText(COL_NAME1, "あああ"))
                .clusteringKey(Key.ofText(COL_NAME2, "あああ"))
                .build());
    assertThat(result).isEmpty();
  }

  @Test
  public void operation_WithHankaku_ShouldWorkProperly() throws ExecutionException, IOException {
    // Arrange
    storage.put(
        Put.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(Key.ofText(COL_NAME1, "ｱｱｱ"))
            .clusteringKey(Key.ofText(COL_NAME2, "ｱｱｱ"))
            .textValue(COL_NAME3, "１１１")
            .build());
    storage.put(
        Put.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(Key.ofText(COL_NAME1, "ｱｱｱ"))
            .clusteringKey(Key.ofText(COL_NAME2, "ｲｲｲ"))
            .textValue(COL_NAME3, "２２２")
            .build());
    storage.put(
        Put.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(Key.ofText(COL_NAME1, "ｱｱｱ"))
            .clusteringKey(Key.ofText(COL_NAME2, "ｳｳｳ"))
            .textValue(COL_NAME3, "３３３")
            .build());
    storage.put(
        Put.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(Key.ofText(COL_NAME1, "ｲｲｲ"))
            .clusteringKey(Key.ofText(COL_NAME2, "ｱｱｱ"))
            .textValue(COL_NAME3, "１１１")
            .build());

    // Act Assert
    Optional<Result> result =
        storage.get(
            Get.newBuilder()
                .namespace(namespace)
                .table(TABLE)
                .partitionKey(Key.ofText(COL_NAME1, "ｱｱｱ"))
                .clusteringKey(Key.ofText(COL_NAME2, "ｱｱｱ"))
                .build());
    assertThat(result).isPresent();
    assertThat(result.get().getText(COL_NAME1)).isEqualTo("ｱｱｱ");
    assertThat(result.get().getText(COL_NAME2)).isEqualTo("ｱｱｱ");
    assertThat(result.get().getText(COL_NAME3)).isEqualTo("１１１");

    Scanner scanner =
        storage.scan(
            Scan.newBuilder()
                .namespace(namespace)
                .table(TABLE)
                .partitionKey(Key.ofText(COL_NAME1, "ｱｱｱ"))
                .build());
    List<Result> results = scanner.all();
    assertThat(results).hasSize(3);
    assertThat(results.get(0).getText(COL_NAME1)).isEqualTo("ｱｱｱ");
    assertThat(results.get(0).getText(COL_NAME2)).isEqualTo("ｱｱｱ");
    assertThat(results.get(0).getText(COL_NAME3)).isEqualTo("１１１");
    assertThat(results.get(1).getText(COL_NAME1)).isEqualTo("ｱｱｱ");
    assertThat(results.get(1).getText(COL_NAME2)).isEqualTo("ｲｲｲ");
    assertThat(results.get(1).getText(COL_NAME3)).isEqualTo("２２２");
    assertThat(results.get(2).getText(COL_NAME1)).isEqualTo("ｱｱｱ");
    assertThat(results.get(2).getText(COL_NAME2)).isEqualTo("ｳｳｳ");
    assertThat(results.get(2).getText(COL_NAME3)).isEqualTo("３３３");
    scanner.close();

    Delete delete =
        Delete.newBuilder()
            .namespace(namespace)
            .table(TABLE)
            .partitionKey(Key.ofText(COL_NAME1, "ｱｱｱ"))
            .clusteringKey(Key.ofText(COL_NAME2, "ｱｱｱ"))
            .build();
    storage.delete(delete);
    result =
        storage.get(
            Get.newBuilder()
                .namespace(namespace)
                .table(TABLE)
                .partitionKey(Key.ofText(COL_NAME1, "ｱｱｱ"))
                .clusteringKey(Key.ofText(COL_NAME2, "ｱｱｱ"))
                .build());
    assertThat(result).isEmpty();
  }
}
