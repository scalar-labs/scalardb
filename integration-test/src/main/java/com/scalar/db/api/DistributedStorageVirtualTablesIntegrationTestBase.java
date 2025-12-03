package com.scalar.db.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.service.StorageFactory;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class DistributedStorageVirtualTablesIntegrationTestBase {
  private static final Logger logger =
      LoggerFactory.getLogger(DistributedStorageVirtualTablesIntegrationTestBase.class);

  private static final String TEST_NAME = "storage_vtable";
  private static final String NAMESPACE = "int_test_" + TEST_NAME;
  private static final String LEFT_SOURCE_TABLE = "left_source_table";
  private static final String RIGHT_SOURCE_TABLE = "right_source_table";
  private static final String VIRTUAL_TABLE = "virtual_table";

  protected DistributedStorageAdmin admin;
  protected DistributedStorage storage;
  protected String namespace;

  @BeforeAll
  public void beforeAll() throws Exception {
    initialize(TEST_NAME);
    StorageFactory factory = StorageFactory.create(getProperties(TEST_NAME));
    admin = factory.getStorageAdmin();
    namespace = getNamespace();
    storage = factory.getStorage();
    admin.createNamespace(namespace, true, getCreationOptions());
  }

  protected void initialize(String testName) throws Exception {}

  protected abstract Properties getProperties(String testName);

  protected String getNamespace() {
    return NAMESPACE;
  }

  protected Map<String, String> getCreationOptions() {
    return Collections.emptyMap();
  }

  @BeforeEach
  public void setUp() throws Exception {
    // Create left source table with partition key (pk1, pk2), clustering key (ck1) and columns
    // (col1, col2, col3, col4, col5)
    TableMetadata leftSourceTableMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey("pk1")
            .addPartitionKey("pk2")
            .addClusteringKey("ck1")
            .addColumn("pk1", DataType.INT)
            .addColumn("pk2", DataType.TEXT)
            .addColumn("ck1", DataType.TEXT)
            .addColumn("col1", DataType.INT)
            .addColumn("col2", DataType.TEXT)
            .addColumn("col3", DataType.BIGINT)
            .addColumn("col4", DataType.FLOAT)
            .addColumn("col5", DataType.BOOLEAN)
            .build();
    admin.createTable(
        namespace, LEFT_SOURCE_TABLE, leftSourceTableMetadata, true, getCreationOptions());

    // Create right source table with the same partition key (pk1, pk2), clustering key (ck1) and
    // different columns (col6, col7, col8, col9, col10)
    TableMetadata rightSourceTableMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey("pk1")
            .addPartitionKey("pk2")
            .addClusteringKey("ck1")
            .addColumn("pk1", DataType.INT)
            .addColumn("pk2", DataType.TEXT)
            .addColumn("ck1", DataType.TEXT)
            .addColumn("col6", DataType.DOUBLE)
            .addColumn("col7", DataType.TEXT)
            .addColumn("col8", DataType.INT)
            .addColumn("col9", DataType.BOOLEAN)
            .addColumn("col10", DataType.BIGINT)
            .build();
    admin.createTable(
        namespace, RIGHT_SOURCE_TABLE, rightSourceTableMetadata, true, getCreationOptions());

    // Create virtual table
    admin.createVirtualTable(
        namespace,
        VIRTUAL_TABLE,
        namespace,
        LEFT_SOURCE_TABLE,
        namespace,
        RIGHT_SOURCE_TABLE,
        VirtualTableJoinType.INNER,
        true,
        getCreationOptions());
  }

  @AfterEach
  public void tearDown() throws Exception {
    try {
      admin.dropTable(namespace, VIRTUAL_TABLE, true);
    } catch (Exception e) {
      logger.warn("Failed to drop virtual table", e);
    }
    try {
      admin.dropTable(namespace, RIGHT_SOURCE_TABLE, true);
    } catch (Exception e) {
      logger.warn("Failed to drop right source table", e);
    }
    try {
      admin.dropTable(namespace, LEFT_SOURCE_TABLE, true);
    } catch (Exception e) {
      logger.warn("Failed to drop left source table", e);
    }
  }

  @AfterAll
  public void afterAll() throws Exception {
    if (admin != null) {
      try {
        admin.dropNamespace(namespace, true);
      } catch (Exception e) {
        logger.warn("Failed to drop namespace", e);
      }
      try {
        admin.close();
      } catch (Exception e) {
        logger.warn("Failed to close admin", e);
      }
    }

    try {
      if (storage != null) {
        storage.close();
      }
    } catch (Exception e) {
      logger.warn("Failed to close storage", e);
    }
  }

  @Test
  public void getTableMetadata_ForVirtualTable_ShouldReturnCorrectMetadata() throws Exception {
    // Arrange
    TableMetadata expectedMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey("pk1")
            .addPartitionKey("pk2")
            .addClusteringKey("ck1")
            .addColumn("pk1", DataType.INT)
            .addColumn("pk2", DataType.TEXT)
            .addColumn("ck1", DataType.TEXT)
            .addColumn("col1", DataType.INT)
            .addColumn("col2", DataType.TEXT)
            .addColumn("col3", DataType.BIGINT)
            .addColumn("col4", DataType.FLOAT)
            .addColumn("col5", DataType.BOOLEAN)
            .addColumn("col6", DataType.DOUBLE)
            .addColumn("col7", DataType.TEXT)
            .addColumn("col8", DataType.INT)
            .addColumn("col9", DataType.BOOLEAN)
            .addColumn("col10", DataType.BIGINT)
            .build();

    // Act
    TableMetadata virtualTableMetadata = admin.getTableMetadata(namespace, VIRTUAL_TABLE);

    // Assert
    assertThat(virtualTableMetadata).isEqualTo(expectedMetadata);
  }

  @Test
  public void getVirtualTableInfo_ForVirtualTable_ShouldReturnCorrectInfo() throws Exception {
    // Arrange

    // Act
    Optional<VirtualTableInfo> tableInfo = admin.getVirtualTableInfo(namespace, VIRTUAL_TABLE);

    // Assert
    assertThat(tableInfo).isPresent();
    assertThat(tableInfo.get().getLeftSourceNamespaceName()).isEqualTo(namespace);
    assertThat(tableInfo.get().getLeftSourceTableName()).isEqualTo(LEFT_SOURCE_TABLE);
    assertThat(tableInfo.get().getRightSourceNamespaceName()).isEqualTo(namespace);
    assertThat(tableInfo.get().getRightSourceTableName()).isEqualTo(RIGHT_SOURCE_TABLE);
    assertThat(tableInfo.get().getJoinType()).isEqualTo(VirtualTableJoinType.INNER);
  }

  @Test
  public void getNamespaceTableNames_ShouldIncludeVirtualTable() throws Exception {
    // Arrange - create additional regular tables
    String regularTable1 = "regular_table1";
    String regularTable2 = "regular_table2";
    TableMetadata regularTableMetadata =
        TableMetadata.newBuilder()
            .addPartitionKey("pk")
            .addColumn("pk", DataType.INT)
            .addColumn("col", DataType.TEXT)
            .build();

    try {
      admin.createTable(namespace, regularTable1, regularTableMetadata, true, getCreationOptions());
      admin.createTable(namespace, regularTable2, regularTableMetadata, true, getCreationOptions());

      // Act
      Set<String> tableNames = admin.getNamespaceTableNames(namespace);

      // Assert - should include source tables, virtual table, and regular tables
      assertThat(tableNames)
          .containsExactlyInAnyOrder(
              LEFT_SOURCE_TABLE, RIGHT_SOURCE_TABLE, VIRTUAL_TABLE, regularTable1, regularTable2);
    } finally {
      // Clean up
      try {
        admin.dropTable(namespace, regularTable1, true);
      } catch (Exception e) {
        logger.warn("Failed to drop regular_table1", e);
      }
      try {
        admin.dropTable(namespace, regularTable2, true);
      } catch (Exception e) {
        logger.warn("Failed to drop regular_table2", e);
      }
    }
  }

  @Test
  public void truncateTable_ForVirtualTable_ShouldTruncateSourceTables() throws Exception {
    // Arrange - insert data into both source tables via virtual table
    storage.put(
        Put.newBuilder()
            .namespace(namespace)
            .table(VIRTUAL_TABLE)
            .partitionKey(Key.of("pk1", 1, "pk2", "1"))
            .clusteringKey(Key.ofText("ck1", "aaa"))
            .intValue("col1", 100)
            .textValue("col2", "data1")
            .bigIntValue("col3", 1000L)
            .floatValue("col4", 10.5f)
            .booleanValue("col5", true)
            .doubleValue("col6", 20.5)
            .textValue("col7", "data2")
            .intValue("col8", 200)
            .booleanValue("col9", false)
            .bigIntValue("col10", 2000L)
            .build());
    storage.put(
        Put.newBuilder()
            .namespace(namespace)
            .table(VIRTUAL_TABLE)
            .partitionKey(Key.of("pk1", 2, "pk2", "2"))
            .clusteringKey(Key.ofText("ck1", "bbb"))
            .intValue("col1", 101)
            .textValue("col2", "data3")
            .bigIntValue("col3", 1001L)
            .floatValue("col4", 11.5f)
            .booleanValue("col5", false)
            .doubleValue("col6", 21.5)
            .textValue("col7", "data4")
            .intValue("col8", 201)
            .booleanValue("col9", true)
            .bigIntValue("col10", 2001L)
            .build());

    // Verify data exists
    Scanner scanner =
        storage.scan(
            Scan.newBuilder()
                .namespace(namespace)
                .table(VIRTUAL_TABLE)
                .partitionKey(Key.of("pk1", 1, "pk2", "1"))
                .build());
    assertThat(scanner.all()).isNotEmpty();
    scanner.close();

    // Act
    admin.truncateTable(namespace, VIRTUAL_TABLE);

    // Assert - verify both source tables are empty
    Scanner leftScanner =
        storage.scan(
            Scan.newBuilder()
                .namespace(namespace)
                .table(LEFT_SOURCE_TABLE)
                .partitionKey(Key.of("pk1", 1, "pk2", "1"))
                .build());
    assertThat(leftScanner.all()).isEmpty();
    leftScanner.close();

    Scanner rightScanner =
        storage.scan(
            Scan.newBuilder()
                .namespace(namespace)
                .table(RIGHT_SOURCE_TABLE)
                .partitionKey(Key.of("pk1", 1, "pk2", "1"))
                .build());
    assertThat(rightScanner.all()).isEmpty();
    rightScanner.close();

    // Verify via virtual table
    Scanner virtualScanner =
        storage.scan(
            Scan.newBuilder()
                .namespace(namespace)
                .table(VIRTUAL_TABLE)
                .partitionKey(Key.of("pk1", 1, "pk2", "1"))
                .build());
    assertThat(virtualScanner.all()).isEmpty();
    virtualScanner.close();
  }

  @Test
  public void dropTable_ForVirtualTable_ShouldDropVirtualTableSuccessfully() throws Exception {
    // Arrange - verify virtual table exists
    assertThat(admin.tableExists(namespace, VIRTUAL_TABLE)).isTrue();
    Optional<VirtualTableInfo> virtualTableInfoBefore =
        admin.getVirtualTableInfo(namespace, VIRTUAL_TABLE);
    assertThat(virtualTableInfoBefore).isPresent();

    // Act
    admin.dropTable(namespace, VIRTUAL_TABLE);

    // Assert - verify virtual table no longer exists
    assertThat(admin.tableExists(namespace, VIRTUAL_TABLE)).isFalse();

    // Verify source tables still exist
    assertThat(admin.tableExists(namespace, LEFT_SOURCE_TABLE)).isTrue();
    assertThat(admin.tableExists(namespace, RIGHT_SOURCE_TABLE)).isTrue();
  }

  @Test
  public void
      dropTable_ForSourceTable_WhenReferencedByVirtualTable_ShouldThrowIllegalArgumentException()
          throws Exception {
    // Arrange - verify virtual table and source tables exist
    assertThat(admin.tableExists(namespace, VIRTUAL_TABLE)).isTrue();
    assertThat(admin.tableExists(namespace, LEFT_SOURCE_TABLE)).isTrue();
    assertThat(admin.tableExists(namespace, RIGHT_SOURCE_TABLE)).isTrue();

    // Act Assert - try to drop left source table
    assertThatThrownBy(() -> admin.dropTable(namespace, LEFT_SOURCE_TABLE))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(LEFT_SOURCE_TABLE)
        .hasMessageContaining(VIRTUAL_TABLE);

    // Act Assert - try to drop right source table
    assertThatThrownBy(() -> admin.dropTable(namespace, RIGHT_SOURCE_TABLE))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(RIGHT_SOURCE_TABLE)
        .hasMessageContaining(VIRTUAL_TABLE);

    // Assert - verify all tables still exist
    assertThat(admin.tableExists(namespace, LEFT_SOURCE_TABLE)).isTrue();
    assertThat(admin.tableExists(namespace, RIGHT_SOURCE_TABLE)).isTrue();
    assertThat(admin.tableExists(namespace, VIRTUAL_TABLE)).isTrue();
  }

  @Test
  public void createIndex_OnLeftSourceTableColumn_ShouldCreateIndexSuccessfully() throws Exception {
    // Arrange
    String columnName = "col1"; // col1 belongs to left source table

    TableMetadata leftSourceTableMetadataBefore =
        admin.getTableMetadata(namespace, LEFT_SOURCE_TABLE);
    assertThat(leftSourceTableMetadataBefore).isNotNull();
    assertThat(leftSourceTableMetadataBefore.getSecondaryIndexNames()).doesNotContain(columnName);

    TableMetadata virtualTableMetadataBefore = admin.getTableMetadata(namespace, VIRTUAL_TABLE);
    assertThat(virtualTableMetadataBefore).isNotNull();
    assertThat(virtualTableMetadataBefore.getSecondaryIndexNames()).doesNotContain(columnName);

    // Act
    admin.createIndex(namespace, VIRTUAL_TABLE, columnName, true);

    // Assert
    TableMetadata leftSourceTableMetadataAfter =
        admin.getTableMetadata(namespace, LEFT_SOURCE_TABLE);
    assertThat(leftSourceTableMetadataAfter).isNotNull();
    assertThat(leftSourceTableMetadataAfter.getSecondaryIndexNames()).contains(columnName);

    TableMetadata virtualTableMetadataAfter = admin.getTableMetadata(namespace, VIRTUAL_TABLE);
    assertThat(virtualTableMetadataAfter).isNotNull();
    assertThat(virtualTableMetadataAfter.getSecondaryIndexNames()).contains(columnName);
  }

  @Test
  public void createIndex_OnRightSourceTableColumn_ShouldCreateIndexSuccessfully()
      throws Exception {
    // Arrange
    String columnName = "col6"; // col6 belongs to right source table

    TableMetadata rightSourceTableMetadataBefore =
        admin.getTableMetadata(namespace, RIGHT_SOURCE_TABLE);
    assertThat(rightSourceTableMetadataBefore).isNotNull();
    assertThat(rightSourceTableMetadataBefore.getSecondaryIndexNames()).doesNotContain(columnName);

    TableMetadata virtualTableMetadataBefore = admin.getTableMetadata(namespace, VIRTUAL_TABLE);
    assertThat(virtualTableMetadataBefore).isNotNull();
    assertThat(virtualTableMetadataBefore.getSecondaryIndexNames()).doesNotContain(columnName);

    // Act
    admin.createIndex(namespace, VIRTUAL_TABLE, columnName, true);

    // Assert
    TableMetadata rightSourceTableMetadataAfter =
        admin.getTableMetadata(namespace, RIGHT_SOURCE_TABLE);
    assertThat(rightSourceTableMetadataAfter).isNotNull();
    assertThat(rightSourceTableMetadataAfter.getSecondaryIndexNames()).contains(columnName);

    TableMetadata virtualTableMetadataAfter = admin.getTableMetadata(namespace, VIRTUAL_TABLE);
    assertThat(virtualTableMetadataAfter).isNotNull();
    assertThat(virtualTableMetadataAfter.getSecondaryIndexNames()).contains(columnName);
  }

  @Test
  public void dropIndex_OnLeftSourceTableColumn_ShouldDropIndexSuccessfully() throws Exception {
    // Arrange
    String columnName = "col1"; // col1 belongs to left source table
    admin.createIndex(namespace, VIRTUAL_TABLE, columnName, true);

    TableMetadata leftSourceTableMetadataBefore =
        admin.getTableMetadata(namespace, LEFT_SOURCE_TABLE);
    assertThat(leftSourceTableMetadataBefore).isNotNull();
    assertThat(leftSourceTableMetadataBefore.getSecondaryIndexNames()).contains(columnName);

    TableMetadata virtualTableMetadataBefore = admin.getTableMetadata(namespace, VIRTUAL_TABLE);
    assertThat(virtualTableMetadataBefore).isNotNull();
    assertThat(virtualTableMetadataBefore.getSecondaryIndexNames()).contains(columnName);

    // Act
    admin.dropIndex(namespace, VIRTUAL_TABLE, columnName);

    // Assert
    TableMetadata leftSourceTableMetadataAfter =
        admin.getTableMetadata(namespace, LEFT_SOURCE_TABLE);
    assertThat(leftSourceTableMetadataAfter).isNotNull();
    assertThat(leftSourceTableMetadataAfter.getSecondaryIndexNames()).doesNotContain(columnName);

    TableMetadata virtualTableMetadataAfter = admin.getTableMetadata(namespace, VIRTUAL_TABLE);
    assertThat(virtualTableMetadataAfter).isNotNull();
    assertThat(virtualTableMetadataAfter.getSecondaryIndexNames()).doesNotContain(columnName);
  }

  @Test
  public void dropIndex_OnRightSourceTableColumn_ShouldDropIndexSuccessfully() throws Exception {
    // Arrange
    String columnName = "col6"; // col6 belongs to right source table
    admin.createIndex(namespace, VIRTUAL_TABLE, columnName, true);

    TableMetadata rightSourceTableMetadataBefore =
        admin.getTableMetadata(namespace, RIGHT_SOURCE_TABLE);
    assertThat(rightSourceTableMetadataBefore).isNotNull();
    assertThat(rightSourceTableMetadataBefore.getSecondaryIndexNames()).contains(columnName);

    TableMetadata virtualTableMetadataBefore = admin.getTableMetadata(namespace, VIRTUAL_TABLE);
    assertThat(virtualTableMetadataBefore).isNotNull();
    assertThat(virtualTableMetadataBefore.getSecondaryIndexNames()).contains(columnName);

    // Act
    admin.dropIndex(namespace, VIRTUAL_TABLE, columnName);

    // Assert
    TableMetadata rightSourceTableMetadataAfter =
        admin.getTableMetadata(namespace, RIGHT_SOURCE_TABLE);
    assertThat(rightSourceTableMetadataAfter).isNotNull();
    assertThat(rightSourceTableMetadataAfter.getSecondaryIndexNames()).doesNotContain(columnName);

    TableMetadata virtualTableMetadataAfter = admin.getTableMetadata(namespace, VIRTUAL_TABLE);
    assertThat(virtualTableMetadataAfter).isNotNull();
    assertThat(virtualTableMetadataAfter.getSecondaryIndexNames()).doesNotContain(columnName);
  }

  @Test
  public void get_ForVirtualTable_ShouldGetProperly() throws Exception {
    // Arrange
    storage.put(
        Put.newBuilder()
            .namespace(namespace)
            .table(VIRTUAL_TABLE)
            .partitionKey(Key.of("pk1", 1, "pk2", "1"))
            .clusteringKey(Key.ofText("ck1", "aaa"))
            .intValue("col1", 100)
            .textValue("col2", "data1")
            .bigIntValue("col3", 1000L)
            .floatValue("col4", 10.5f)
            .booleanValue("col5", true)
            .doubleValue("col6", 20.5)
            .textValue("col7", "data2")
            .intValue("col8", 200)
            .booleanValue("col9", false)
            .bigIntValue("col10", 2000L)
            .build());

    // Act
    Optional<Result> result =
        storage.get(
            Get.newBuilder()
                .namespace(namespace)
                .table(VIRTUAL_TABLE)
                .partitionKey(Key.of("pk1", 1, "pk2", "1"))
                .clusteringKey(Key.ofText("ck1", "aaa"))
                .build());

    // Assert
    assertThat(result).isPresent();
    assertThat(result.get().getInt("pk1")).isEqualTo(1);
    assertThat(result.get().getText("pk2")).isEqualTo("1");
    assertThat(result.get().getText("ck1")).isEqualTo("aaa");
    assertThat(result.get().getInt("col1")).isEqualTo(100);
    assertThat(result.get().getText("col2")).isEqualTo("data1");
    assertThat(result.get().getBigInt("col3")).isEqualTo(1000L);
    assertThat(result.get().getFloat("col4")).isEqualTo(10.5f);
    assertThat(result.get().getBoolean("col5")).isEqualTo(true);
    assertThat(result.get().getDouble("col6")).isEqualTo(20.5);
    assertThat(result.get().getText("col7")).isEqualTo("data2");
    assertThat(result.get().getInt("col8")).isEqualTo(200);
    assertThat(result.get().getBoolean("col9")).isEqualTo(false);
    assertThat(result.get().getBigInt("col10")).isEqualTo(2000L);
  }

  @Test
  public void get_ForVirtualTable_WithProjection_ShouldGetOnlyProjectedColumns() throws Exception {
    // Arrange
    storage.put(
        Put.newBuilder()
            .namespace(namespace)
            .table(VIRTUAL_TABLE)
            .partitionKey(Key.of("pk1", 1, "pk2", "1"))
            .clusteringKey(Key.ofText("ck1", "aaa"))
            .intValue("col1", 100)
            .textValue("col2", "data1")
            .bigIntValue("col3", 1000L)
            .floatValue("col4", 10.5f)
            .booleanValue("col5", true)
            .doubleValue("col6", 20.5)
            .textValue("col7", "data2")
            .intValue("col8", 200)
            .booleanValue("col9", false)
            .bigIntValue("col10", 2000L)
            .build());

    // Act - project only col1, col2 from left table and col6, col7 from right table
    Optional<Result> result =
        storage.get(
            Get.newBuilder()
                .namespace(namespace)
                .table(VIRTUAL_TABLE)
                .partitionKey(Key.of("pk1", 1, "pk2", "1"))
                .clusteringKey(Key.ofText("ck1", "aaa"))
                .projections("col1", "col2", "col6", "col7")
                .build());

    // Assert
    assertThat(result).isPresent();
    assertThat(result.get().getContainedColumnNames()).containsOnly("col1", "col2", "col6", "col7");
    assertThat(result.get().getInt("col1")).isEqualTo(100);
    assertThat(result.get().getText("col2")).isEqualTo("data1");
    assertThat(result.get().getDouble("col6")).isEqualTo(20.5);
    assertThat(result.get().getText("col7")).isEqualTo("data2");
  }

  @Test
  public void scan_ForVirtualTable_ShouldScanProperly() throws Exception {
    // Arrange
    storage.put(
        Put.newBuilder()
            .namespace(namespace)
            .table(VIRTUAL_TABLE)
            .partitionKey(Key.of("pk1", 1, "pk2", "1"))
            .clusteringKey(Key.ofText("ck1", "aaa"))
            .intValue("col1", 100)
            .textValue("col2", "data1")
            .bigIntValue("col3", 1000L)
            .floatValue("col4", 10.5f)
            .booleanValue("col5", true)
            .doubleValue("col6", 20.5)
            .textValue("col7", "data2")
            .intValue("col8", 200)
            .booleanValue("col9", false)
            .bigIntValue("col10", 2000L)
            .build());
    storage.put(
        Put.newBuilder()
            .namespace(namespace)
            .table(VIRTUAL_TABLE)
            .partitionKey(Key.of("pk1", 1, "pk2", "1"))
            .clusteringKey(Key.ofText("ck1", "bbb"))
            .intValue("col1", 101)
            .textValue("col2", "data3")
            .bigIntValue("col3", 1001L)
            .floatValue("col4", 11.5f)
            .booleanValue("col5", false)
            .doubleValue("col6", 21.5)
            .textValue("col7", "data4")
            .intValue("col8", 201)
            .booleanValue("col9", true)
            .bigIntValue("col10", 2001L)
            .build());
    storage.put(
        Put.newBuilder()
            .namespace(namespace)
            .table(VIRTUAL_TABLE)
            .partitionKey(Key.of("pk1", 1, "pk2", "1"))
            .clusteringKey(Key.ofText("ck1", "ccc"))
            .intValue("col1", 102)
            .textValue("col2", "data5")
            .bigIntValue("col3", 1002L)
            .floatValue("col4", 12.5f)
            .booleanValue("col5", true)
            .doubleValue("col6", 22.5)
            .textValue("col7", "data6")
            .intValue("col8", 202)
            .booleanValue("col9", false)
            .bigIntValue("col10", 2002L)
            .build());

    // Act
    Scanner scanner =
        storage.scan(
            Scan.newBuilder()
                .namespace(namespace)
                .table(VIRTUAL_TABLE)
                .partitionKey(Key.of("pk1", 1, "pk2", "1"))
                .build());
    List<Result> results = scanner.all();
    scanner.close();

    // Assert
    assertThat(results).hasSize(3);
    // First record
    assertThat(results.get(0).getInt("pk1")).isEqualTo(1);
    assertThat(results.get(0).getText("pk2")).isEqualTo("1");
    assertThat(results.get(0).getText("ck1")).isEqualTo("aaa");
    assertThat(results.get(0).getInt("col1")).isEqualTo(100);
    assertThat(results.get(0).getText("col2")).isEqualTo("data1");
    assertThat(results.get(0).getBigInt("col3")).isEqualTo(1000L);
    assertThat(results.get(0).getFloat("col4")).isEqualTo(10.5f);
    assertThat(results.get(0).getBoolean("col5")).isEqualTo(true);
    assertThat(results.get(0).getDouble("col6")).isEqualTo(20.5);
    assertThat(results.get(0).getText("col7")).isEqualTo("data2");
    assertThat(results.get(0).getInt("col8")).isEqualTo(200);
    assertThat(results.get(0).getBoolean("col9")).isEqualTo(false);
    assertThat(results.get(0).getBigInt("col10")).isEqualTo(2000L);
    // Second record
    assertThat(results.get(1).getInt("pk1")).isEqualTo(1);
    assertThat(results.get(1).getText("pk2")).isEqualTo("1");
    assertThat(results.get(1).getText("ck1")).isEqualTo("bbb");
    assertThat(results.get(1).getInt("col1")).isEqualTo(101);
    assertThat(results.get(1).getText("col2")).isEqualTo("data3");
    assertThat(results.get(1).getBigInt("col3")).isEqualTo(1001L);
    assertThat(results.get(1).getFloat("col4")).isEqualTo(11.5f);
    assertThat(results.get(1).getBoolean("col5")).isEqualTo(false);
    assertThat(results.get(1).getDouble("col6")).isEqualTo(21.5);
    assertThat(results.get(1).getText("col7")).isEqualTo("data4");
    assertThat(results.get(1).getInt("col8")).isEqualTo(201);
    assertThat(results.get(1).getBoolean("col9")).isEqualTo(true);
    assertThat(results.get(1).getBigInt("col10")).isEqualTo(2001L);
    // Third record
    assertThat(results.get(2).getInt("pk1")).isEqualTo(1);
    assertThat(results.get(2).getText("pk2")).isEqualTo("1");
    assertThat(results.get(2).getText("ck1")).isEqualTo("ccc");
    assertThat(results.get(2).getInt("col1")).isEqualTo(102);
    assertThat(results.get(2).getText("col2")).isEqualTo("data5");
    assertThat(results.get(2).getBigInt("col3")).isEqualTo(1002L);
    assertThat(results.get(2).getFloat("col4")).isEqualTo(12.5f);
    assertThat(results.get(2).getBoolean("col5")).isEqualTo(true);
    assertThat(results.get(2).getDouble("col6")).isEqualTo(22.5);
    assertThat(results.get(2).getText("col7")).isEqualTo("data6");
    assertThat(results.get(2).getInt("col8")).isEqualTo(202);
    assertThat(results.get(2).getBoolean("col9")).isEqualTo(false);
    assertThat(results.get(2).getBigInt("col10")).isEqualTo(2002L);
  }

  @Test
  public void scan_ForVirtualTable_WithProjection_ShouldScanOnlyProjectedColumns()
      throws Exception {
    // Arrange
    storage.put(
        Put.newBuilder()
            .namespace(namespace)
            .table(VIRTUAL_TABLE)
            .partitionKey(Key.of("pk1", 1, "pk2", "1"))
            .clusteringKey(Key.ofText("ck1", "aaa"))
            .intValue("col1", 100)
            .textValue("col2", "data1")
            .bigIntValue("col3", 1000L)
            .floatValue("col4", 10.5f)
            .booleanValue("col5", true)
            .doubleValue("col6", 20.5)
            .textValue("col7", "data2")
            .intValue("col8", 200)
            .booleanValue("col9", false)
            .bigIntValue("col10", 2000L)
            .build());
    storage.put(
        Put.newBuilder()
            .namespace(namespace)
            .table(VIRTUAL_TABLE)
            .partitionKey(Key.of("pk1", 1, "pk2", "1"))
            .clusteringKey(Key.ofText("ck1", "bbb"))
            .intValue("col1", 101)
            .textValue("col2", "data3")
            .bigIntValue("col3", 1001L)
            .floatValue("col4", 11.5f)
            .booleanValue("col5", false)
            .doubleValue("col6", 21.5)
            .textValue("col7", "data4")
            .intValue("col8", 201)
            .booleanValue("col9", true)
            .bigIntValue("col10", 2001L)
            .build());

    // Act - project only col1, col5 from left table and col8, col9 from right table
    Scanner scanner =
        storage.scan(
            Scan.newBuilder()
                .namespace(namespace)
                .table(VIRTUAL_TABLE)
                .partitionKey(Key.of("pk1", 1, "pk2", "1"))
                .projections("col1", "col5", "col8", "col9")
                .build());
    List<Result> results = scanner.all();
    scanner.close();

    // Assert
    assertThat(results).hasSize(2);
    // First record
    assertThat(results.get(0).getContainedColumnNames())
        .containsOnly("col1", "col5", "col8", "col9");
    assertThat(results.get(0).getInt("col1")).isEqualTo(100);
    assertThat(results.get(0).getBoolean("col5")).isEqualTo(true);
    assertThat(results.get(0).getInt("col8")).isEqualTo(200);
    assertThat(results.get(0).getBoolean("col9")).isEqualTo(false);
    // Second record
    assertThat(results.get(1).getContainedColumnNames())
        .containsOnly("col1", "col5", "col8", "col9");
    assertThat(results.get(1).getInt("col1")).isEqualTo(101);
    assertThat(results.get(1).getBoolean("col5")).isEqualTo(false);
    assertThat(results.get(1).getInt("col8")).isEqualTo(201);
    assertThat(results.get(1).getBoolean("col9")).isEqualTo(true);
  }

  @Test
  public void put_ForVirtualTable_ShouldStoreProperly() throws Exception {
    // Arrange
    Put put =
        Put.newBuilder()
            .namespace(namespace)
            .table(VIRTUAL_TABLE)
            .partitionKey(Key.of("pk1", 1, "pk2", "1"))
            .clusteringKey(Key.ofText("ck1", "aaa"))
            .intValue("col1", 100)
            .textValue("col2", "data1")
            .bigIntValue("col3", 1000L)
            .floatValue("col4", 10.5f)
            .booleanValue("col5", true)
            .doubleValue("col6", 20.5)
            .textValue("col7", "data2")
            .intValue("col8", 200)
            .booleanValue("col9", false)
            .bigIntValue("col10", 2000L)
            .build();

    // Act
    storage.put(put);

    // Assert
    Optional<Result> result =
        storage.get(
            Get.newBuilder()
                .namespace(namespace)
                .table(VIRTUAL_TABLE)
                .partitionKey(Key.of("pk1", 1, "pk2", "1"))
                .clusteringKey(Key.ofText("ck1", "aaa"))
                .build());
    assertThat(result).isPresent();
    assertThat(result.get().getInt("pk1")).isEqualTo(1);
    assertThat(result.get().getText("pk2")).isEqualTo("1");
    assertThat(result.get().getText("ck1")).isEqualTo("aaa");
    assertThat(result.get().getInt("col1")).isEqualTo(100);
    assertThat(result.get().getText("col2")).isEqualTo("data1");
    assertThat(result.get().getBigInt("col3")).isEqualTo(1000L);
    assertThat(result.get().getFloat("col4")).isEqualTo(10.5f);
    assertThat(result.get().getBoolean("col5")).isEqualTo(true);
    assertThat(result.get().getDouble("col6")).isEqualTo(20.5);
    assertThat(result.get().getText("col7")).isEqualTo("data2");
    assertThat(result.get().getInt("col8")).isEqualTo(200);
    assertThat(result.get().getBoolean("col9")).isEqualTo(false);
    assertThat(result.get().getBigInt("col10")).isEqualTo(2000L);
  }

  @Test
  public void delete_ForVirtualTable_ShouldDeleteProperly() throws Exception {
    // Arrange
    storage.put(
        Put.newBuilder()
            .namespace(namespace)
            .table(VIRTUAL_TABLE)
            .partitionKey(Key.of("pk1", 1, "pk2", "1"))
            .clusteringKey(Key.ofText("ck1", "aaa"))
            .intValue("col1", 100)
            .textValue("col2", "data1")
            .bigIntValue("col3", 1000L)
            .floatValue("col4", 10.5f)
            .booleanValue("col5", true)
            .doubleValue("col6", 20.5)
            .textValue("col7", "data2")
            .intValue("col8", 200)
            .booleanValue("col9", false)
            .bigIntValue("col10", 2000L)
            .build());

    // Act
    storage.delete(
        Delete.newBuilder()
            .namespace(namespace)
            .table(VIRTUAL_TABLE)
            .partitionKey(Key.of("pk1", 1, "pk2", "1"))
            .clusteringKey(Key.ofText("ck1", "aaa"))
            .build());

    // Assert
    Optional<Result> result =
        storage.get(
            Get.newBuilder()
                .namespace(namespace)
                .table(VIRTUAL_TABLE)
                .partitionKey(Key.of("pk1", 1, "pk2", "1"))
                .clusteringKey(Key.ofText("ck1", "aaa"))
                .build());
    assertThat(result).isEmpty();
  }

  @Test
  public void mutate_ForVirtualTable_ShouldMutateProperly() throws Exception {
    // Arrange
    storage.put(
        Put.newBuilder()
            .namespace(namespace)
            .table(VIRTUAL_TABLE)
            .partitionKey(Key.of("pk1", 1, "pk2", "1"))
            .clusteringKey(Key.ofText("ck1", "aaa"))
            .intValue("col1", 100)
            .textValue("col2", "data1")
            .bigIntValue("col3", 1000L)
            .floatValue("col4", 10.5f)
            .booleanValue("col5", true)
            .doubleValue("col6", 20.5)
            .textValue("col7", "data2")
            .intValue("col8", 200)
            .booleanValue("col9", false)
            .bigIntValue("col10", 2000L)
            .build());

    // Act
    storage.mutate(
        Arrays.asList(
            Put.newBuilder()
                .namespace(namespace)
                .table(VIRTUAL_TABLE)
                .partitionKey(Key.of("pk1", 2, "pk2", "2"))
                .clusteringKey(Key.ofText("ck1", "bbb"))
                .intValue("col1", 101)
                .textValue("col2", "data3")
                .bigIntValue("col3", 1001L)
                .floatValue("col4", 11.5f)
                .booleanValue("col5", false)
                .doubleValue("col6", 21.5)
                .textValue("col7", "data4")
                .intValue("col8", 201)
                .booleanValue("col9", true)
                .bigIntValue("col10", 2001L)
                .build(),
            Delete.newBuilder()
                .namespace(namespace)
                .table(VIRTUAL_TABLE)
                .partitionKey(Key.of("pk1", 1, "pk2", "1"))
                .clusteringKey(Key.ofText("ck1", "aaa"))
                .build()));

    // Assert
    Optional<Result> result1 =
        storage.get(
            Get.newBuilder()
                .namespace(namespace)
                .table(VIRTUAL_TABLE)
                .partitionKey(Key.of("pk1", 2, "pk2", "2"))
                .clusteringKey(Key.ofText("ck1", "bbb"))
                .build());
    assertThat(result1).isPresent();
    assertThat(result1.get().getInt("pk1")).isEqualTo(2);
    assertThat(result1.get().getText("pk2")).isEqualTo("2");
    assertThat(result1.get().getText("ck1")).isEqualTo("bbb");
    assertThat(result1.get().getInt("col1")).isEqualTo(101);
    assertThat(result1.get().getText("col2")).isEqualTo("data3");
    assertThat(result1.get().getBigInt("col3")).isEqualTo(1001L);
    assertThat(result1.get().getFloat("col4")).isEqualTo(11.5f);
    assertThat(result1.get().getBoolean("col5")).isEqualTo(false);
    assertThat(result1.get().getDouble("col6")).isEqualTo(21.5);
    assertThat(result1.get().getText("col7")).isEqualTo("data4");
    assertThat(result1.get().getInt("col8")).isEqualTo(201);
    assertThat(result1.get().getBoolean("col9")).isEqualTo(true);
    assertThat(result1.get().getBigInt("col10")).isEqualTo(2001L);

    Optional<Result> result2 =
        storage.get(
            Get.newBuilder()
                .namespace(namespace)
                .table(VIRTUAL_TABLE)
                .partitionKey(Key.of("pk1", 1, "pk2", "1"))
                .clusteringKey(Key.ofText("ck1", "aaa"))
                .build());
    assertThat(result2).isEmpty();
  }

  @Test
  public void putIfExists_ForVirtualTable_ShouldUpdateProperly() throws Exception {
    // Arrange
    storage.put(
        Put.newBuilder()
            .namespace(namespace)
            .table(VIRTUAL_TABLE)
            .partitionKey(Key.of("pk1", 1, "pk2", "1"))
            .clusteringKey(Key.ofText("ck1", "aaa"))
            .intValue("col1", 100)
            .textValue("col2", "data1")
            .bigIntValue("col3", 1000L)
            .floatValue("col4", 10.5f)
            .booleanValue("col5", true)
            .doubleValue("col6", 20.5)
            .textValue("col7", "data2")
            .intValue("col8", 200)
            .booleanValue("col9", false)
            .bigIntValue("col10", 2000L)
            .build());

    // Act
    storage.put(
        Put.newBuilder()
            .namespace(namespace)
            .table(VIRTUAL_TABLE)
            .partitionKey(Key.of("pk1", 1, "pk2", "1"))
            .clusteringKey(Key.ofText("ck1", "aaa"))
            .intValue("col1", 111)
            .textValue("col2", "updated1")
            .bigIntValue("col3", 1111L)
            .floatValue("col4", 11.5f)
            .booleanValue("col5", false)
            .doubleValue("col6", 21.5)
            .textValue("col7", "updated2")
            .intValue("col8", 211)
            .booleanValue("col9", true)
            .bigIntValue("col10", 2111L)
            .condition(ConditionBuilder.putIfExists())
            .build());

    // Assert
    Optional<Result> result =
        storage.get(
            Get.newBuilder()
                .namespace(namespace)
                .table(VIRTUAL_TABLE)
                .partitionKey(Key.of("pk1", 1, "pk2", "1"))
                .clusteringKey(Key.ofText("ck1", "aaa"))
                .build());
    assertThat(result).isPresent();
    assertThat(result.get().getInt("col1")).isEqualTo(111);
    assertThat(result.get().getText("col2")).isEqualTo("updated1");
    assertThat(result.get().getBigInt("col3")).isEqualTo(1111L);
    assertThat(result.get().getFloat("col4")).isEqualTo(11.5f);
    assertThat(result.get().getBoolean("col5")).isEqualTo(false);
    assertThat(result.get().getDouble("col6")).isEqualTo(21.5);
    assertThat(result.get().getText("col7")).isEqualTo("updated2");
    assertThat(result.get().getInt("col8")).isEqualTo(211);
    assertThat(result.get().getBoolean("col9")).isEqualTo(true);
    assertThat(result.get().getBigInt("col10")).isEqualTo(2111L);
  }

  @Test
  public void putIfNotExists_ForVirtualTable_ShouldInsertProperly() throws Exception {
    // Arrange - no existing data

    // Act
    storage.put(
        Put.newBuilder()
            .namespace(namespace)
            .table(VIRTUAL_TABLE)
            .partitionKey(Key.of("pk1", 1, "pk2", "1"))
            .clusteringKey(Key.ofText("ck1", "aaa"))
            .intValue("col1", 100)
            .textValue("col2", "data1")
            .bigIntValue("col3", 1000L)
            .floatValue("col4", 10.5f)
            .booleanValue("col5", true)
            .doubleValue("col6", 20.5)
            .textValue("col7", "data2")
            .intValue("col8", 200)
            .booleanValue("col9", false)
            .bigIntValue("col10", 2000L)
            .condition(ConditionBuilder.putIfNotExists())
            .build());

    // Assert
    Optional<Result> result =
        storage.get(
            Get.newBuilder()
                .namespace(namespace)
                .table(VIRTUAL_TABLE)
                .partitionKey(Key.of("pk1", 1, "pk2", "1"))
                .clusteringKey(Key.ofText("ck1", "aaa"))
                .build());
    assertThat(result).isPresent();
    assertThat(result.get().getInt("col1")).isEqualTo(100);
    assertThat(result.get().getText("col2")).isEqualTo("data1");
    assertThat(result.get().getBigInt("col3")).isEqualTo(1000L);
    assertThat(result.get().getFloat("col4")).isEqualTo(10.5f);
    assertThat(result.get().getBoolean("col5")).isEqualTo(true);
    assertThat(result.get().getDouble("col6")).isEqualTo(20.5);
    assertThat(result.get().getText("col7")).isEqualTo("data2");
    assertThat(result.get().getInt("col8")).isEqualTo(200);
    assertThat(result.get().getBoolean("col9")).isEqualTo(false);
    assertThat(result.get().getBigInt("col10")).isEqualTo(2000L);
  }

  @Test
  public void putIf_ForVirtualTable_ShouldUpdateConditionally() throws Exception {
    // Arrange
    storage.put(
        Put.newBuilder()
            .namespace(namespace)
            .table(VIRTUAL_TABLE)
            .partitionKey(Key.of("pk1", 1, "pk2", "1"))
            .clusteringKey(Key.ofText("ck1", "aaa"))
            .intValue("col1", 100)
            .textValue("col2", "data1")
            .bigIntValue("col3", 1000L)
            .floatValue("col4", 10.5f)
            .booleanValue("col5", true)
            .doubleValue("col6", 20.5)
            .textValue("col7", "data2")
            .intValue("col8", 200)
            .booleanValue("col9", false)
            .bigIntValue("col10", 2000L)
            .build());

    // Act
    storage.put(
        Put.newBuilder()
            .namespace(namespace)
            .table(VIRTUAL_TABLE)
            .partitionKey(Key.of("pk1", 1, "pk2", "1"))
            .clusteringKey(Key.ofText("ck1", "aaa"))
            .intValue("col1", 111)
            .textValue("col2", "updated1")
            .bigIntValue("col3", 1111L)
            .floatValue("col4", 11.5f)
            .booleanValue("col5", false)
            .doubleValue("col6", 21.5)
            .textValue("col7", "updated2")
            .intValue("col8", 211)
            .booleanValue("col9", true)
            .bigIntValue("col10", 2111L)
            .condition(
                ConditionBuilder.putIf(ConditionBuilder.column("col1").isEqualToInt(100))
                    .and(ConditionBuilder.column("col5").isEqualToBoolean(true))
                    .build())
            .build());

    // Assert
    Optional<Result> result =
        storage.get(
            Get.newBuilder()
                .namespace(namespace)
                .table(VIRTUAL_TABLE)
                .partitionKey(Key.of("pk1", 1, "pk2", "1"))
                .clusteringKey(Key.ofText("ck1", "aaa"))
                .build());
    assertThat(result).isPresent();
    assertThat(result.get().getInt("col1")).isEqualTo(111);
    assertThat(result.get().getText("col2")).isEqualTo("updated1");
    assertThat(result.get().getBigInt("col3")).isEqualTo(1111L);
    assertThat(result.get().getFloat("col4")).isEqualTo(11.5f);
    assertThat(result.get().getBoolean("col5")).isEqualTo(false);
    assertThat(result.get().getDouble("col6")).isEqualTo(21.5);
    assertThat(result.get().getText("col7")).isEqualTo("updated2");
    assertThat(result.get().getInt("col8")).isEqualTo(211);
    assertThat(result.get().getBoolean("col9")).isEqualTo(true);
    assertThat(result.get().getBigInt("col10")).isEqualTo(2111L);
  }

  @Test
  public void putIfExists_ForVirtualTable_WhenRecordDoesNotExist_ShouldThrowNoMutationException()
      throws Exception {
    // Arrange - no existing data

    // Act Assert
    assertThatThrownBy(
            () ->
                storage.put(
                    Put.newBuilder()
                        .namespace(namespace)
                        .table(VIRTUAL_TABLE)
                        .partitionKey(Key.of("pk1", 1, "pk2", "1"))
                        .clusteringKey(Key.ofText("ck1", "aaa"))
                        .intValue("col1", 100)
                        .textValue("col2", "data1")
                        .bigIntValue("col3", 1000L)
                        .floatValue("col4", 10.5f)
                        .booleanValue("col5", true)
                        .doubleValue("col6", 20.5)
                        .textValue("col7", "data2")
                        .intValue("col8", 200)
                        .booleanValue("col9", false)
                        .bigIntValue("col10", 2000L)
                        .condition(ConditionBuilder.putIfExists())
                        .build()))
        .isInstanceOf(NoMutationException.class);

    Optional<Result> result =
        storage.get(
            Get.newBuilder()
                .namespace(namespace)
                .table(VIRTUAL_TABLE)
                .partitionKey(Key.of("pk1", 1, "pk2", "1"))
                .clusteringKey(Key.ofText("ck1", "aaa"))
                .build());
    assertThat(result).isEmpty();
  }

  @Test
  public void putIfNotExists_ForVirtualTable_WhenRecordExists_ShouldThrowNoMutationException()
      throws Exception {
    // Arrange
    storage.put(
        Put.newBuilder()
            .namespace(namespace)
            .table(VIRTUAL_TABLE)
            .partitionKey(Key.of("pk1", 1, "pk2", "1"))
            .clusteringKey(Key.ofText("ck1", "aaa"))
            .intValue("col1", 100)
            .textValue("col2", "data1")
            .bigIntValue("col3", 1000L)
            .floatValue("col4", 10.5f)
            .booleanValue("col5", true)
            .doubleValue("col6", 20.5)
            .textValue("col7", "data2")
            .intValue("col8", 200)
            .booleanValue("col9", false)
            .bigIntValue("col10", 2000L)
            .build());

    // Act Assert
    assertThatThrownBy(
            () ->
                storage.put(
                    Put.newBuilder()
                        .namespace(namespace)
                        .table(VIRTUAL_TABLE)
                        .partitionKey(Key.of("pk1", 1, "pk2", "1"))
                        .clusteringKey(Key.ofText("ck1", "aaa"))
                        .intValue("col1", 111)
                        .textValue("col2", "updated1")
                        .bigIntValue("col3", 1111L)
                        .floatValue("col4", 11.5f)
                        .booleanValue("col5", false)
                        .doubleValue("col6", 21.5)
                        .textValue("col7", "updated2")
                        .intValue("col8", 211)
                        .booleanValue("col9", true)
                        .bigIntValue("col10", 2111L)
                        .condition(ConditionBuilder.putIfNotExists())
                        .build()))
        .isInstanceOf(NoMutationException.class);

    // Verify original data remains unchanged
    Optional<Result> result =
        storage.get(
            Get.newBuilder()
                .namespace(namespace)
                .table(VIRTUAL_TABLE)
                .partitionKey(Key.of("pk1", 1, "pk2", "1"))
                .clusteringKey(Key.ofText("ck1", "aaa"))
                .build());
    assertThat(result).isPresent();
    assertThat(result.get().getInt("col1")).isEqualTo(100);
    assertThat(result.get().getText("col2")).isEqualTo("data1");
  }

  @Test
  public void putIf_ForVirtualTable_WhenConditionNotMet_ShouldThrowNoMutationException()
      throws Exception {
    // Arrange
    storage.put(
        Put.newBuilder()
            .namespace(namespace)
            .table(VIRTUAL_TABLE)
            .partitionKey(Key.of("pk1", 1, "pk2", "1"))
            .clusteringKey(Key.ofText("ck1", "aaa"))
            .intValue("col1", 100)
            .textValue("col2", "data1")
            .bigIntValue("col3", 1000L)
            .floatValue("col4", 10.5f)
            .booleanValue("col5", true)
            .doubleValue("col6", 20.5)
            .textValue("col7", "data2")
            .intValue("col8", 200)
            .booleanValue("col9", false)
            .bigIntValue("col10", 2000L)
            .build());

    // Act Assert - condition col1 == 999 should fail
    assertThatThrownBy(
            () ->
                storage.put(
                    Put.newBuilder()
                        .namespace(namespace)
                        .table(VIRTUAL_TABLE)
                        .partitionKey(Key.of("pk1", 1, "pk2", "1"))
                        .clusteringKey(Key.ofText("ck1", "aaa"))
                        .intValue("col1", 111)
                        .textValue("col2", "updated1")
                        .bigIntValue("col3", 1111L)
                        .floatValue("col4", 11.5f)
                        .booleanValue("col5", false)
                        .doubleValue("col6", 21.5)
                        .textValue("col7", "updated2")
                        .intValue("col8", 211)
                        .booleanValue("col9", true)
                        .bigIntValue("col10", 2111L)
                        .condition(
                            ConditionBuilder.putIf(
                                    ConditionBuilder.column("col1").isEqualToInt(999))
                                .build())
                        .build()))
        .isInstanceOf(NoMutationException.class);

    // Verify original data remains unchanged
    Optional<Result> result =
        storage.get(
            Get.newBuilder()
                .namespace(namespace)
                .table(VIRTUAL_TABLE)
                .partitionKey(Key.of("pk1", 1, "pk2", "1"))
                .clusteringKey(Key.ofText("ck1", "aaa"))
                .build());
    assertThat(result).isPresent();
    assertThat(result.get().getInt("col1")).isEqualTo(100);
    assertThat(result.get().getText("col2")).isEqualTo("data1");
  }

  @Test
  public void deleteIfExists_ForVirtualTable_ShouldDeleteProperly() throws Exception {
    // Arrange
    storage.put(
        Put.newBuilder()
            .namespace(namespace)
            .table(VIRTUAL_TABLE)
            .partitionKey(Key.of("pk1", 1, "pk2", "1"))
            .clusteringKey(Key.ofText("ck1", "aaa"))
            .intValue("col1", 100)
            .textValue("col2", "data1")
            .bigIntValue("col3", 1000L)
            .floatValue("col4", 10.5f)
            .booleanValue("col5", true)
            .doubleValue("col6", 20.5)
            .textValue("col7", "data2")
            .intValue("col8", 200)
            .booleanValue("col9", false)
            .bigIntValue("col10", 2000L)
            .build());

    // Act
    storage.delete(
        Delete.newBuilder()
            .namespace(namespace)
            .table(VIRTUAL_TABLE)
            .partitionKey(Key.of("pk1", 1, "pk2", "1"))
            .clusteringKey(Key.ofText("ck1", "aaa"))
            .condition(ConditionBuilder.deleteIfExists())
            .build());

    // Assert
    Optional<Result> result =
        storage.get(
            Get.newBuilder()
                .namespace(namespace)
                .table(VIRTUAL_TABLE)
                .partitionKey(Key.of("pk1", 1, "pk2", "1"))
                .clusteringKey(Key.ofText("ck1", "aaa"))
                .build());
    assertThat(result).isEmpty();
  }

  @Test
  public void deleteIf_ForVirtualTable_ShouldDeleteConditionally() throws Exception {
    // Arrange
    storage.put(
        Put.newBuilder()
            .namespace(namespace)
            .table(VIRTUAL_TABLE)
            .partitionKey(Key.of("pk1", 1, "pk2", "1"))
            .clusteringKey(Key.ofText("ck1", "aaa"))
            .intValue("col1", 100)
            .textValue("col2", "data1")
            .bigIntValue("col3", 1000L)
            .floatValue("col4", 10.5f)
            .booleanValue("col5", true)
            .doubleValue("col6", 20.5)
            .textValue("col7", "data2")
            .intValue("col8", 200)
            .booleanValue("col9", false)
            .bigIntValue("col10", 2000L)
            .build());

    // Act
    storage.delete(
        Delete.newBuilder()
            .namespace(namespace)
            .table(VIRTUAL_TABLE)
            .partitionKey(Key.of("pk1", 1, "pk2", "1"))
            .clusteringKey(Key.ofText("ck1", "aaa"))
            .condition(
                ConditionBuilder.deleteIf(ConditionBuilder.column("col1").isEqualToInt(100))
                    .and(ConditionBuilder.column("col5").isEqualToBoolean(true))
                    .build())
            .build());

    // Assert
    Optional<Result> result =
        storage.get(
            Get.newBuilder()
                .namespace(namespace)
                .table(VIRTUAL_TABLE)
                .partitionKey(Key.of("pk1", 1, "pk2", "1"))
                .clusteringKey(Key.ofText("ck1", "aaa"))
                .build());
    assertThat(result).isEmpty();
  }

  @Test
  public void deleteIfExists_ForVirtualTable_WhenRecordDoesNotExist_ShouldThrowNoMutationException()
      throws Exception {
    // Arrange - no existing data

    // Act Assert
    assertThatThrownBy(
            () ->
                storage.delete(
                    Delete.newBuilder()
                        .namespace(namespace)
                        .table(VIRTUAL_TABLE)
                        .partitionKey(Key.of("pk1", 1, "pk2", "1"))
                        .clusteringKey(Key.ofText("ck1", "aaa"))
                        .condition(ConditionBuilder.deleteIfExists())
                        .build()))
        .isInstanceOf(NoMutationException.class);

    Optional<Result> result =
        storage.get(
            Get.newBuilder()
                .namespace(namespace)
                .table(VIRTUAL_TABLE)
                .partitionKey(Key.of("pk1", 1, "pk2", "1"))
                .clusteringKey(Key.ofText("ck1", "aaa"))
                .build());
    assertThat(result).isEmpty();
  }

  @Test
  public void deleteIf_ForVirtualTable_WhenConditionNotMet_ShouldThrowNoMutationException()
      throws Exception {
    // Arrange
    storage.put(
        Put.newBuilder()
            .namespace(namespace)
            .table(VIRTUAL_TABLE)
            .partitionKey(Key.of("pk1", 1, "pk2", "1"))
            .clusteringKey(Key.ofText("ck1", "aaa"))
            .intValue("col1", 100)
            .textValue("col2", "data1")
            .bigIntValue("col3", 1000L)
            .floatValue("col4", 10.5f)
            .booleanValue("col5", true)
            .doubleValue("col6", 20.5)
            .textValue("col7", "data2")
            .intValue("col8", 200)
            .booleanValue("col9", false)
            .bigIntValue("col10", 2000L)
            .build());

    // Act Assert - condition col1 == 999 should fail
    assertThatThrownBy(
            () ->
                storage.delete(
                    Delete.newBuilder()
                        .namespace(namespace)
                        .table(VIRTUAL_TABLE)
                        .partitionKey(Key.of("pk1", 1, "pk2", "1"))
                        .clusteringKey(Key.ofText("ck1", "aaa"))
                        .condition(
                            ConditionBuilder.deleteIf(
                                    ConditionBuilder.column("col1").isEqualToInt(999))
                                .build())
                        .build()))
        .isInstanceOf(NoMutationException.class);

    // Verify original data remains unchanged
    Optional<Result> result =
        storage.get(
            Get.newBuilder()
                .namespace(namespace)
                .table(VIRTUAL_TABLE)
                .partitionKey(Key.of("pk1", 1, "pk2", "1"))
                .clusteringKey(Key.ofText("ck1", "aaa"))
                .build());
    assertThat(result).isPresent();
    assertThat(result.get().getInt("col1")).isEqualTo(100);
  }
}
