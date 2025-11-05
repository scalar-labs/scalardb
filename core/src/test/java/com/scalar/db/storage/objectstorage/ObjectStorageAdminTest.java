package com.scalar.db.storage.objectstorage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.assertj.core.util.Sets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class ObjectStorageAdminTest {
  private static final String METADATA_NAMESPACE = "scalardb";

  @Mock private ObjectStorageWrapper wrapper;
  @Mock private ObjectStorageConfig config;
  private ObjectStorageAdmin admin;

  @Captor private ArgumentCaptor<String> objectKeyCaptor;
  @Captor private ArgumentCaptor<String> payloadCaptor;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    when(config.getMetadataNamespace()).thenReturn(METADATA_NAMESPACE);
    admin = new ObjectStorageAdmin(wrapper, config);
  }

  @Test
  public void getTableMetadata_ShouldReturnCorrectTableMetadata() throws Exception {
    // Arrange
    String namespace = "ns";
    String table = "table";
    String tableMetadataKey = namespace + ObjectStorageUtils.CONCATENATED_KEY_DELIMITER + table;
    String objectKey =
        ObjectStorageUtils.getObjectKey(
            METADATA_NAMESPACE, ObjectStorageAdmin.TABLE_METADATA_TABLE);

    Map<String, String> columnsMap =
        new ImmutableMap.Builder<String, String>()
            .put("c1", "int")
            .put("c2", "text")
            .put("c3", "bigint")
            .put("c4", "boolean")
            .put("c5", "blob")
            .put("c6", "float")
            .put("c7", "double")
            .put("c8", "date")
            .put("c9", "time")
            .put("c10", "timestamp")
            .put("c11", "timestamptz")
            .build();

    LinkedHashSet<String> partitionKeyNames = Sets.newLinkedHashSet("c1");
    LinkedHashSet<String> clusteringKeyNames = Sets.newLinkedHashSet("c2", "c3");
    Map<String, String> clusteringOrders = ImmutableMap.of("c2", "ASC", "c3", "DESC");

    ObjectStorageTableMetadata objectStorageTableMetadata =
        ObjectStorageTableMetadata.newBuilder()
            .partitionKeyNames(partitionKeyNames)
            .clusteringKeyNames(clusteringKeyNames)
            .clusteringOrders(clusteringOrders)
            .secondaryIndexNames(Collections.emptySet())
            .columns(columnsMap)
            .build();

    Map<String, ObjectStorageTableMetadata> metadataTable = new HashMap<>();
    metadataTable.put(tableMetadataKey, objectStorageTableMetadata);
    String serializedMetadata = Serializer.serialize(metadataTable);
    ObjectStorageWrapperResponse response =
        new ObjectStorageWrapperResponse(serializedMetadata, "version1");

    when(wrapper.get(objectKey)).thenReturn(Optional.of(response));

    // Act
    TableMetadata actual = admin.getTableMetadata(namespace, table);

    // Assert
    assertThat(actual)
        .isEqualTo(
            TableMetadata.newBuilder()
                .addPartitionKey("c1")
                .addClusteringKey("c2", Order.ASC)
                .addClusteringKey("c3", Order.DESC)
                .addColumn("c1", DataType.INT)
                .addColumn("c2", DataType.TEXT)
                .addColumn("c3", DataType.BIGINT)
                .addColumn("c4", DataType.BOOLEAN)
                .addColumn("c5", DataType.BLOB)
                .addColumn("c6", DataType.FLOAT)
                .addColumn("c7", DataType.DOUBLE)
                .addColumn("c8", DataType.DATE)
                .addColumn("c9", DataType.TIME)
                .addColumn("c10", DataType.TIMESTAMP)
                .addColumn("c11", DataType.TIMESTAMPTZ)
                .build());
  }

  @Test
  public void unsupportedOperations_ShouldThrowUnsupportedException() {
    // Arrange
    String namespace = "sample_ns";
    String table = "tbl";
    String column = "col";

    // Act
    Throwable thrown1 =
        catchThrowable(() -> admin.createIndex(namespace, table, column, Collections.emptyMap()));
    Throwable thrown2 = catchThrowable(() -> admin.dropIndex(namespace, table, column));
    Throwable thrown3 =
        catchThrowable(
            () -> admin.getImportTableMetadata(namespace, table, Collections.emptyMap()));
    Throwable thrown4 =
        catchThrowable(() -> admin.addRawColumnToTable(namespace, table, column, DataType.INT));
    Throwable thrown5 =
        catchThrowable(
            () ->
                admin.importTable(
                    namespace, table, Collections.emptyMap(), Collections.emptyMap()));
    Throwable thrown6 = catchThrowable(() -> admin.dropColumnFromTable(namespace, table, column));
    Throwable thrown7 =
        catchThrowable(() -> admin.renameColumn(namespace, table, column, "newCol"));
    Throwable thrown8 = catchThrowable(() -> admin.renameTable(namespace, table, "newTable"));
    Throwable thrown9 =
        catchThrowable(() -> admin.alterColumnType(namespace, table, column, DataType.INT));

    // Assert
    assertThat(thrown1).isInstanceOf(UnsupportedOperationException.class);
    assertThat(thrown2).isInstanceOf(UnsupportedOperationException.class);
    assertThat(thrown3).isInstanceOf(UnsupportedOperationException.class);
    assertThat(thrown4).isInstanceOf(UnsupportedOperationException.class);
    assertThat(thrown5).isInstanceOf(UnsupportedOperationException.class);
    assertThat(thrown6).isInstanceOf(UnsupportedOperationException.class);
    assertThat(thrown7).isInstanceOf(UnsupportedOperationException.class);
    assertThat(thrown8).isInstanceOf(UnsupportedOperationException.class);
    assertThat(thrown9).isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void getNamespaceNames_ShouldWorkProperly() throws Exception {
    // Arrange
    Map<String, ObjectStorageNamespaceMetadata> namespaceMetadataTable = new HashMap<>();
    namespaceMetadataTable.put("ns1", new ObjectStorageNamespaceMetadata("ns1"));
    namespaceMetadataTable.put("ns2", new ObjectStorageNamespaceMetadata("ns2"));
    String serializedMetadata = Serializer.serialize(namespaceMetadataTable);
    ObjectStorageWrapperResponse response =
        new ObjectStorageWrapperResponse(serializedMetadata, "version1");

    when(wrapper.get(
            ObjectStorageUtils.getObjectKey(
                METADATA_NAMESPACE, ObjectStorageAdmin.NAMESPACE_METADATA_TABLE)))
        .thenReturn(Optional.of(response));

    // Act
    Set<String> actualNamespaces = admin.getNamespaceNames();

    // Assert
    assertThat(actualNamespaces).containsExactlyInAnyOrder("ns1", "ns2");
  }

  @Test
  public void getNamespaceNames_NamespaceMetadataTableDoesNotExist_ShouldReturnEmptySet()
      throws Exception {
    // Arrange
    when(wrapper.get(
            ObjectStorageUtils.getObjectKey(
                METADATA_NAMESPACE, ObjectStorageAdmin.NAMESPACE_METADATA_TABLE)))
        .thenReturn(Optional.empty());

    // Act
    Set<String> actualNamespaces = admin.getNamespaceNames();

    // Assert
    assertThat(actualNamespaces).isEmpty();
  }

  @Test
  public void namespaceExists_WithExistingNamespace_ShouldReturnTrue() throws Exception {
    // Arrange
    String namespace = "ns";
    Map<String, ObjectStorageNamespaceMetadata> metadataTable = new HashMap<>();
    metadataTable.put(namespace, new ObjectStorageNamespaceMetadata(namespace));
    String serializedMetadata = Serializer.serialize(metadataTable);
    ObjectStorageWrapperResponse response =
        new ObjectStorageWrapperResponse(serializedMetadata, "version1");

    when(wrapper.get(
            ObjectStorageUtils.getObjectKey(
                METADATA_NAMESPACE, ObjectStorageAdmin.NAMESPACE_METADATA_TABLE)))
        .thenReturn(Optional.of(response));

    // Act & Assert
    assertThat(admin.namespaceExists(namespace)).isTrue();
  }

  @Test
  public void namespaceExists_WithNonExistingNamespace_ShouldReturnFalse() throws Exception {
    // Arrange
    when(wrapper.get(
            ObjectStorageUtils.getObjectKey(
                METADATA_NAMESPACE, ObjectStorageAdmin.NAMESPACE_METADATA_TABLE)))
        .thenReturn(Optional.empty());

    // Act & Assert
    assertThat(admin.namespaceExists("ns")).isFalse();
  }

  @Test
  public void namespaceExists_WithMetadataNamespace_ShouldReturnTrue() throws Exception {
    // Act & Assert
    assertThat(admin.namespaceExists(METADATA_NAMESPACE)).isTrue();
  }

  @Test
  public void createNamespace_ShouldInsertNamespaceMetadata() throws Exception {
    // Arrange
    String namespace = "ns";
    String expectedObjectKey =
        ObjectStorageUtils.getObjectKey(
            METADATA_NAMESPACE, ObjectStorageAdmin.NAMESPACE_METADATA_TABLE);

    when(wrapper.get(expectedObjectKey)).thenReturn(Optional.empty());

    // Act
    admin.createNamespace(namespace, Collections.emptyMap());

    // Assert
    verify(wrapper).insert(eq(expectedObjectKey), payloadCaptor.capture());

    Map<String, ObjectStorageNamespaceMetadata> insertedMetadata =
        Serializer.deserialize(
            payloadCaptor.getValue(),
            new TypeReference<Map<String, ObjectStorageNamespaceMetadata>>() {});
    assertThat(insertedMetadata).containsKey(namespace);
    assertThat(insertedMetadata.get(namespace).getName()).isEqualTo(namespace);
  }

  @Test
  public void createTable_ShouldInsertTableMetadata() throws Exception {
    // Arrange
    String namespace = "ns";
    String table = "sample_table";
    TableMetadata metadata =
        TableMetadata.newBuilder()
            .addPartitionKey("c3")
            .addClusteringKey("c1", Order.DESC)
            .addClusteringKey("c2", Order.ASC)
            .addColumn("c1", DataType.TEXT)
            .addColumn("c2", DataType.BIGINT)
            .addColumn("c3", DataType.BOOLEAN)
            .build();
    String expectedObjectKey =
        ObjectStorageUtils.getObjectKey(
            METADATA_NAMESPACE, ObjectStorageAdmin.TABLE_METADATA_TABLE);

    when(wrapper.get(expectedObjectKey)).thenReturn(Optional.empty());

    // Act
    admin.createTable(namespace, table, metadata, Collections.emptyMap());

    // Assert
    verify(wrapper).insert(eq(expectedObjectKey), payloadCaptor.capture());

    Map<String, ObjectStorageTableMetadata> insertedMetadata =
        Serializer.deserialize(
            payloadCaptor.getValue(),
            new TypeReference<Map<String, ObjectStorageTableMetadata>>() {});
    String tableMetadataKey = namespace + ObjectStorageUtils.CONCATENATED_KEY_DELIMITER + table;
    assertThat(insertedMetadata).containsKey(tableMetadataKey);
    ObjectStorageTableMetadata tableMetadata = insertedMetadata.get(tableMetadataKey);
    assertThat(tableMetadata.getPartitionKeyNames()).containsExactly("c3");
    assertThat(tableMetadata.getClusteringKeyNames()).containsExactly("c1", "c2");
    assertThat(tableMetadata.getClusteringOrders())
        .containsEntry("c1", "DESC")
        .containsEntry("c2", "ASC");
    assertThat(tableMetadata.getColumns())
        .containsEntry("c1", "text")
        .containsEntry("c2", "bigint")
        .containsEntry("c3", "boolean");
  }

  @Test
  public void dropNamespace_ShouldDeleteNamespaceMetadataAndDeleteMetadataTableIfEmpty()
      throws Exception {
    // Arrange
    String namespace = "ns";
    Map<String, ObjectStorageNamespaceMetadata> metadataTable = new HashMap<>();
    metadataTable.put(namespace, new ObjectStorageNamespaceMetadata(namespace));
    String serializedMetadata = Serializer.serialize(metadataTable);
    ObjectStorageWrapperResponse response =
        new ObjectStorageWrapperResponse(serializedMetadata, "version1");
    String expectedObjectKey =
        ObjectStorageUtils.getObjectKey(
            METADATA_NAMESPACE, ObjectStorageAdmin.NAMESPACE_METADATA_TABLE);

    when(wrapper.get(expectedObjectKey)).thenReturn(Optional.of(response));

    // Act
    admin.dropNamespace(namespace);

    // Assert
    verify(wrapper).delete(eq(expectedObjectKey), eq("version1"));
  }

  @Test
  public void dropNamespace_ShouldDeleteNamespaceMetadataAndUpdateMetadataTableIfNotEmpty()
      throws Exception {
    // Arrange
    String namespace = "ns";
    String anotherNamespace = "other_ns";
    Map<String, ObjectStorageNamespaceMetadata> metadataTable = new HashMap<>();
    metadataTable.put(namespace, new ObjectStorageNamespaceMetadata(namespace));
    metadataTable.put(anotherNamespace, new ObjectStorageNamespaceMetadata(anotherNamespace));
    String serializedMetadata = Serializer.serialize(metadataTable);
    ObjectStorageWrapperResponse response =
        new ObjectStorageWrapperResponse(serializedMetadata, "version1");
    String expectedObjectKey =
        ObjectStorageUtils.getObjectKey(
            METADATA_NAMESPACE, ObjectStorageAdmin.NAMESPACE_METADATA_TABLE);

    when(wrapper.get(expectedObjectKey)).thenReturn(Optional.of(response));

    // Act
    admin.dropNamespace(namespace);

    // Assert
    verify(wrapper).update(eq(expectedObjectKey), payloadCaptor.capture(), eq("version1"));
    Map<String, ObjectStorageNamespaceMetadata> updatedMetadata =
        Serializer.deserialize(
            payloadCaptor.getValue(),
            new TypeReference<Map<String, ObjectStorageNamespaceMetadata>>() {});
    assertThat(updatedMetadata).doesNotContainKey(namespace);
    assertThat(updatedMetadata).containsKey(anotherNamespace);
  }

  @Test
  public void dropTable_ShouldDeleteTableMetadataAndDropMetadataTableIfEmpty() throws Exception {
    // Arrange
    String namespace = "ns";
    String table = "table";
    String tableMetadataKey = namespace + ObjectStorageUtils.CONCATENATED_KEY_DELIMITER + table;

    Map<String, ObjectStorageTableMetadata> metadataTable = new HashMap<>();
    metadataTable.put(tableMetadataKey, ObjectStorageTableMetadata.newBuilder().build());
    String serializedMetadata = Serializer.serialize(metadataTable);
    ObjectStorageWrapperResponse response =
        new ObjectStorageWrapperResponse(serializedMetadata, "version1");
    String expectedObjectKey =
        ObjectStorageUtils.getObjectKey(
            METADATA_NAMESPACE, ObjectStorageAdmin.TABLE_METADATA_TABLE);

    when(wrapper.get(expectedObjectKey)).thenReturn(Optional.of(response));

    // Act
    admin.dropTable(namespace, table);

    // Assert
    verify(wrapper).delete(eq(expectedObjectKey), eq("version1"));
  }

  @Test
  public void dropTable_ShouldDeleteTableMetadataAndUpdateMetadataTableIfNotEmpty()
      throws Exception {
    // Arrange
    String namespace = "ns";
    String table = "table";
    String anotherTable = "tbl2";
    String tableMetadataKey = namespace + ObjectStorageUtils.CONCATENATED_KEY_DELIMITER + table;
    String anotherTableMetadataKey =
        namespace + ObjectStorageUtils.CONCATENATED_KEY_DELIMITER + anotherTable;

    Map<String, ObjectStorageTableMetadata> metadataTable = new HashMap<>();
    metadataTable.put(tableMetadataKey, ObjectStorageTableMetadata.newBuilder().build());
    metadataTable.put(anotherTableMetadataKey, ObjectStorageTableMetadata.newBuilder().build());
    String serializedMetadata = Serializer.serialize(metadataTable);
    ObjectStorageWrapperResponse response =
        new ObjectStorageWrapperResponse(serializedMetadata, "version1");
    String expectedObjectKey =
        ObjectStorageUtils.getObjectKey(
            METADATA_NAMESPACE, ObjectStorageAdmin.TABLE_METADATA_TABLE);

    when(wrapper.get(expectedObjectKey)).thenReturn(Optional.of(response));

    // Act
    admin.dropTable(namespace, table);

    // Assert
    verify(wrapper).update(eq(expectedObjectKey), payloadCaptor.capture(), eq("version1"));
    Map<String, ObjectStorageTableMetadata> updatedMetadata =
        Serializer.deserialize(
            payloadCaptor.getValue(),
            new TypeReference<Map<String, ObjectStorageTableMetadata>>() {});
    assertThat(updatedMetadata).doesNotContainKey(tableMetadataKey);
    assertThat(updatedMetadata).containsKey(anotherTableMetadataKey);
  }

  @Test
  public void truncateTable_ShouldDeleteTableData() throws Exception {
    // Arrange
    String namespace = "ns";
    String table = "table";
    String tableDataPrefix = ObjectStorageUtils.getObjectKey(namespace, table, "");

    // Act
    admin.truncateTable(namespace, table);

    // Assert
    verify(wrapper).deleteByPrefix(tableDataPrefix);
    verify(wrapper, never())
        .delete(
            ObjectStorageUtils.getObjectKey(
                METADATA_NAMESPACE, ObjectStorageAdmin.TABLE_METADATA_TABLE));
  }

  @Test
  public void truncateTable_WithMetadataOnlyTable_ShouldNotThrowException() throws Exception {
    // Arrange
    String namespace = "ns";
    String table = "table";
    String tableDataPrefix = ObjectStorageUtils.getObjectKey(namespace, table, "");

    // Act Assert
    assertThatCode(() -> admin.truncateTable(namespace, table)).doesNotThrowAnyException();
    verify(wrapper).deleteByPrefix(tableDataPrefix);
    verify(wrapper, never())
        .delete(
            ObjectStorageUtils.getObjectKey(
                METADATA_NAMESPACE, ObjectStorageAdmin.TABLE_METADATA_TABLE));
  }

  @Test
  public void getNamespaceTableNames_ShouldReturnTableNamesProperly() throws Exception {
    // Arrange
    String namespace = "ns";
    String tableMetadataKey1 = namespace + ObjectStorageUtils.CONCATENATED_KEY_DELIMITER + "t1";
    String tableMetadataKey2 = namespace + ObjectStorageUtils.CONCATENATED_KEY_DELIMITER + "t2";
    String tableMetadataKey3 = "other_ns" + ObjectStorageUtils.CONCATENATED_KEY_DELIMITER + "t3";

    Map<String, ObjectStorageTableMetadata> metadataTable = new HashMap<>();
    metadataTable.put(tableMetadataKey1, ObjectStorageTableMetadata.newBuilder().build());
    metadataTable.put(tableMetadataKey2, ObjectStorageTableMetadata.newBuilder().build());
    metadataTable.put(tableMetadataKey3, ObjectStorageTableMetadata.newBuilder().build());

    String serializedTableMetadata = Serializer.serialize(metadataTable);
    ObjectStorageWrapperResponse tableMetadataResponse =
        new ObjectStorageWrapperResponse(serializedTableMetadata, "version1");

    Map<String, ObjectStorageNamespaceMetadata> namespaceMetadata = new HashMap<>();
    namespaceMetadata.put(namespace, new ObjectStorageNamespaceMetadata(namespace));
    String serializedNamespaceMetadata = Serializer.serialize(namespaceMetadata);
    ObjectStorageWrapperResponse namespaceResponse =
        new ObjectStorageWrapperResponse(serializedNamespaceMetadata, "version1");

    String tableMetadataObjectKey =
        ObjectStorageUtils.getObjectKey(
            METADATA_NAMESPACE, ObjectStorageAdmin.TABLE_METADATA_TABLE);
    String namespaceMetadataObjectKey =
        ObjectStorageUtils.getObjectKey(
            METADATA_NAMESPACE, ObjectStorageAdmin.NAMESPACE_METADATA_TABLE);

    when(wrapper.get(tableMetadataObjectKey)).thenReturn(Optional.of(tableMetadataResponse));
    when(wrapper.get(namespaceMetadataObjectKey)).thenReturn(Optional.of(namespaceResponse));

    // Act
    Set<String> actualTableNames = admin.getNamespaceTableNames(namespace);

    // Assert
    assertThat(actualTableNames).containsExactlyInAnyOrder("t1", "t2");
  }

  @Test
  public void addNewColumnToTable_ShouldWorkProperly() throws Exception {
    // Arrange
    String namespace = "ns";
    String table = "table";
    String currentColumn = "c1";
    String newColumn = "c2";
    String tableMetadataKey = namespace + ObjectStorageUtils.CONCATENATED_KEY_DELIMITER + table;

    LinkedHashSet<String> partitionKeyNames = Sets.newLinkedHashSet(currentColumn);
    Map<String, String> columns = ImmutableMap.of(currentColumn, "text");
    ObjectStorageTableMetadata existingTableMetadata =
        ObjectStorageTableMetadata.newBuilder()
            .partitionKeyNames(partitionKeyNames)
            .secondaryIndexNames(Collections.emptySet())
            .columns(columns)
            .build();

    Map<String, ObjectStorageTableMetadata> metadataTable = new HashMap<>();
    metadataTable.put(tableMetadataKey, existingTableMetadata);
    String serializedMetadata = Serializer.serialize(metadataTable);
    ObjectStorageWrapperResponse response =
        new ObjectStorageWrapperResponse(serializedMetadata, "version1");
    String expectedObjectKey =
        ObjectStorageUtils.getObjectKey(
            METADATA_NAMESPACE, ObjectStorageAdmin.TABLE_METADATA_TABLE);

    when(wrapper.get(expectedObjectKey)).thenReturn(Optional.of(response));

    // Act
    admin.addNewColumnToTable(namespace, table, newColumn, DataType.INT);

    // Assert
    verify(wrapper).update(eq(expectedObjectKey), payloadCaptor.capture(), eq("version1"));

    Map<String, ObjectStorageTableMetadata> updatedMetadata =
        Serializer.deserialize(
            payloadCaptor.getValue(),
            new TypeReference<Map<String, ObjectStorageTableMetadata>>() {});

    ObjectStorageTableMetadata updatedTableMetadata = updatedMetadata.get(tableMetadataKey);
    assertThat(updatedTableMetadata.getPartitionKeyNames()).containsExactly(currentColumn);
    assertThat(updatedTableMetadata.getColumns())
        .containsEntry(currentColumn, "text")
        .containsEntry(newColumn, "int");
  }

  @Test
  public void repairNamespace_ShouldUpsertNamespaceMetadata() throws Exception {
    // Arrange
    String namespace = "ns";
    Map<String, ObjectStorageNamespaceMetadata> metadataTable = new HashMap<>();
    String serializedMetadata = Serializer.serialize(metadataTable);
    ObjectStorageWrapperResponse response =
        new ObjectStorageWrapperResponse(serializedMetadata, "version1");
    String expectedObjectKey =
        ObjectStorageUtils.getObjectKey(
            METADATA_NAMESPACE, ObjectStorageAdmin.NAMESPACE_METADATA_TABLE);

    when(wrapper.get(expectedObjectKey)).thenReturn(Optional.of(response));

    // Act
    admin.repairNamespace(namespace, Collections.emptyMap());

    // Assert
    verify(wrapper).insert(eq(expectedObjectKey), payloadCaptor.capture());

    Map<String, ObjectStorageNamespaceMetadata> insertedMetadata =
        Serializer.deserialize(
            payloadCaptor.getValue(),
            new TypeReference<Map<String, ObjectStorageNamespaceMetadata>>() {});
    assertThat(insertedMetadata).containsKey(namespace);
    assertThat(insertedMetadata.get(namespace).getName()).isEqualTo(namespace);
  }

  @Test
  public void repairTable_ShouldUpsertTableMetadata() throws Exception {
    // Arrange
    String namespace = "ns";
    String table = "tbl";
    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn("c1", DataType.INT)
            .addColumn("c2", DataType.TEXT)
            .addColumn("c3", DataType.BIGINT)
            .addPartitionKey("c1")
            .build();

    Map<String, ObjectStorageTableMetadata> metadataTable = new HashMap<>();
    String serializedMetadata = Serializer.serialize(metadataTable);
    ObjectStorageWrapperResponse response =
        new ObjectStorageWrapperResponse(serializedMetadata, "version1");
    String expectedObjectKey =
        ObjectStorageUtils.getObjectKey(
            METADATA_NAMESPACE, ObjectStorageAdmin.TABLE_METADATA_TABLE);

    when(wrapper.get(expectedObjectKey)).thenReturn(Optional.of(response));

    // Act
    admin.repairTable(namespace, table, tableMetadata, Collections.emptyMap());

    // Assert
    verify(wrapper).insert(eq(expectedObjectKey), payloadCaptor.capture());

    Map<String, ObjectStorageTableMetadata> insertedMetadata =
        Serializer.deserialize(
            payloadCaptor.getValue(),
            new TypeReference<Map<String, ObjectStorageTableMetadata>>() {});

    String tableMetadataKey = namespace + ObjectStorageUtils.CONCATENATED_KEY_DELIMITER + table;
    assertThat(insertedMetadata).containsKey(tableMetadataKey);

    ObjectStorageTableMetadata insertedTableMetadata = insertedMetadata.get(tableMetadataKey);
    assertThat(insertedTableMetadata.getPartitionKeyNames()).containsExactly("c1");
    assertThat(insertedTableMetadata.getColumns())
        .containsEntry("c1", "int")
        .containsEntry("c2", "text")
        .containsEntry("c3", "bigint");
  }

  @Test
  public void upgrade_ShouldDoNothing() {
    // Arrange

    // Act Assert
    assertThatCode(() -> admin.upgrade(Collections.emptyMap())).doesNotThrowAnyException();
  }
}
