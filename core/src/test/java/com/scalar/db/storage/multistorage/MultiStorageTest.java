package com.scalar.db.storage.multistorage;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.verify;

import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Put;
import com.scalar.db.api.Scan;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Key;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class MultiStorageTest {

  protected static final String NAMESPACE1 = "test_ns1";
  protected static final String NAMESPACE2 = "test_ns2";
  protected static final String TABLE1 = "test_table1";
  protected static final String TABLE2 = "test_table2";
  protected static final String TABLE3 = "test_table3";
  protected static final String COL_NAME1 = "c1";
  protected static final String COL_NAME2 = "c2";
  protected static final String COL_NAME3 = "c3";
  @Mock private DatabaseConfig databaseConfig;
  @Mock private DistributedStorage storage1;
  @Mock private DistributedStorage storage2;
  @Mock private DistributedStorage storage3;

  private MultiStorage multiStorage;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    Map<String, DistributedStorage> tableStorageMap = new HashMap<>();
    tableStorageMap.put(NAMESPACE1 + "." + TABLE1, storage1);
    tableStorageMap.put(NAMESPACE1 + "." + TABLE2, storage2);
    Map<String, DistributedStorage> namespaceStorageMap = new HashMap<>();
    namespaceStorageMap.put(NAMESPACE2, storage2);
    DistributedStorage defaultStorage = storage3;
    multiStorage =
        new MultiStorage(databaseConfig, tableStorageMap, namespaceStorageMap, defaultStorage);
  }

  @Test
  public void whenGetDataFromTable1_DataShouldBeGottenFromStorage1() throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE1;
    String table = TABLE1;
    Key partitionKey = new Key(COL_NAME1, 1);
    Key clusteringKey = new Key(COL_NAME2, 2);
    Get get = new Get(partitionKey, clusteringKey).forNamespace(namespace).forTable(table);

    // Act
    multiStorage.get(get);

    // Assert
    verify(storage1).get(any(Get.class));
  }

  @Test
  public void whenGetDataFromTable2_DataShouldBeGottenFromStorage2() throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE1;
    String table = TABLE2;
    Key partitionKey = new Key(COL_NAME1, 1);
    Key clusteringKey = new Key(COL_NAME2, 2);
    Get get = new Get(partitionKey, clusteringKey).forNamespace(namespace).forTable(table);

    // Act
    multiStorage.get(get);

    // Assert
    verify(storage2).get(any(Get.class));
  }

  @Test
  public void whenGetDataFromTable3_DataShouldBeGottenFromDefaultStorage()
      throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE1;
    String table = TABLE3;
    Key partitionKey = new Key(COL_NAME1, 1);
    Key clusteringKey = new Key(COL_NAME2, 2);
    Get get = new Get(partitionKey, clusteringKey).forNamespace(namespace).forTable(table);

    // Act
    multiStorage.get(get);

    // Assert
    verify(storage3).get(any(Get.class));
  }

  @Test
  public void whenScanDataFromTable1_DataShouldBeScannedFromStorage1() throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE1;
    String table = TABLE1;
    Key partitionKey = new Key(COL_NAME1, 1);
    Scan scan = new Scan(partitionKey).forNamespace(namespace).forTable(table);

    // Act
    multiStorage.scan(scan);

    // Assert
    verify(storage1).scan(any(Scan.class));
  }

  @Test
  public void whenScanDataFromTable2_DataShouldBeScannedFromStorage2() throws ExecutionException {
    String namespace = NAMESPACE1;
    String table = TABLE2;
    Key partitionKey = new Key(COL_NAME1, 1);
    Scan scan = new Scan(partitionKey).forNamespace(namespace).forTable(table);

    // Act
    multiStorage.scan(scan);

    // Assert
    verify(storage2).scan(any(Scan.class));
  }

  @Test
  public void whenScanDataFromTable3_DataShouldBeScannedFromDefaultStorage()
      throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE1;
    String table = TABLE3;
    Key partitionKey = new Key(COL_NAME1, 1);
    Scan scan = new Scan(partitionKey).forNamespace(namespace).forTable(table);

    // Act
    multiStorage.scan(scan);

    // Assert
    verify(storage3).scan(any(Scan.class));
  }

  @Test
  public void whenPutDataIntoTable1_DataShouldBeWrittenIntoStorage1() throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE1;
    String table = TABLE1;
    Key partitionKey = new Key(COL_NAME1, 1);
    Key clusteringKey = new Key(COL_NAME2, 2);
    Put put =
        new Put(partitionKey, clusteringKey)
            .withValue(COL_NAME3, 3)
            .forNamespace(namespace)
            .forTable(table);

    // Act
    multiStorage.put(put);

    // Assert
    verify(storage1).put(any(Put.class));
  }

  @Test
  public void whenPutDataIntoTable2_DataShouldBeWrittenIntoStorage2() throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE1;
    String table = TABLE2;
    Key partitionKey = new Key(COL_NAME1, 1);
    Key clusteringKey = new Key(COL_NAME2, 2);
    Put put =
        new Put(partitionKey, clusteringKey)
            .withValue(COL_NAME3, 3)
            .forNamespace(namespace)
            .forTable(table);

    // Act
    multiStorage.put(put);

    // Assert
    verify(storage2).put(any(Put.class));
  }

  @Test
  public void whenPutDataIntoTable3_DataShouldBeWrittenIntoDefaultStorage()
      throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE1;
    String table = TABLE3;
    Key partitionKey = new Key(COL_NAME1, 1);
    Key clusteringKey = new Key(COL_NAME2, 2);
    Put put =
        new Put(partitionKey, clusteringKey)
            .withValue(COL_NAME3, 3)
            .forNamespace(namespace)
            .forTable(table);

    // Act
    multiStorage.put(put);

    // Assert
    verify(storage3).put(any(Put.class));
  }

  @Test
  public void whenDeleteDataFromTable1_DataShouldBeDeletedFromStorage1() throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE1;
    String table = TABLE1;
    Key partitionKey = new Key(COL_NAME1, 1);
    Key clusteringKey = new Key(COL_NAME2, 2);
    Delete delete = new Delete(partitionKey, clusteringKey).forNamespace(namespace).forTable(table);

    // Act
    multiStorage.delete(delete);

    // Assert
    verify(storage1).delete(any(Delete.class));
  }

  @Test
  public void whenDeleteDataFromTable2_DataShouldBeDeletedFromStorage2() throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE1;
    String table = TABLE2;
    Key partitionKey = new Key(COL_NAME1, 1);
    Key clusteringKey = new Key(COL_NAME2, 2);
    Delete delete = new Delete(partitionKey, clusteringKey).forNamespace(namespace).forTable(table);

    // Act
    multiStorage.delete(delete);

    // Assert
    verify(storage2).delete(any(Delete.class));
  }

  @Test
  public void whenDeleteDataFromTable3_DataShouldBeDeletedFromDefaultStorage()
      throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE1;
    String table = TABLE3;
    Key partitionKey = new Key(COL_NAME1, 1);
    Key clusteringKey = new Key(COL_NAME2, 2);
    Delete delete = new Delete(partitionKey, clusteringKey).forNamespace(namespace).forTable(table);

    // Act
    multiStorage.delete(delete);

    // Assert
    verify(storage3).delete(any(Delete.class));
  }

  @Test
  public void whenMutateDataToTable1_ShouldExecuteForStorage1() throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE1;
    String table = TABLE1;
    Key partitionKey = new Key(COL_NAME1, 1);
    Key clusteringKey1 = new Key(COL_NAME2, 2);
    Key clusteringKey2 = new Key(COL_NAME2, 3);

    // Act
    multiStorage.mutate(
        Arrays.asList(
            new Put(partitionKey, clusteringKey2)
                .withValue(COL_NAME3, 3)
                .forNamespace(namespace)
                .forTable(table),
            new Delete(partitionKey, clusteringKey1).forNamespace(namespace).forTable(table)));

    // Assert
    verify(storage1).mutate(anyList());
  }

  @Test
  public void whenMutateDataToTable2_ShouldExecuteForStorage2() throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE1;
    String table = TABLE2;
    Key partitionKey = new Key(COL_NAME1, 1);
    Key clusteringKey1 = new Key(COL_NAME2, 2);
    Key clusteringKey2 = new Key(COL_NAME2, 3);

    // Act
    multiStorage.mutate(
        Arrays.asList(
            new Put(partitionKey, clusteringKey2)
                .withValue(COL_NAME3, 3)
                .forNamespace(namespace)
                .forTable(table),
            new Delete(partitionKey, clusteringKey1).forNamespace(namespace).forTable(table)));

    // Assert
    verify(storage2).mutate(anyList());
  }

  @Test
  public void whenMutateDataToTable3_ShouldExecuteForDefaultStorage() throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE1;
    String table = TABLE3;
    Key partitionKey = new Key(COL_NAME1, 1);
    Key clusteringKey1 = new Key(COL_NAME2, 2);
    Key clusteringKey2 = new Key(COL_NAME2, 3);

    // Act
    multiStorage.mutate(
        Arrays.asList(
            new Put(partitionKey, clusteringKey2)
                .withValue(COL_NAME3, 3)
                .forNamespace(namespace)
                .forTable(table),
            new Delete(partitionKey, clusteringKey1).forNamespace(namespace).forTable(table)));

    // Assert
    verify(storage3).mutate(anyList());
  }

  @Test
  public void whenCallMutateWithEmptyList_ShouldThrowIllegalArgumentException() {
    // Arrange Act Assert
    assertThatThrownBy(() -> multiStorage.mutate(Collections.emptyList()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void whenGetDataFromTable1InNamespace2_DataShouldBeGottenFromStorage2()
      throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE2;
    String table = TABLE1;
    Key partitionKey = new Key(COL_NAME1, 1);
    Key clusteringKey = new Key(COL_NAME2, 2);
    Get get = new Get(partitionKey, clusteringKey).forNamespace(namespace).forTable(table);

    // Act
    multiStorage.get(get);

    // Assert
    verify(storage2).get(any(Get.class));
  }

  @Test
  public void whenScanDataFromTable1InNamespace2_DataShouldBeScannedFromStorage2()
      throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE2;
    String table = TABLE1;
    Key partitionKey = new Key(COL_NAME1, 1);
    Scan scan = new Scan(partitionKey).forNamespace(namespace).forTable(table);

    // Act
    multiStorage.scan(scan);

    // Assert
    verify(storage2).scan(any(Scan.class));
  }

  @Test
  public void whenPutDataIntoTable1InNamespace2_DataShouldBeWrittenIntoStorage2()
      throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE2;
    String table = TABLE1;
    Key partitionKey = new Key(COL_NAME1, 1);
    Key clusteringKey = new Key(COL_NAME2, 2);
    Put put =
        new Put(partitionKey, clusteringKey)
            .withValue(COL_NAME3, 3)
            .forNamespace(namespace)
            .forTable(table);

    // Act
    multiStorage.put(put);

    // Assert
    verify(storage2).put(any(Put.class));
  }

  @Test
  public void whenDeleteDataFromTable1InNamespace2_DataShouldBeDeletedFromStorage1()
      throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE2;
    String table = TABLE1;
    Key partitionKey = new Key(COL_NAME1, 1);
    Key clusteringKey = new Key(COL_NAME2, 2);
    Delete delete = new Delete(partitionKey, clusteringKey).forNamespace(namespace).forTable(table);

    // Act
    multiStorage.delete(delete);

    // Assert
    verify(storage2).delete(any(Delete.class));
  }

  @Test
  public void whenMutateDataToTable1InNamespace2_ShouldExecuteForStorage2()
      throws ExecutionException {
    // Arrange
    String namespace = NAMESPACE2;
    String table = TABLE1;
    Key partitionKey = new Key(COL_NAME1, 1);
    Key clusteringKey1 = new Key(COL_NAME2, 2);
    Key clusteringKey2 = new Key(COL_NAME2, 3);

    // Act
    multiStorage.mutate(
        Arrays.asList(
            new Put(partitionKey, clusteringKey2)
                .withValue(COL_NAME3, 3)
                .forNamespace(namespace)
                .forTable(table),
            new Delete(partitionKey, clusteringKey1).forNamespace(namespace).forTable(table)));

    // Assert
    verify(storage2).mutate(anyList());
  }
}
