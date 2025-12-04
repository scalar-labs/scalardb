package com.scalar.db.storage.objectstorage;

import static com.scalar.db.api.ConditionBuilder.column;
import static com.scalar.db.api.ConditionBuilder.deleteIf;
import static com.scalar.db.api.ConditionBuilder.deleteIfExists;
import static com.scalar.db.api.ConditionBuilder.putIf;
import static com.scalar.db.api.ConditionBuilder.putIfExists;
import static com.scalar.db.api.ConditionBuilder.putIfNotExists;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;

import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.MutationCondition;
import com.scalar.db.api.Put;
import com.scalar.db.api.Scan;
import com.scalar.db.api.StorageInfo;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.StorageInfoImpl;
import com.scalar.db.common.StorageInfoProvider;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

public class ObjectStorageOperationCheckerTest {
  private static final String NAMESPACE_NAME = "n1";
  private static final String TABLE_NAME = "t1";
  private static final String PKEY1 = "p1";
  private static final String CKEY1 = "c1";
  private static final String COL1 = "v1";
  private static final String COL2 = "v2";
  private static final String COL3 = "v3";
  private static final String COL4 = "v4";
  private static final StorageInfo STORAGE_INFO =
      new StorageInfoImpl("ObjectStorage", StorageInfo.MutationAtomicityUnit.STORAGE, 100, false);

  private static final TableMetadata TABLE_METADATA1 =
      TableMetadata.newBuilder()
          .addColumn(PKEY1, DataType.INT)
          .addColumn(CKEY1, DataType.INT)
          .addColumn(COL1, DataType.INT)
          .addColumn(COL2, DataType.BOOLEAN)
          .addColumn(COL3, DataType.TEXT)
          .addColumn(COL4, DataType.BLOB)
          .addPartitionKey(PKEY1)
          .addClusteringKey(CKEY1)
          .build();

  private static final TableMetadata TABLE_METADATA2 =
      TableMetadata.newBuilder()
          .addColumn(PKEY1, DataType.TEXT)
          .addColumn(CKEY1, DataType.TEXT)
          .addPartitionKey(PKEY1)
          .addClusteringKey(CKEY1)
          .build();

  @Mock private DatabaseConfig databaseConfig;
  @Mock private TableMetadataManager metadataManager;
  @Mock private StorageInfoProvider storageInfoProvider;
  private ObjectStorageOperationChecker operationChecker;

  @BeforeEach
  public void setUp() throws Exception {
    openMocks(this).close();
    when(storageInfoProvider.getStorageInfo(any())).thenReturn(STORAGE_INFO);
    operationChecker =
        new ObjectStorageOperationChecker(databaseConfig, metadataManager, storageInfoProvider);
  }

  @Test
  public void check_ForMutationsWithPut_ShouldDoNothing() throws ExecutionException {
    // Arrange
    when(metadataManager.getTableMetadata(any())).thenReturn(TABLE_METADATA1);
    Put putWithoutSettingIndex =
        Put.newBuilder()
            .namespace(NAMESPACE_NAME)
            .table(TABLE_NAME)
            .partitionKey(Key.ofInt(PKEY1, 0))
            .clusteringKey(Key.ofInt(CKEY1, 0))
            .build();
    Put put =
        Put.newBuilder()
            .namespace(NAMESPACE_NAME)
            .table(TABLE_NAME)
            .partitionKey(Key.ofInt(PKEY1, 0))
            .clusteringKey(Key.ofInt(CKEY1, 1))
            .intValue(COL1, 1)
            .build();

    // Act Assert
    assertThatCode(() -> operationChecker.check(Arrays.asList(putWithoutSettingIndex, put)))
        .doesNotThrowAnyException();
  }

  @Test
  public void check_ForMutationsWithDelete_ShouldDoNothing() throws ExecutionException {
    // Arrange
    when(metadataManager.getTableMetadata(any())).thenReturn(TABLE_METADATA1);
    Delete deleteWithoutSettingIndex =
        Delete.newBuilder()
            .namespace(NAMESPACE_NAME)
            .table(TABLE_NAME)
            .partitionKey(Key.ofInt(PKEY1, 0))
            .clusteringKey(Key.ofInt(CKEY1, 0))
            .build();
    Delete delete =
        Delete.newBuilder()
            .namespace(NAMESPACE_NAME)
            .table(TABLE_NAME)
            .partitionKey(Key.ofInt(PKEY1, 0))
            .clusteringKey(Key.ofInt(CKEY1, 1))
            .build();

    // Act Assert
    assertThatCode(() -> operationChecker.check(Arrays.asList(deleteWithoutSettingIndex, delete)))
        .doesNotThrowAnyException();
  }

  @Test
  public void
      check_GetGiven_WhenIllegalCharacterInPrimaryKeyColumn_ShouldThrowIllegalArgumentException()
          throws ExecutionException {
    // Arrange
    when(metadataManager.getTableMetadata(any())).thenReturn(TABLE_METADATA2);

    Get get1 =
        Get.newBuilder()
            .namespace(NAMESPACE_NAME)
            .table(TABLE_NAME)
            .partitionKey(Key.ofText(PKEY1, "ab"))
            .clusteringKey(Key.ofText(CKEY1, "ab"))
            .build();
    Get get2 =
        Get.newBuilder()
            .namespace(NAMESPACE_NAME)
            .table(TABLE_NAME)
            .partitionKey(Key.ofText(PKEY1, "ab" + ObjectStorageUtils.OBJECT_KEY_DELIMITER))
            .clusteringKey(Key.ofText(CKEY1, "ab"))
            .build();
    Get get3 =
        Get.newBuilder()
            .namespace(NAMESPACE_NAME)
            .table(TABLE_NAME)
            .partitionKey(Key.ofText(PKEY1, "ab"))
            .clusteringKey(Key.ofText(CKEY1, "ab" + ObjectStorageUtils.OBJECT_KEY_DELIMITER))
            .build();
    Get get4 =
        Get.newBuilder()
            .namespace(NAMESPACE_NAME)
            .table(TABLE_NAME)
            .partitionKey(Key.ofText(PKEY1, "ab" + ObjectStorageUtils.CONCATENATED_KEY_DELIMITER))
            .clusteringKey(Key.ofText(CKEY1, "ab"))
            .build();
    Get get5 =
        Get.newBuilder()
            .namespace(NAMESPACE_NAME)
            .table(TABLE_NAME)
            .partitionKey(Key.ofText(PKEY1, "ab"))
            .clusteringKey(Key.ofText(CKEY1, "ab" + ObjectStorageUtils.CONCATENATED_KEY_DELIMITER))
            .build();

    // Act Assert
    assertThatCode(() -> operationChecker.check(get1)).doesNotThrowAnyException();
    assertThatThrownBy(() -> operationChecker.check(get2))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> operationChecker.check(get3))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> operationChecker.check(get4))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> operationChecker.check(get5))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      check_ScanGiven_WhenIllegalCharacterInPrimaryKeyColumn_ShouldThrowIllegalArgumentException()
          throws ExecutionException {
    // Arrange
    when(metadataManager.getTableMetadata(any())).thenReturn(TABLE_METADATA2);

    Scan scan1 =
        Scan.newBuilder()
            .namespace(NAMESPACE_NAME)
            .table(TABLE_NAME)
            .partitionKey(Key.ofText(PKEY1, "ab"))
            .start(Key.ofText(CKEY1, "ab"))
            .end(Key.ofText(CKEY1, "ab"))
            .build();
    Scan scan2 =
        Scan.newBuilder()
            .namespace(NAMESPACE_NAME)
            .table(TABLE_NAME)
            .partitionKey(Key.ofText(PKEY1, "ab" + ObjectStorageUtils.OBJECT_KEY_DELIMITER))
            .start(Key.ofText(CKEY1, "ab"))
            .end(Key.ofText(CKEY1, "ab"))
            .build();
    Scan scan3 =
        Scan.newBuilder()
            .namespace(NAMESPACE_NAME)
            .table(TABLE_NAME)
            .partitionKey(Key.ofText(PKEY1, "ab"))
            .start(Key.ofText(CKEY1, "ab" + ObjectStorageUtils.OBJECT_KEY_DELIMITER))
            .end(Key.ofText(CKEY1, "ab"))
            .build();
    Scan scan4 =
        Scan.newBuilder()
            .namespace(NAMESPACE_NAME)
            .table(TABLE_NAME)
            .partitionKey(Key.ofText(PKEY1, "ab"))
            .start(Key.ofText(CKEY1, "ab"))
            .end(Key.ofText(CKEY1, "ab" + ObjectStorageUtils.CONCATENATED_KEY_DELIMITER))
            .build();

    // Act Assert
    assertThatCode(() -> operationChecker.check(scan1)).doesNotThrowAnyException();
    assertThatThrownBy(() -> operationChecker.check(scan2))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> operationChecker.check(scan3))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> operationChecker.check(scan4))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      check_PutGiven_WhenIllegalCharacterInPrimaryKeyColumn_ShouldThrowIllegalArgumentException()
          throws ExecutionException {
    // Arrange
    when(metadataManager.getTableMetadata(any())).thenReturn(TABLE_METADATA2);

    Put put1 =
        Put.newBuilder()
            .namespace(NAMESPACE_NAME)
            .table(TABLE_NAME)
            .partitionKey(Key.ofText(PKEY1, "ab"))
            .clusteringKey(Key.ofText(CKEY1, "ab"))
            .build();
    Put put2 =
        Put.newBuilder()
            .namespace(NAMESPACE_NAME)
            .table(TABLE_NAME)
            .partitionKey(Key.ofText(PKEY1, "ab" + ObjectStorageUtils.OBJECT_KEY_DELIMITER))
            .clusteringKey(Key.ofText(CKEY1, "ab"))
            .build();
    Put put3 =
        Put.newBuilder()
            .namespace(NAMESPACE_NAME)
            .table(TABLE_NAME)
            .partitionKey(Key.ofText(PKEY1, "ab"))
            .clusteringKey(Key.ofText(CKEY1, "ab" + ObjectStorageUtils.CONCATENATED_KEY_DELIMITER))
            .build();

    // Act Assert
    assertThatCode(() -> operationChecker.check(put1)).doesNotThrowAnyException();
    assertThatThrownBy(() -> operationChecker.check(put2))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> operationChecker.check(put3))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      check_DeleteGiven_WhenIllegalCharacterInPrimaryKeyColumn_ShouldThrowIllegalArgumentException()
          throws ExecutionException {
    // Arrange
    when(metadataManager.getTableMetadata(any())).thenReturn(TABLE_METADATA2);

    Delete delete1 =
        Delete.newBuilder()
            .namespace(NAMESPACE_NAME)
            .table(TABLE_NAME)
            .partitionKey(Key.ofText(PKEY1, "ab"))
            .clusteringKey(Key.ofText(CKEY1, "ab"))
            .build();
    Delete delete2 =
        Delete.newBuilder()
            .namespace(NAMESPACE_NAME)
            .table(TABLE_NAME)
            .partitionKey(Key.ofText(PKEY1, "ab" + ObjectStorageUtils.OBJECT_KEY_DELIMITER))
            .clusteringKey(Key.ofText(CKEY1, "ab"))
            .build();
    Delete delete3 =
        Delete.newBuilder()
            .namespace(NAMESPACE_NAME)
            .table(TABLE_NAME)
            .partitionKey(Key.ofText(PKEY1, "ab"))
            .clusteringKey(Key.ofText(CKEY1, "ab" + ObjectStorageUtils.CONCATENATED_KEY_DELIMITER))
            .build();

    // Act Assert
    assertThatCode(() -> operationChecker.check(delete1)).doesNotThrowAnyException();
    assertThatThrownBy(() -> operationChecker.check(delete2))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> operationChecker.check(delete3))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      check_MutationsGiven_WhenIllegalCharacterInPrimaryKeyColumn_ShouldThrowIllegalArgumentException()
          throws ExecutionException {
    // Arrange
    when(metadataManager.getTableMetadata(any())).thenReturn(TABLE_METADATA2);
    when(storageInfoProvider.getStorageInfo(any())).thenReturn(STORAGE_INFO);

    Put put1 =
        Put.newBuilder()
            .namespace(NAMESPACE_NAME)
            .table(TABLE_NAME)
            .partitionKey(Key.ofText(PKEY1, "ab"))
            .clusteringKey(Key.ofText(CKEY1, "ab"))
            .build();
    Put put2 =
        Put.newBuilder()
            .namespace(NAMESPACE_NAME)
            .table(TABLE_NAME)
            .partitionKey(Key.ofText(PKEY1, "ab" + ObjectStorageUtils.OBJECT_KEY_DELIMITER))
            .clusteringKey(Key.ofText(CKEY1, "ab"))
            .build();
    Delete delete1 =
        Delete.newBuilder()
            .namespace(NAMESPACE_NAME)
            .table(TABLE_NAME)
            .partitionKey(Key.ofText(PKEY1, "ab"))
            .clusteringKey(Key.ofText(CKEY1, "ab"))
            .build();
    Delete delete2 =
        Delete.newBuilder()
            .namespace(NAMESPACE_NAME)
            .table(TABLE_NAME)
            .partitionKey(Key.ofText(PKEY1, "ab" + ObjectStorageUtils.CONCATENATED_KEY_DELIMITER))
            .clusteringKey(Key.ofText(CKEY1, "ab"))
            .build();

    // Act Assert
    assertThatCode(() -> operationChecker.check(Arrays.asList(put1, delete1)))
        .doesNotThrowAnyException();
    assertThatThrownBy(() -> operationChecker.check(Arrays.asList(put2, delete1)))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> operationChecker.check(Arrays.asList(put1, delete2)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void check_ForPutWithCondition_ShouldBehaveProperly() throws ExecutionException {
    // Arrange
    when(metadataManager.getTableMetadata(any())).thenReturn(TABLE_METADATA1);

    // Act Assert
    assertThatCode(() -> operationChecker.check(buildPutWithCondition(putIfExists())))
        .doesNotThrowAnyException();
    assertThatCode(() -> operationChecker.check(buildPutWithCondition(putIfNotExists())))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    buildPutWithCondition(putIf(column(COL1).isEqualToInt(1)).build())))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    buildPutWithCondition(
                        putIf(column(COL1).isGreaterThanOrEqualToInt(1)).build())))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    buildPutWithCondition(putIf(column(COL1).isNullInt()).build())))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    buildPutWithCondition(putIf(column(COL2).isEqualToBoolean(true)).build())))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    buildPutWithCondition(putIf(column(COL2).isNotEqualToBoolean(true)).build())))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    buildPutWithCondition(putIf(column(COL2).isNullBoolean()).build())))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    buildPutWithCondition(putIf(column(COL2).isNotNullBoolean()).build())))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    buildPutWithCondition(putIf(column(COL2).isGreaterThanBoolean(false)).build())))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    buildPutWithCondition(
                        putIf(column(COL2).isLessThanOrEqualToBoolean(true)).build())))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    buildPutWithCondition(
                        putIf(column(COL4).isEqualToBlob(new byte[] {1, 2, 3})).build())))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    buildPutWithCondition(
                        putIf(column(COL4).isNotEqualToBlob(new byte[] {1, 2, 3})).build())))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    buildPutWithCondition(putIf(column(COL4).isNullBlob()).build())))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    buildPutWithCondition(putIf(column(COL4).isNotNullBlob()).build())))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    buildPutWithCondition(
                        putIf(column(COL4).isGreaterThanBlob(new byte[] {1, 2, 3})).build())))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    buildPutWithCondition(
                        putIf(column(COL4).isLessThanOrEqualToBlob(new byte[] {1, 2, 3})).build())))
        .doesNotThrowAnyException();
  }

  @Test
  public void check_ForDeleteWithCondition_ShouldBehaveProperly() throws ExecutionException {
    // Arrange
    when(metadataManager.getTableMetadata(any())).thenReturn(TABLE_METADATA1);

    // Act Assert
    assertThatCode(() -> operationChecker.check(buildDeleteWithCondition(deleteIfExists())))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    buildDeleteWithCondition(deleteIf(column(COL1).isEqualToInt(1)).build())))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    buildDeleteWithCondition(
                        deleteIf(column(COL1).isGreaterThanOrEqualToInt(1)).build())))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    buildDeleteWithCondition(deleteIf(column(COL1).isNullInt()).build())))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    buildDeleteWithCondition(
                        deleteIf(column(COL2).isEqualToBoolean(true)).build())))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    buildDeleteWithCondition(
                        deleteIf(column(COL2).isNotEqualToBoolean(true)).build())))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    buildDeleteWithCondition(deleteIf(column(COL2).isNullBoolean()).build())))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    buildDeleteWithCondition(deleteIf(column(COL2).isNotNullBoolean()).build())))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    buildDeleteWithCondition(
                        deleteIf(column(COL2).isGreaterThanBoolean(false)).build())))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    buildDeleteWithCondition(
                        deleteIf(column(COL2).isLessThanOrEqualToBoolean(true)).build())))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    buildDeleteWithCondition(
                        deleteIf(column(COL4).isEqualToBlob(new byte[] {1, 2, 3})).build())))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    buildDeleteWithCondition(
                        deleteIf(column(COL4).isNotEqualToBlob(new byte[] {1, 2, 3})).build())))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    buildDeleteWithCondition(deleteIf(column(COL4).isNullBlob()).build())))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    buildDeleteWithCondition(deleteIf(column(COL4).isNotNullBlob()).build())))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    buildDeleteWithCondition(
                        deleteIf(column(COL4).isGreaterThanBlob(new byte[] {1, 2, 3})).build())))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    buildDeleteWithCondition(
                        deleteIf(column(COL4).isLessThanOrEqualToBlob(new byte[] {1, 2, 3}))
                            .build())))
        .doesNotThrowAnyException();
  }

  @Test
  public void check_ForMutationsWithPutWithCondition_ShouldBehaveProperly()
      throws ExecutionException {
    // Arrange
    when(metadataManager.getTableMetadata(any())).thenReturn(TABLE_METADATA1);
    Put put =
        Put.newBuilder()
            .namespace(NAMESPACE_NAME)
            .table(TABLE_NAME)
            .partitionKey(Key.ofInt(PKEY1, 0))
            .clusteringKey(Key.ofInt(CKEY1, 1))
            .intValue(COL1, 1)
            .build();

    // Act Assert
    assertThatCode(
            () -> operationChecker.check(Arrays.asList(buildPutWithCondition(putIfExists()), put)))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(Arrays.asList(buildPutWithCondition(putIfNotExists()), put)))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    Arrays.asList(
                        buildPutWithCondition(putIf(column(COL1).isEqualToInt(1)).build()), put)))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    Arrays.asList(
                        buildPutWithCondition(
                            putIf(column(COL1).isGreaterThanOrEqualToInt(1)).build()),
                        put)))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    Arrays.asList(
                        buildPutWithCondition(putIf(column(COL1).isNullInt()).build()), put)))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    Arrays.asList(
                        buildPutWithCondition(putIf(column(COL2).isEqualToBoolean(true)).build()),
                        put)))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    Arrays.asList(
                        buildPutWithCondition(
                            putIf(column(COL2).isNotEqualToBoolean(true)).build()),
                        put)))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    Arrays.asList(
                        buildPutWithCondition(putIf(column(COL2).isNullBoolean()).build()), put)))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    Arrays.asList(
                        buildPutWithCondition(putIf(column(COL2).isNotNullBoolean()).build()),
                        put)))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    Arrays.asList(
                        buildPutWithCondition(
                            putIf(column(COL2).isGreaterThanBoolean(false)).build()),
                        put)))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    Arrays.asList(
                        buildPutWithCondition(
                            putIf(column(COL2).isLessThanOrEqualToBoolean(true)).build()),
                        put)))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    Arrays.asList(
                        buildPutWithCondition(
                            putIf(column(COL4).isEqualToBlob(new byte[] {1, 2, 3})).build()),
                        put)))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    Arrays.asList(
                        buildPutWithCondition(
                            putIf(column(COL4).isNotEqualToBlob(new byte[] {1, 2, 3})).build()),
                        put)))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    Arrays.asList(
                        buildPutWithCondition(putIf(column(COL4).isNullBlob()).build()), put)))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    Arrays.asList(
                        buildPutWithCondition(putIf(column(COL4).isNotNullBlob()).build()), put)))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    Arrays.asList(
                        buildPutWithCondition(
                            putIf(column(COL4).isGreaterThanBlob(new byte[] {1, 2, 3})).build()),
                        put)))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    Arrays.asList(
                        buildPutWithCondition(
                            putIf(column(COL4).isLessThanOrEqualToBlob(new byte[] {1, 2, 3}))
                                .build()),
                        put)))
        .doesNotThrowAnyException();
  }

  @Test
  public void check_ForMutationsWithDeleteWithCondition_ShouldBehaveProperly()
      throws ExecutionException {
    // Arrange
    when(metadataManager.getTableMetadata(any())).thenReturn(TABLE_METADATA1);
    Delete delete =
        Delete.newBuilder()
            .namespace(NAMESPACE_NAME)
            .table(TABLE_NAME)
            .partitionKey(Key.ofInt(PKEY1, 0))
            .clusteringKey(Key.ofInt(CKEY1, 1))
            .build();

    // Act Assert
    assertThatCode(
            () ->
                operationChecker.check(
                    Arrays.asList(buildDeleteWithCondition(deleteIfExists()), delete)))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    Arrays.asList(
                        buildDeleteWithCondition(deleteIf(column(COL1).isEqualToInt(1)).build()),
                        delete)))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    Arrays.asList(
                        buildDeleteWithCondition(
                            deleteIf(column(COL1).isGreaterThanOrEqualToInt(1)).build()),
                        delete)))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    Arrays.asList(
                        buildDeleteWithCondition(deleteIf(column(COL1).isNullInt()).build()),
                        delete)))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    Arrays.asList(
                        buildDeleteWithCondition(
                            deleteIf(column(COL2).isEqualToBoolean(true)).build()),
                        delete)))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    Arrays.asList(
                        buildDeleteWithCondition(
                            deleteIf(column(COL2).isNotEqualToBoolean(true)).build()),
                        delete)))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    Arrays.asList(
                        buildDeleteWithCondition(deleteIf(column(COL2).isNullBoolean()).build()),
                        delete)))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    Arrays.asList(
                        buildDeleteWithCondition(deleteIf(column(COL2).isNotNullBoolean()).build()),
                        delete)))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    Arrays.asList(
                        buildDeleteWithCondition(
                            deleteIf(column(COL2).isGreaterThanBoolean(false)).build()),
                        delete)))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    Arrays.asList(
                        buildDeleteWithCondition(
                            deleteIf(column(COL2).isLessThanOrEqualToBoolean(true)).build()),
                        delete)))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    Arrays.asList(
                        buildDeleteWithCondition(
                            deleteIf(column(COL4).isEqualToBlob(new byte[] {1, 2, 3})).build()),
                        delete)))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    Arrays.asList(
                        buildDeleteWithCondition(
                            deleteIf(column(COL4).isNotEqualToBlob(new byte[] {1, 2, 3})).build()),
                        delete)))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    Arrays.asList(
                        buildDeleteWithCondition(deleteIf(column(COL4).isNullBlob()).build()),
                        delete)))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    Arrays.asList(
                        buildDeleteWithCondition(deleteIf(column(COL4).isNotNullBlob()).build()),
                        delete)))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    Arrays.asList(
                        buildDeleteWithCondition(
                            deleteIf(column(COL4).isGreaterThanBlob(new byte[] {1, 2, 3})).build()),
                        delete)))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    Arrays.asList(
                        buildDeleteWithCondition(
                            deleteIf(column(COL4).isLessThanOrEqualToBlob(new byte[] {1, 2, 3}))
                                .build()),
                        delete)))
        .doesNotThrowAnyException();
  }

  @Test
  public void check_PutGiven_WhenTextColumnExceedsMaxLength_ShouldThrowIllegalArgumentException()
      throws Exception {
    // Arrange
    when(metadataManager.getTableMetadata(any())).thenReturn(TABLE_METADATA1);

    // Temporarily set MAX_STRING_LENGTH_ALLOWED to a small value for testing
    Field field = Serializer.class.getDeclaredField("MAX_STRING_LENGTH_ALLOWED");
    field.setAccessible(true);

    Field modifiersField = Field.class.getDeclaredField("modifiers");
    modifiersField.setAccessible(true);
    modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);

    Integer originalValue = (Integer) field.get(null);
    try {
      field.set(null, 10);

      Put put =
          Put.newBuilder()
              .namespace(NAMESPACE_NAME)
              .table(TABLE_NAME)
              .partitionKey(Key.ofInt(PKEY1, 0))
              .clusteringKey(Key.ofInt(CKEY1, 0))
              .textValue(COL3, "12345678901") // 11 characters, exceeds limit of 10
              .build();

      // Act Assert
      assertThatThrownBy(() -> operationChecker.check(put))
          .isInstanceOf(IllegalArgumentException.class);
    } finally {
      field.set(null, originalValue);
    }
  }

  @Test
  public void check_PutGiven_WhenBlobColumnExceedsMaxLength_ShouldThrowIllegalArgumentException()
      throws Exception {
    // Arrange
    when(metadataManager.getTableMetadata(any())).thenReturn(TABLE_METADATA1);

    // Temporarily set MAX_STRING_LENGTH_ALLOWED to a small value for testing
    Field field = Serializer.class.getDeclaredField("MAX_STRING_LENGTH_ALLOWED");
    field.setAccessible(true);

    Field modifiersField = Field.class.getDeclaredField("modifiers");
    modifiersField.setAccessible(true);
    modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);

    Integer originalValue = (Integer) field.get(null);
    try {
      field.set(null, 10);

      // 9 bytes -> Base64 encoded length = ((9 + 2) / 3) * 4 = 12, which exceeds limit of 10
      byte[] blob = new byte[9];
      Put put =
          Put.newBuilder()
              .namespace(NAMESPACE_NAME)
              .table(TABLE_NAME)
              .partitionKey(Key.ofInt(PKEY1, 0))
              .clusteringKey(Key.ofInt(CKEY1, 0))
              .blobValue(COL4, blob)
              .build();

      // Act Assert
      assertThatThrownBy(() -> operationChecker.check(put))
          .isInstanceOf(IllegalArgumentException.class);
    } finally {
      field.set(null, originalValue);
    }
  }

  private Put buildPutWithCondition(MutationCondition condition) {
    return Put.newBuilder()
        .namespace(NAMESPACE_NAME)
        .table(TABLE_NAME)
        .partitionKey(Key.ofInt(PKEY1, 0))
        .clusteringKey(Key.ofInt(CKEY1, 1))
        .intValue(COL1, 1)
        .condition(condition)
        .build();
  }

  private Delete buildDeleteWithCondition(MutationCondition condition) {
    return Delete.newBuilder()
        .namespace(NAMESPACE_NAME)
        .table(TABLE_NAME)
        .partitionKey(Key.ofInt(PKEY1, 0))
        .clusteringKey(Key.ofInt(CKEY1, 1))
        .condition(condition)
        .build();
  }
}
