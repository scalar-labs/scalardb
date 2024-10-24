package com.scalar.db.storage.cosmos;

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
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

public class CosmosOperationCheckerTest {

  private static final String NAMESPACE_NAME = "n1";
  private static final String TABLE_NAME = "t1";
  private static final String PKEY1 = "p1";
  private static final String CKEY1 = "c1";
  private static final String COL1 = "v1";
  private static final String COL2 = "v2";

  private static final TableMetadata TABLE_METADATA1 =
      TableMetadata.newBuilder()
          .addColumn(PKEY1, DataType.INT)
          .addColumn(CKEY1, DataType.INT)
          .addColumn(COL1, DataType.INT)
          .addColumn(COL2, DataType.BLOB)
          .addPartitionKey(PKEY1)
          .addClusteringKey(CKEY1)
          .addSecondaryIndex(COL1)
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
  private CosmosOperationChecker operationChecker;

  @BeforeEach
  public void setUp() throws Exception {
    openMocks(this).close();

    operationChecker = new CosmosOperationChecker(databaseConfig, metadataManager);
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
                    buildPutWithCondition(
                        putIf(column(COL2).isEqualToBlob("blob".getBytes(StandardCharsets.UTF_8)))
                            .build())))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    buildPutWithCondition(
                        putIf(
                                column(COL2)
                                    .isNotEqualToBlob("blob".getBytes(StandardCharsets.UTF_8)))
                            .build())))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    buildPutWithCondition(putIf(column(COL2).isNullBlob()).build())))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    buildPutWithCondition(putIf(column(COL2).isNotNullBlob()).build())))
        .doesNotThrowAnyException();
    assertThatThrownBy(
            () ->
                operationChecker.check(
                    buildPutWithCondition(
                        putIf(
                                column(COL2)
                                    .isGreaterThanBlob("blob".getBytes(StandardCharsets.UTF_8)))
                            .build())))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () ->
                operationChecker.check(
                    buildPutWithCondition(
                        putIf(
                                column(COL2)
                                    .isLessThanOrEqualToBlob(
                                        "blob".getBytes(StandardCharsets.UTF_8)))
                            .build())))
        .isInstanceOf(IllegalArgumentException.class);
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
                        deleteIf(
                                column(COL2).isEqualToBlob("blob".getBytes(StandardCharsets.UTF_8)))
                            .build())))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    buildDeleteWithCondition(
                        deleteIf(
                                column(COL2)
                                    .isNotEqualToBlob("blob".getBytes(StandardCharsets.UTF_8)))
                            .build())))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    buildDeleteWithCondition(deleteIf(column(COL2).isNullBlob()).build())))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    buildDeleteWithCondition(deleteIf(column(COL2).isNotNullBlob()).build())))
        .doesNotThrowAnyException();
    assertThatThrownBy(
            () ->
                operationChecker.check(
                    buildDeleteWithCondition(
                        deleteIf(
                                column(COL2)
                                    .isGreaterThanBlob("blob".getBytes(StandardCharsets.UTF_8)))
                            .build())))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () ->
                operationChecker.check(
                    buildDeleteWithCondition(
                        deleteIf(
                                column(COL2)
                                    .isLessThanOrEqualToBlob(
                                        "blob".getBytes(StandardCharsets.UTF_8)))
                            .build())))
        .isInstanceOf(IllegalArgumentException.class);
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
                        buildPutWithCondition(
                            putIf(
                                    column(COL2)
                                        .isEqualToBlob("blob".getBytes(StandardCharsets.UTF_8)))
                                .build()),
                        put)))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    Arrays.asList(
                        buildPutWithCondition(
                            putIf(
                                    column(COL2)
                                        .isNotEqualToBlob("blob".getBytes(StandardCharsets.UTF_8)))
                                .build()),
                        put)))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    Arrays.asList(
                        buildPutWithCondition(putIf(column(COL2).isNullBlob()).build()), put)))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    Arrays.asList(
                        buildPutWithCondition(putIf(column(COL2).isNotNullBlob()).build()), put)))
        .doesNotThrowAnyException();
    assertThatThrownBy(
            () ->
                operationChecker.check(
                    Arrays.asList(
                        buildPutWithCondition(
                            putIf(
                                    column(COL2)
                                        .isGreaterThanBlob("blob".getBytes(StandardCharsets.UTF_8)))
                                .build()),
                        put)))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () ->
                operationChecker.check(
                    Arrays.asList(
                        buildPutWithCondition(
                            putIf(
                                    column(COL2)
                                        .isLessThanOrEqualToBlob(
                                            "blob".getBytes(StandardCharsets.UTF_8)))
                                .build()),
                        put)))
        .isInstanceOf(IllegalArgumentException.class);
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
                            deleteIf(
                                    column(COL2)
                                        .isEqualToBlob("blob".getBytes(StandardCharsets.UTF_8)))
                                .build()),
                        delete)))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    Arrays.asList(
                        buildDeleteWithCondition(
                            deleteIf(
                                    column(COL2)
                                        .isNotEqualToBlob("blob".getBytes(StandardCharsets.UTF_8)))
                                .build()),
                        delete)))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    Arrays.asList(
                        buildDeleteWithCondition(deleteIf(column(COL2).isNullBlob()).build()),
                        delete)))
        .doesNotThrowAnyException();
    assertThatCode(
            () ->
                operationChecker.check(
                    Arrays.asList(
                        buildDeleteWithCondition(deleteIf(column(COL2).isNotNullBlob()).build()),
                        delete)))
        .doesNotThrowAnyException();
    assertThatThrownBy(
            () ->
                operationChecker.check(
                    Arrays.asList(
                        buildDeleteWithCondition(
                            deleteIf(
                                    column(COL2)
                                        .isGreaterThanBlob("blob".getBytes(StandardCharsets.UTF_8)))
                                .build()),
                        delete)))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () ->
                operationChecker.check(
                    Arrays.asList(
                        buildDeleteWithCondition(
                            deleteIf(
                                    column(COL2)
                                        .isLessThanOrEqualToBlob(
                                            "blob".getBytes(StandardCharsets.UTF_8)))
                                .build()),
                        delete)))
        .isInstanceOf(IllegalArgumentException.class);
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
            .partitionKey(Key.ofText(PKEY1, "a:b"))
            .clusteringKey(Key.ofText(CKEY1, "ab"))
            .build();
    Get get3 =
        Get.newBuilder()
            .namespace(NAMESPACE_NAME)
            .table(TABLE_NAME)
            .partitionKey(Key.ofText(PKEY1, "ab"))
            .clusteringKey(Key.ofText(CKEY1, "a:b"))
            .build();
    Get get4 =
        Get.newBuilder()
            .namespace(NAMESPACE_NAME)
            .table(TABLE_NAME)
            .partitionKey(Key.ofText(PKEY1, "a/b"))
            .clusteringKey(Key.ofText(CKEY1, "ab"))
            .build();
    Get get5 =
        Get.newBuilder()
            .namespace(NAMESPACE_NAME)
            .table(TABLE_NAME)
            .partitionKey(Key.ofText(PKEY1, "ab"))
            .clusteringKey(Key.ofText(CKEY1, "a/b"))
            .build();
    Get get6 =
        Get.newBuilder()
            .namespace(NAMESPACE_NAME)
            .table(TABLE_NAME)
            .partitionKey(Key.ofText(PKEY1, "a\\b"))
            .clusteringKey(Key.ofText(CKEY1, "ab"))
            .build();
    Get get7 =
        Get.newBuilder()
            .namespace(NAMESPACE_NAME)
            .table(TABLE_NAME)
            .partitionKey(Key.ofText(PKEY1, "ab"))
            .clusteringKey(Key.ofText(CKEY1, "a\\b"))
            .build();
    Get get8 =
        Get.newBuilder()
            .namespace(NAMESPACE_NAME)
            .table(TABLE_NAME)
            .partitionKey(Key.ofText(PKEY1, "a#b"))
            .clusteringKey(Key.ofText(CKEY1, "ab"))
            .build();
    Get get9 =
        Get.newBuilder()
            .namespace(NAMESPACE_NAME)
            .table(TABLE_NAME)
            .partitionKey(Key.ofText(PKEY1, "ab"))
            .clusteringKey(Key.ofText(CKEY1, "a#b"))
            .build();
    Get get10 =
        Get.newBuilder()
            .namespace(NAMESPACE_NAME)
            .table(TABLE_NAME)
            .partitionKey(Key.ofText(PKEY1, "a?b"))
            .clusteringKey(Key.ofText(CKEY1, "ab"))
            .build();
    Get get11 =
        Get.newBuilder()
            .namespace(NAMESPACE_NAME)
            .table(TABLE_NAME)
            .partitionKey(Key.ofText(PKEY1, "ab"))
            .clusteringKey(Key.ofText(CKEY1, "a?b"))
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
    assertThatThrownBy(() -> operationChecker.check(get6))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> operationChecker.check(get7))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> operationChecker.check(get8))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> operationChecker.check(get9))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> operationChecker.check(get10))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> operationChecker.check(get11))
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
            .partitionKey(Key.ofText(PKEY1, "a:b"))
            .start(Key.ofText(CKEY1, "ab"))
            .end(Key.ofText(CKEY1, "ab"))
            .build();
    Scan scan3 =
        Scan.newBuilder()
            .namespace(NAMESPACE_NAME)
            .table(TABLE_NAME)
            .partitionKey(Key.ofText(PKEY1, "ab"))
            .start(Key.ofText(CKEY1, "a:b"))
            .end(Key.ofText(CKEY1, "ab"))
            .build();
    Scan scan4 =
        Scan.newBuilder()
            .namespace(NAMESPACE_NAME)
            .table(TABLE_NAME)
            .partitionKey(Key.ofText(PKEY1, "ab"))
            .start(Key.ofText(CKEY1, "ab"))
            .end(Key.ofText(CKEY1, "a:b"))
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
            .partitionKey(Key.ofText(PKEY1, "a:b"))
            .clusteringKey(Key.ofText(CKEY1, "ab"))
            .build();
    Put put3 =
        Put.newBuilder()
            .namespace(NAMESPACE_NAME)
            .table(TABLE_NAME)
            .partitionKey(Key.ofText(PKEY1, "ab"))
            .clusteringKey(Key.ofText(CKEY1, "a:b"))
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
            .partitionKey(Key.ofText(PKEY1, "a:b"))
            .clusteringKey(Key.ofText(CKEY1, "ab"))
            .build();
    Delete delete3 =
        Delete.newBuilder()
            .namespace(NAMESPACE_NAME)
            .table(TABLE_NAME)
            .partitionKey(Key.ofText(PKEY1, "ab"))
            .clusteringKey(Key.ofText(CKEY1, "a:b"))
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
            .partitionKey(Key.ofText(PKEY1, "a:b"))
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
            .partitionKey(Key.ofText(PKEY1, "a:b"))
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
