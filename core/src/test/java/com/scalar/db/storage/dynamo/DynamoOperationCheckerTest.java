package com.scalar.db.storage.dynamo;

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
import com.scalar.db.api.MutationCondition;
import com.scalar.db.api.Put;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import java.util.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

public class DynamoOperationCheckerTest {

  private static final String NAMESPACE_NAME = "n1";
  private static final String TABLE_NAME = "t1";
  private static final String PKEY1 = "p1";
  private static final String CKEY1 = "c1";
  private static final String COL1 = "v1";
  private static final String COL2 = "v2";
  private static final String COL3 = "v3";
  private static final String COL4 = "v4";
  @Mock private DatabaseConfig databaseConfig;
  @Mock private TableMetadataManager metadataManager;
  private DynamoOperationChecker operationChecker;

  @BeforeEach
  public void setUp() throws Exception {
    openMocks(this).close();
    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn(PKEY1, DataType.INT)
            .addColumn(CKEY1, DataType.INT)
            .addColumn(COL1, DataType.INT)
            .addColumn(COL2, DataType.BOOLEAN)
            .addColumn(COL3, DataType.TEXT)
            .addColumn(COL4, DataType.BLOB)
            .addPartitionKey(PKEY1)
            .addClusteringKey(CKEY1)
            .addSecondaryIndex(COL1)
            .addSecondaryIndex(COL3)
            .addSecondaryIndex(COL4)
            .build();
    when(metadataManager.getTableMetadata(any())).thenReturn(tableMetadata);
    operationChecker = new DynamoOperationChecker(databaseConfig, metadataManager);
  }

  @Test
  public void check_ForPutWithNullIndex_ShouldThrowIllegalArgumentException() {
    // Arrange
    Put put = new Put(Key.ofInt(PKEY1, 0), Key.ofInt(CKEY1, 0)).withIntValue(COL1, null);

    // Act Assert
    assertThatThrownBy(() -> operationChecker.check(put))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void check_ForPutWithNonNullIndex_ShouldDoNothing() {
    // Arrange
    Put put = new Put(Key.ofInt(PKEY1, 0), Key.ofInt(CKEY1, 0)).withIntValue(COL1, 1);

    // Act Assert
    assertThatCode(() -> operationChecker.check(put)).doesNotThrowAnyException();
  }

  @Test
  public void check_ForPutWithoutSettingIndex_ShouldDoNothing() {
    // Arrange
    Put put = new Put(Key.ofInt(PKEY1, 0), Key.ofInt(CKEY1, 0));

    // Act Assert
    assertThatCode(() -> operationChecker.check(put)).doesNotThrowAnyException();
  }

  @Test
  public void check_ForPutWithEmptyTextValueIndex_ShouldThrowIllegalArgumentException() {
    // Arrange
    Put put =
        Put.newBuilder()
            .namespace(NAMESPACE_NAME)
            .table(TABLE_NAME)
            .partitionKey(Key.ofInt(PKEY1, 0))
            .clusteringKey(Key.ofInt(CKEY1, 0))
            .textValue(COL3, "")
            .build();

    // Act Assert
    assertThatThrownBy(() -> operationChecker.check(put))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void check_ForPutWithEmptyBlobValueIndex_ShouldThrowIllegalArgumentException() {
    // Arrange
    Put put =
        Put.newBuilder()
            .namespace(NAMESPACE_NAME)
            .table(TABLE_NAME)
            .partitionKey(Key.ofInt(PKEY1, 0))
            .clusteringKey(Key.ofInt(CKEY1, 0))
            .blobValue(COL4, new byte[0])
            .build();

    // Act Assert
    assertThatThrownBy(() -> operationChecker.check(put))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void check_ForMutationsWithPutWithNullIndex_ShouldThrowIllegalArgumentException() {
    // Arrange
    Put putWithNullIndex =
        Put.newBuilder()
            .namespace(NAMESPACE_NAME)
            .table(TABLE_NAME)
            .partitionKey(Key.ofInt(PKEY1, 0))
            .clusteringKey(Key.ofInt(CKEY1, 0))
            .intValue(COL1, null)
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
    assertThatThrownBy(() -> operationChecker.check(Arrays.asList(putWithNullIndex, put)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void check_ForMutationsWithPutWithNonNullIndex_ShouldDoNothing() {
    // Arrange
    Put putWithNonNullIndex =
        Put.newBuilder()
            .namespace(NAMESPACE_NAME)
            .table(TABLE_NAME)
            .partitionKey(Key.ofInt(PKEY1, 0))
            .clusteringKey(Key.ofInt(CKEY1, 0))
            .intValue(COL1, 1)
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
    assertThatCode(() -> operationChecker.check(Arrays.asList(putWithNonNullIndex, put)))
        .doesNotThrowAnyException();
  }

  @Test
  public void check_ForMutationsWithPutWithoutSettingIndex_ShouldDoNothing() {
    // Arrange
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
  public void check_ForPutWithCondition_ShouldBehaveProperly() {
    // Arrange

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
    assertThatThrownBy(
            () ->
                operationChecker.check(
                    buildPutWithCondition(putIf(column(COL2).isGreaterThanBoolean(false)).build())))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () ->
                operationChecker.check(
                    buildPutWithCondition(
                        putIf(column(COL2).isLessThanOrEqualToBoolean(true)).build())))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void check_ForDeleteWithCondition_ShouldBehaveProperly() {
    // Arrange

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
    assertThatThrownBy(
            () ->
                operationChecker.check(
                    buildDeleteWithCondition(
                        deleteIf(column(COL2).isGreaterThanBoolean(false)).build())))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () ->
                operationChecker.check(
                    buildDeleteWithCondition(
                        deleteIf(column(COL2).isLessThanOrEqualToBoolean(true)).build())))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void check_ForMutationsWithPutWithCondition_ShouldBehaveProperly() {
    // Arrange
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
    assertThatThrownBy(
            () ->
                operationChecker.check(
                    Arrays.asList(
                        buildPutWithCondition(
                            putIf(column(COL2).isGreaterThanBoolean(false)).build()),
                        put)))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () ->
                operationChecker.check(
                    Arrays.asList(
                        buildPutWithCondition(
                            putIf(column(COL2).isLessThanOrEqualToBoolean(true)).build()),
                        put)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void check_ForMutationsWithDeleteWithCondition_ShouldBehaveProperly() {
    // Arrange
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
    assertThatThrownBy(
            () ->
                operationChecker.check(
                    Arrays.asList(
                        buildDeleteWithCondition(
                            deleteIf(column(COL2).isGreaterThanBoolean(false)).build()),
                        delete)))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () ->
                operationChecker.check(
                    Arrays.asList(
                        buildDeleteWithCondition(
                            deleteIf(column(COL2).isLessThanOrEqualToBoolean(true)).build()),
                        delete)))
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
