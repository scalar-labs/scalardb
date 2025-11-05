package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DeleteIf;
import com.scalar.db.api.DeleteIfExists;
import com.scalar.db.api.Get;
import com.scalar.db.api.MutationCondition;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.PutIfExists;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.api.Scan;
import com.scalar.db.api.ScanAll;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.checker.ConditionChecker;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class ConsensusCommitOperationCheckerTest {
  private static final String ANY_COL_1 = "any_col_1";
  private static final String ANY_COL_2 = "any_col_2";
  private static final String ANY_METADATA_COL_1 = "any_metadata_col_1";
  private static final String ANY_METADATA_COL_2 = "any_metadata_col_2";
  @Mock private TransactionTableMetadataManager metadataManager;
  @Mock private Put put;
  @Mock private Delete delete;
  @Mock private TransactionTableMetadata tableMetadata;
  @Mock private ConditionChecker conditionChecker;
  private ConsensusCommitOperationChecker checker;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
    checker = spy(new ConsensusCommitOperationChecker(metadataManager, false));
    when(checker.createConditionChecker(any())).thenReturn(conditionChecker);
    when(metadataManager.getTransactionTableMetadata(any())).thenReturn(tableMetadata);
    LinkedHashSet<String> metadataColumns = new LinkedHashSet<>();
    metadataColumns.add(ANY_METADATA_COL_1);
    metadataColumns.add(ANY_METADATA_COL_2);
    when(tableMetadata.getTransactionMetaColumnNames()).thenReturn(metadataColumns);
  }

  @ParameterizedTest
  @ValueSource(classes = {DeleteIf.class, DeleteIfExists.class})
  public void checkForPut_WithNonAllowedCondition_ShouldThrowIllegalArgumentException(
      Class<? extends MutationCondition> deleteConditonClass) {
    // Arrange
    when(put.getCondition()).thenReturn(Optional.of(mock(deleteConditonClass)));

    // Act Assert
    assertThatThrownBy(() -> checker.check(put)).isInstanceOf(IllegalArgumentException.class);
  }

  @ParameterizedTest
  @ValueSource(classes = {PutIf.class, PutIfExists.class, PutIfNotExists.class})
  public void checkForPut_WithAllowedCondition_ShouldCallConditionChecker(
      Class<? extends MutationCondition> putConditionClass) {
    // Arrange
    MutationCondition condition = mock(putConditionClass);
    when(put.getCondition()).thenReturn(Optional.of(condition));

    // Act Assert
    assertThatCode(() -> checker.check(put)).doesNotThrowAnyException();
    verify(conditionChecker).check(condition, true);
  }

  @Test
  public void checkForPut_ThatMutatesMetadataColumns_ShouldThrowIllegalArgumentException()
      throws ExecutionException {
    // Arrange
    String fullTableName = "ns.tbl";
    Set<String> columns = ImmutableSet.of(ANY_COL_1, ANY_METADATA_COL_1, ANY_COL_2);
    when(put.forFullTableName()).thenReturn(Optional.of(fullTableName));
    when(put.getContainedColumnNames()).thenReturn(columns);

    // Act Assert
    assertThatThrownBy(() -> checker.check(put))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(fullTableName)
        .hasMessageContaining(ANY_METADATA_COL_1);
    verify(metadataManager).getTransactionTableMetadata(put);
  }

  @Test
  public void checkForPut_ThatDoNotMutateMetadataColumns_ShouldDoNothing()
      throws ExecutionException {
    // Arrange
    Set<String> columns = ImmutableSet.of(ANY_COL_1, ANY_COL_2);
    when(put.getContainedColumnNames()).thenReturn(columns);

    // Act Assert
    assertThatCode(() -> checker.check(put)).doesNotThrowAnyException();
    verify(metadataManager).getTransactionTableMetadata(put);
  }

  @Test
  public void
      checkForPut_WithConditionThatTargetMetadataColumns_ShouldThrowIllegalArgumentException()
          throws ExecutionException {
    // Arrange
    MutationCondition condition =
        ConditionBuilder.putIf(ConditionBuilder.column(ANY_COL_1).isNullInt())
            .and(ConditionBuilder.column(ANY_METADATA_COL_1).isNullText())
            .build();
    when(put.getCondition()).thenReturn(Optional.of(condition));
    when(put.forFullTableName()).thenReturn(Optional.of("ns.tbl"));

    // Act Assert
    assertThatThrownBy(() -> checker.check(put)).isInstanceOf(IllegalArgumentException.class);
    verify(metadataManager).getTransactionTableMetadata(put);
  }

  @Test
  public void checkForPut_WithConditionThatDoNotTargetMetadataColumns_ShouldCallConditionChecker()
      throws ExecutionException {
    // Arrange
    MutationCondition condition =
        ConditionBuilder.putIf(ConditionBuilder.column(ANY_COL_1).isNullInt())
            .and(ConditionBuilder.column(ANY_COL_2).isNullText())
            .build();
    when(put.getCondition()).thenReturn(Optional.of(condition));

    // Act Assert
    assertThatCode(() -> checker.check(put)).doesNotThrowAnyException();
    verify(metadataManager).getTransactionTableMetadata(put);
    verify(conditionChecker).check(condition, true);
  }

  @ParameterizedTest
  @ValueSource(classes = {PutIf.class, PutIfExists.class, PutIfNotExists.class})
  public void checkForDelete_WithNonAllowedCondition_ShouldThrowIllegalArgumentException(
      Class<? extends MutationCondition> putConditionClass) {
    // Arrange
    when(delete.getCondition()).thenReturn(Optional.of(mock(putConditionClass)));

    // Act Assert
    assertThatThrownBy(() -> checker.check(delete)).isInstanceOf(IllegalArgumentException.class);
  }

  @ParameterizedTest
  @ValueSource(classes = {DeleteIf.class, DeleteIfExists.class})
  public void checkForDelete_WithAllowedCondition_ShouldCheckCondition(
      Class<? extends MutationCondition> deleteConditionClass) {
    // Arrange
    MutationCondition condition = mock(deleteConditionClass);
    when(delete.getCondition()).thenReturn(Optional.of(condition));

    // Act Assert
    assertThatCode(() -> checker.check(delete)).doesNotThrowAnyException();
    verify(conditionChecker).check(condition, false);
  }

  @Test
  public void
      checkForDelete_WithConditionThatTargetMetadataColumns_ShouldThrowIllegalArgumentException()
          throws ExecutionException {
    // Arrange
    MutationCondition condition =
        ConditionBuilder.deleteIf(ConditionBuilder.column(ANY_COL_1).isNullInt())
            .and(ConditionBuilder.column(ANY_METADATA_COL_1).isNullText())
            .build();
    when(delete.getCondition()).thenReturn(Optional.of(condition));
    when(delete.forFullTableName()).thenReturn(Optional.of("ns.tbl"));

    // Act Assert
    assertThatThrownBy(() -> checker.check(delete)).isInstanceOf(IllegalArgumentException.class);
    verify(metadataManager).getTransactionTableMetadata(delete);
  }

  @Test
  public void
      checkForDelete_WithConditionThatDoNotTargetMetadataColumns_ShouldCallConditionChecker()
          throws ExecutionException {
    // Arrange
    MutationCondition condition =
        ConditionBuilder.deleteIf(ConditionBuilder.column(ANY_COL_1).isNullInt())
            .and(ConditionBuilder.column(ANY_COL_2).isNullText())
            .build();
    when(delete.getCondition()).thenReturn(Optional.of(condition));

    // Act Assert
    assertThatCode(() -> checker.check(delete)).doesNotThrowAnyException();
    verify(metadataManager).getTransactionTableMetadata(delete);
    verify(conditionChecker).check(condition, false);
  }

  @Test
  public void checkForGet_WithMetadataColumnsInProjection_ShouldThrowIllegalArgumentException() {
    // Arrange
    Get get =
        Get.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofInt("pk", 1))
            .projections(ANY_COL_1, ANY_METADATA_COL_1)
            .build();
    TransactionContext context =
        new TransactionContext("txId", null, Isolation.SNAPSHOT, false, false);

    // Act Assert
    assertThatThrownBy(() -> checker.check(get, context))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("ns.tbl")
        .hasMessageContaining(ANY_METADATA_COL_1);
  }

  @Test
  public void checkForGet_WithMetadataColumnsInCondition_ShouldThrowIllegalArgumentException() {
    // Arrange
    Get get =
        Get.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofInt("pk", 1))
            .where(ConditionBuilder.column(ANY_METADATA_COL_1).isEqualToInt(10))
            .build();
    TransactionContext context =
        new TransactionContext("txId", null, Isolation.SNAPSHOT, false, false);

    // Act Assert
    assertThatThrownBy(() -> checker.check(get, context))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void checkForGet_IncludeMetadataEnabled_ShouldNotThrowException() {
    // Arrange
    checker = spy(new ConsensusCommitOperationChecker(metadataManager, true));

    Get get =
        Get.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofInt("pk", 1))
            .projections(ANY_COL_1, ANY_METADATA_COL_1)
            .where(ConditionBuilder.column(ANY_METADATA_COL_1).isEqualToInt(10))
            .build();
    TransactionContext context =
        new TransactionContext("txId", null, Isolation.SNAPSHOT, false, false);

    // Act Assert
    assertThatCode(() -> checker.check(get, context)).doesNotThrowAnyException();
  }

  @Test
  public void checkForGet_WithSecondaryIndexInSerializable_ShouldThrowIllegalArgumentException() {
    // Arrange
    TableMetadata metadata =
        TableMetadata.newBuilder()
            .addColumn("pk", DataType.INT)
            .addColumn("idx", DataType.INT)
            .addPartitionKey("pk")
            .addSecondaryIndex("idx")
            .build();
    when(tableMetadata.getTableMetadata()).thenReturn(metadata);

    Get get = Get.newBuilder().namespace("ns").table("tbl").indexKey(Key.ofInt("idx", 100)).build();
    TransactionContext context =
        new TransactionContext("txId", null, Isolation.SERIALIZABLE, false, false);

    // Act Assert
    assertThatThrownBy(() -> checker.check(get, context))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void checkForGet_ValidGet_ShouldNotThrowException() {
    // Arrange
    Get get =
        Get.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofInt("pk", 1))
            .projections(ANY_COL_1, ANY_COL_2)
            .build();
    TransactionContext context =
        new TransactionContext("txId", null, Isolation.SNAPSHOT, false, false);

    // Act Assert
    assertThatCode(() -> checker.check(get, context)).doesNotThrowAnyException();
  }

  @Test
  public void checkForScan_WithMetadataColumnsInProjection_ShouldThrowIllegalArgumentException() {
    // Arrange
    Scan scan =
        Scan.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofInt("pk", 1))
            .projections(ANY_COL_1, ANY_METADATA_COL_1)
            .build();
    TransactionContext context =
        new TransactionContext("txId", null, Isolation.SNAPSHOT, false, false);

    // Act Assert
    assertThatThrownBy(() -> checker.check(scan, context))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("ns.tbl")
        .hasMessageContaining(ANY_METADATA_COL_1);
  }

  @Test
  public void checkForScan_WithMetadataColumnsInCondition_ShouldThrowIllegalArgumentException() {
    // Arrange
    Scan scan =
        Scan.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofInt("pk", 1))
            .where(ConditionBuilder.column(ANY_METADATA_COL_1).isEqualToInt(10))
            .build();
    TransactionContext context =
        new TransactionContext("txId", null, Isolation.SNAPSHOT, false, false);

    // Act Assert
    assertThatThrownBy(() -> checker.check(scan, context))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void checkForScan_WithMetadataColumnsInOrdering_ShouldThrowIllegalArgumentException() {
    // Arrange
    Scan scan =
        Scan.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofInt("pk", 1))
            .orderings(Scan.Ordering.asc(ANY_METADATA_COL_1))
            .build();
    TransactionContext context =
        new TransactionContext("txId", null, Isolation.SNAPSHOT, false, false);

    // Act Assert
    assertThatThrownBy(() -> checker.check(scan, context))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("ns.tbl")
        .hasMessageContaining(ANY_METADATA_COL_1);
  }

  @Test
  public void checkForScan_IncludeMetadataEnabled_ShouldNotThrowException() {
    // Arrange
    checker = spy(new ConsensusCommitOperationChecker(metadataManager, true));

    Scan scan =
        Scan.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofInt("pk", 1))
            .projections(ANY_COL_1, ANY_METADATA_COL_1)
            .where(ConditionBuilder.column(ANY_METADATA_COL_1).isEqualToInt(10))
            .orderings(Scan.Ordering.asc(ANY_METADATA_COL_1))
            .build();
    TransactionContext context =
        new TransactionContext("txId", null, Isolation.SNAPSHOT, false, false);

    // Act Assert
    assertThatCode(() -> checker.check(scan, context)).doesNotThrowAnyException();
  }

  @Test
  public void checkForScan_WithSecondaryIndexInSerializable_ShouldThrowIllegalArgumentException() {
    // Arrange
    TableMetadata metadata =
        TableMetadata.newBuilder()
            .addColumn("pk", DataType.INT)
            .addColumn("idx", DataType.INT)
            .addPartitionKey("pk")
            .addSecondaryIndex("idx")
            .build();
    when(tableMetadata.getTableMetadata()).thenReturn(metadata);

    Scan scan =
        Scan.newBuilder().namespace("ns").table("tbl").indexKey(Key.ofInt("idx", 100)).build();
    TransactionContext context =
        new TransactionContext("txId", null, Isolation.SERIALIZABLE, false, false);

    // Act Assert
    assertThatThrownBy(() -> checker.check(scan, context))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      checkForScan_ScanAllWithConditionOnIndexedColumnInSerializable_ShouldThrowIllegalArgumentException() {
    // Arrange
    Set<String> secondaryIndexNames = new LinkedHashSet<>(Collections.singletonList("idx_col"));
    when(tableMetadata.getSecondaryIndexNames()).thenReturn(secondaryIndexNames);

    Scan scan =
        ScanAll.newBuilder()
            .namespace("ns")
            .table("tbl")
            .all()
            .where(ConditionBuilder.column("idx_col").isEqualToInt(100))
            .build();
    TransactionContext context =
        new TransactionContext("txId", null, Isolation.SERIALIZABLE, false, false);

    // Act Assert
    assertThatThrownBy(() -> checker.check(scan, context))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void
      checkForScan_ScanAllWithConditionOnNonIndexedColumnInSerializable_ShouldNotThrowException() {
    // Arrange
    Set<String> secondaryIndexNames = new LinkedHashSet<>(Collections.singletonList("idx_col"));
    when(tableMetadata.getSecondaryIndexNames()).thenReturn(secondaryIndexNames);

    Scan scan =
        ScanAll.newBuilder()
            .namespace("ns")
            .table("tbl")
            .all()
            .where(ConditionBuilder.column("non_idx_col").isEqualToInt(100))
            .build();
    TransactionContext context =
        new TransactionContext("txId", null, Isolation.SERIALIZABLE, false, false);

    // Act Assert
    assertThatCode(() -> checker.check(scan, context)).doesNotThrowAnyException();
  }

  @Test
  public void
      checkForScan_RegularScanWithConditionOnIndexedColumnInSerializable_ShouldNotThrowException() {
    // Arrange
    TableMetadata metadata =
        TableMetadata.newBuilder()
            .addColumn("pk", DataType.INT)
            .addColumn("idx_col", DataType.INT)
            .addPartitionKey("pk")
            .addSecondaryIndex("idx_col")
            .build();
    Set<String> secondaryIndexNames = new LinkedHashSet<>(Collections.singletonList("idx_col"));
    when(tableMetadata.getTableMetadata()).thenReturn(metadata);
    when(tableMetadata.getSecondaryIndexNames()).thenReturn(secondaryIndexNames);

    Scan scan =
        Scan.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofInt("pk", 1))
            .where(ConditionBuilder.column("idx_col").isEqualToInt(100))
            .build();
    TransactionContext context =
        new TransactionContext("txId", null, Isolation.SERIALIZABLE, false, false);

    // Act Assert
    assertThatCode(() -> checker.check(scan, context)).doesNotThrowAnyException();
  }

  @Test
  public void checkForScan_ValidScan_ShouldNotThrowException() {
    // Arrange
    Scan scan =
        Scan.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofInt("pk", 1))
            .projections(ANY_COL_1, ANY_COL_2)
            .build();
    TransactionContext context =
        new TransactionContext("txId", null, Isolation.SNAPSHOT, false, false);

    // Act Assert
    assertThatCode(() -> checker.check(scan, context)).doesNotThrowAnyException();
  }
}
