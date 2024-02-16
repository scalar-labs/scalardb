package com.scalar.db.transaction.consensuscommit;

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
import com.scalar.db.api.MutationCondition;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.PutIfExists;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.common.checker.ConditionChecker;
import com.scalar.db.exception.storage.ExecutionException;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class ConsensusCommitMutationOperationCheckerTest {
  private static final String ANY_COL_1 = "any_col_1";
  private static final String ANY_COL_2 = "any_col_2";
  private static final String ANY_METADATA_COL_1 = "any_metadata_col_1";
  private static final String ANY_METADATA_COL_2 = "any_metadata_col_2";
  @Mock private TransactionTableMetadataManager metadataManager;
  @Mock private Put put;
  @Mock private Delete delete;
  @Mock private TransactionTableMetadata tableMetadata;
  @Mock private ConditionChecker conditionChecker;
  private ConsensusCommitMutationOperationChecker checker;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
    checker = spy(new ConsensusCommitMutationOperationChecker(metadataManager));
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
    Assertions.assertThatThrownBy(() -> checker.check(put))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @ParameterizedTest
  @ValueSource(classes = {PutIf.class, PutIfExists.class, PutIfNotExists.class})
  public void checkForPut_WithAllowedCondition_ShouldCallConditionChecker(
      Class<? extends MutationCondition> putConditionClass) {
    // Arrange
    MutationCondition condition = mock(putConditionClass);
    when(put.getCondition()).thenReturn(Optional.of(condition));

    // Act Assert
    org.junit.jupiter.api.Assertions.assertDoesNotThrow(() -> checker.check(put));
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
    Assertions.assertThatThrownBy(() -> checker.check(put))
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
    org.junit.jupiter.api.Assertions.assertDoesNotThrow(() -> checker.check(put));
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

    // Act Assert
    Assertions.assertThatThrownBy(() -> checker.check(put))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(ANY_METADATA_COL_1);
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
    org.junit.jupiter.api.Assertions.assertDoesNotThrow(() -> checker.check(put));
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
    Assertions.assertThatThrownBy(() -> checker.check(delete))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @ParameterizedTest
  @ValueSource(classes = {DeleteIf.class, DeleteIfExists.class})
  public void checkForDelete_WithAllowedCondition_ShouldCheckCondition(
      Class<? extends MutationCondition> deleteConditionClass) {
    // Arrange
    MutationCondition condition = mock(deleteConditionClass);
    when(delete.getCondition()).thenReturn(Optional.of(condition));

    // Act Assert
    org.junit.jupiter.api.Assertions.assertDoesNotThrow(() -> checker.check(delete));
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

    // Act Assert
    Assertions.assertThatThrownBy(() -> checker.check(delete))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(ANY_METADATA_COL_1);
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
    org.junit.jupiter.api.Assertions.assertDoesNotThrow(() -> checker.check(delete));
    verify(metadataManager).getTransactionTableMetadata(delete);
    verify(conditionChecker).check(condition, false);
  }
}
