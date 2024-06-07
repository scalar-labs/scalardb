package com.scalar.db.transaction.singlecrudoperation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.scalar.db.common.DecoratedDistributedTransactionManager;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitManager;
import org.junit.jupiter.api.Test;

public class SingleCrudOperationTransactionUtilsTest {

  @Test
  public void
      isSingleCrudOperationTransactionManager_SingleCrudOperationTransactionManagerGiven_ShouldReturnTrue() {
    // Arrange

    // Act
    boolean result =
        SingleCrudOperationTransactionUtils.isSingleCrudOperationTransactionManager(
            mock(SingleCrudOperationTransactionManager.class));

    // Assert
    assertThat(result).isTrue();
  }

  @Test
  public void
      isSingleCrudOperationTransactionManager_ConsensusCommitManagerGiven_ShouldReturnFalse() {
    // Arrange

    // Act
    boolean result =
        SingleCrudOperationTransactionUtils.isSingleCrudOperationTransactionManager(
            mock(ConsensusCommitManager.class));

    // Assert
    assertThat(result).isFalse();
  }

  @Test
  public void
      isSingleCrudOperationTransactionManager_DecoratedDistributedTransactionManagerWrappingSingleCrudOperationTransactionManagerGiven_ShouldReturnTrue() {
    // Arrange
    DecoratedDistributedTransactionManager decoratedTransactionManager =
        mock(DecoratedDistributedTransactionManager.class);
    when(decoratedTransactionManager.getOriginalTransactionManager())
        .thenReturn(mock(SingleCrudOperationTransactionManager.class));

    // Act
    boolean result =
        SingleCrudOperationTransactionUtils.isSingleCrudOperationTransactionManager(
            decoratedTransactionManager);

    // Assert
    assertThat(result).isTrue();
  }

  @Test
  public void
      isSingleCrudOperationTransactionManager_DecoratedDistributedTransactionManagerWrappingConsensusCommitManagerGiven_ShouldReturnFalse() {
    // Arrange
    DecoratedDistributedTransactionManager decoratedTransactionManager =
        mock(DecoratedDistributedTransactionManager.class);
    when(decoratedTransactionManager.getOriginalTransactionManager())
        .thenReturn(mock(ConsensusCommitManager.class));

    // Act
    boolean result =
        SingleCrudOperationTransactionUtils.isSingleCrudOperationTransactionManager(
            decoratedTransactionManager);

    // Assert
    assertThat(result).isFalse();
  }
}
