package com.scalar.db.transaction.autocommit;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.common.DecoratedDistributedTransaction;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.transaction.TransactionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class AutoCommitTransactionManagerTest {

  @Mock private DistributedStorage storage;
  @Mock private DatabaseConfig databaseConfig;

  private AutoCommitTransactionManager transactionManager;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
    transactionManager = new AutoCommitTransactionManager(databaseConfig, storage);
  }

  @Test
  public void begin_ShouldReturnAutoCommitTransaction() throws TransactionException {
    // Arrange

    // Act
    DistributedTransaction transaction = transactionManager.begin();

    // Assert
    assertThat(transaction.getId()).isNotNull();

    assertThat(transaction).isInstanceOf(DecoratedDistributedTransaction.class);
    DecoratedDistributedTransaction decoratedTransaction =
        (DecoratedDistributedTransaction) transaction;
    assertThat(decoratedTransaction.getOriginalTransaction())
        .isInstanceOf(AutoCommitTransaction.class);
    assertThat(((AutoCommitTransaction) decoratedTransaction.getOriginalTransaction()).getStorage())
        .isEqualTo(storage);
  }

  @Test
  public void begin_WithTransactionId_ShouldReturnAutoCommitTransaction()
      throws TransactionException {
    // Arrange
    String transactionId = "id";

    // Act
    DistributedTransaction transaction = transactionManager.begin(transactionId);

    // Assert
    assertThat(transaction.getId()).isEqualTo(transactionId);

    assertThat(transaction).isInstanceOf(DecoratedDistributedTransaction.class);
    DecoratedDistributedTransaction decoratedTransaction =
        (DecoratedDistributedTransaction) transaction;
    assertThat(decoratedTransaction.getOriginalTransaction())
        .isInstanceOf(AutoCommitTransaction.class);
    assertThat(((AutoCommitTransaction) decoratedTransaction.getOriginalTransaction()).getStorage())
        .isEqualTo(storage);
  }
}
