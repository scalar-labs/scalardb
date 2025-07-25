package com.scalar.db.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Insert;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TwoPhaseCommitTransaction;
import com.scalar.db.api.TwoPhaseCommitTransactionManager;
import com.scalar.db.api.Update;
import com.scalar.db.api.Upsert;
import com.scalar.db.exception.transaction.AbortException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.PreparationException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.exception.transaction.ValidationException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class StateManagedTwoPhaseCommitTransactionManagerTest {

  @Mock private TwoPhaseCommitTransactionManager wrappedTransactionManager;

  private StateManagedTwoPhaseCommitTransactionManager transactionManager;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    transactionManager =
        new StateManagedTwoPhaseCommitTransactionManager(wrappedTransactionManager);
  }

  @Test
  public void begin_ShouldReturnStateManagedTransaction() throws TransactionException {
    // Arrange
    when(wrappedTransactionManager.begin()).thenReturn(mock(TwoPhaseCommitTransaction.class));
    when(wrappedTransactionManager.begin(anyString()))
        .thenReturn(mock(TwoPhaseCommitTransaction.class));

    // Act
    TwoPhaseCommitTransaction transaction1 = transactionManager.begin();
    TwoPhaseCommitTransaction transaction2 = transactionManager.begin("txId");

    // Assert
    assertThat(transaction1)
        .isInstanceOf(StateManagedTwoPhaseCommitTransactionManager.StateManagedTransaction.class);
    assertThat(transaction2)
        .isInstanceOf(StateManagedTwoPhaseCommitTransactionManager.StateManagedTransaction.class);
  }

  @Test
  public void start_ShouldReturnStateManagedTransaction() throws TransactionException {
    // Arrange
    when(wrappedTransactionManager.start()).thenReturn(mock(TwoPhaseCommitTransaction.class));
    when(wrappedTransactionManager.start(anyString()))
        .thenReturn(mock(TwoPhaseCommitTransaction.class));

    // Act
    TwoPhaseCommitTransaction transaction1 = transactionManager.start();
    TwoPhaseCommitTransaction transaction2 = transactionManager.start("txId");

    // Assert
    assertThat(transaction1)
        .isInstanceOf(StateManagedTwoPhaseCommitTransactionManager.StateManagedTransaction.class);
    assertThat(transaction2)
        .isInstanceOf(StateManagedTwoPhaseCommitTransactionManager.StateManagedTransaction.class);
  }

  @SuppressFBWarnings({"SIC_INNER_SHOULD_BE_STATIC", "EI_EXPOSE_REP2"})
  @SuppressWarnings("ClassCanBeStatic")
  @Nested
  public class StateManagedTransactionTest {

    @Mock private TwoPhaseCommitTransaction wrappedTransaction;

    private StateManagedTwoPhaseCommitTransactionManager.StateManagedTransaction transaction;

    @BeforeEach
    public void setUp() throws Exception {
      MockitoAnnotations.openMocks(this).close();

      // Arrange
      transaction =
          new StateManagedTwoPhaseCommitTransactionManager.StateManagedTransaction(
              wrappedTransaction);
    }

    @Test
    public void crud_ShouldNotThrowAnyException() {
      // Arrange
      Get get = mock(Get.class);
      Scan scan = mock(Scan.class);
      Put put = mock(Put.class);
      @SuppressWarnings("unchecked")
      List<Put> puts = (List<Put>) mock(List.class);
      Delete delete = mock(Delete.class);
      @SuppressWarnings("unchecked")
      List<Delete> deletes = (List<Delete>) mock(List.class);
      Insert insert = mock(Insert.class);
      Upsert upsert = mock(Upsert.class);
      Update update = mock(Update.class);
      @SuppressWarnings("unchecked")
      List<Mutation> mutations = (List<Mutation>) mock(List.class);

      // Act Assert
      assertThatCode(() -> transaction.get(get)).doesNotThrowAnyException();
      assertThatCode(() -> transaction.scan(scan)).doesNotThrowAnyException();
      assertThatCode(() -> transaction.put(put)).doesNotThrowAnyException();
      assertThatCode(() -> transaction.put(puts)).doesNotThrowAnyException();
      assertThatCode(() -> transaction.delete(delete)).doesNotThrowAnyException();
      assertThatCode(() -> transaction.delete(deletes)).doesNotThrowAnyException();
      assertThatCode(() -> transaction.insert(insert)).doesNotThrowAnyException();
      assertThatCode(() -> transaction.upsert(upsert)).doesNotThrowAnyException();
      assertThatCode(() -> transaction.update(update)).doesNotThrowAnyException();
      assertThatCode(() -> transaction.mutate(mutations)).doesNotThrowAnyException();
    }

    @Test
    public void crud_AfterPrepare_ShouldThrowIllegalStateException() throws PreparationException {
      // Arrange
      Get get = mock(Get.class);
      Scan scan = mock(Scan.class);
      Put put = mock(Put.class);
      @SuppressWarnings("unchecked")
      List<Put> puts = (List<Put>) mock(List.class);
      Delete delete = mock(Delete.class);
      @SuppressWarnings("unchecked")
      List<Delete> deletes = (List<Delete>) mock(List.class);
      Insert insert = mock(Insert.class);
      Upsert upsert = mock(Upsert.class);
      Update update = mock(Update.class);
      @SuppressWarnings("unchecked")
      List<Mutation> mutations = (List<Mutation>) mock(List.class);

      transaction.prepare();

      // Act Assert
      assertThatThrownBy(() -> transaction.get(get)).isInstanceOf(IllegalStateException.class);
      assertThatThrownBy(() -> transaction.scan(scan)).isInstanceOf(IllegalStateException.class);
      assertThatThrownBy(() -> transaction.put(put)).isInstanceOf(IllegalStateException.class);
      assertThatThrownBy(() -> transaction.put(puts)).isInstanceOf(IllegalStateException.class);
      assertThatThrownBy(() -> transaction.delete(delete))
          .isInstanceOf(IllegalStateException.class);
      assertThatThrownBy(() -> transaction.delete(deletes))
          .isInstanceOf(IllegalStateException.class);
      assertThatThrownBy(() -> transaction.insert(insert))
          .isInstanceOf(IllegalStateException.class);
      assertThatThrownBy(() -> transaction.upsert(upsert))
          .isInstanceOf(IllegalStateException.class);
      assertThatThrownBy(() -> transaction.update(update))
          .isInstanceOf(IllegalStateException.class);
      assertThatThrownBy(() -> transaction.mutate(mutations))
          .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void crud_AfterRollback_ShouldThrowIllegalStateException() throws RollbackException {
      // Arrange
      Get get = mock(Get.class);
      Scan scan = mock(Scan.class);
      Put put = mock(Put.class);
      @SuppressWarnings("unchecked")
      List<Put> puts = (List<Put>) mock(List.class);
      Delete delete = mock(Delete.class);
      @SuppressWarnings("unchecked")
      List<Delete> deletes = (List<Delete>) mock(List.class);
      Insert insert = mock(Insert.class);
      Upsert upsert = mock(Upsert.class);
      Update update = mock(Update.class);
      @SuppressWarnings("unchecked")
      List<Mutation> mutations = (List<Mutation>) mock(List.class);

      transaction.rollback();

      // Act Assert
      assertThatThrownBy(() -> transaction.get(get)).isInstanceOf(IllegalStateException.class);
      assertThatThrownBy(() -> transaction.scan(scan)).isInstanceOf(IllegalStateException.class);
      assertThatThrownBy(() -> transaction.put(put)).isInstanceOf(IllegalStateException.class);
      assertThatThrownBy(() -> transaction.put(puts)).isInstanceOf(IllegalStateException.class);
      assertThatThrownBy(() -> transaction.delete(delete))
          .isInstanceOf(IllegalStateException.class);
      assertThatThrownBy(() -> transaction.delete(deletes))
          .isInstanceOf(IllegalStateException.class);
      assertThatThrownBy(() -> transaction.insert(insert))
          .isInstanceOf(IllegalStateException.class);
      assertThatThrownBy(() -> transaction.upsert(upsert))
          .isInstanceOf(IllegalStateException.class);
      assertThatThrownBy(() -> transaction.update(update))
          .isInstanceOf(IllegalStateException.class);
      assertThatThrownBy(() -> transaction.mutate(mutations))
          .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void prepare_ShouldNotThrowAnyException() {
      // Arrange

      // Act Assert
      assertThatCode(() -> transaction.prepare()).doesNotThrowAnyException();
    }

    @Test
    public void prepare_Twice_ShouldThrowIllegalStateException() throws PreparationException {
      // Arrange
      transaction.prepare();

      // Act Assert
      assertThatThrownBy(() -> transaction.prepare()).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void validate_AfterPrepare_ShouldNotThrowAnyException() throws PreparationException {
      // Arrange
      transaction.prepare();

      // Act Assert
      assertThatCode(() -> transaction.validate()).doesNotThrowAnyException();
    }

    @Test
    public void validate_Twice_ShouldThrowIllegalStateException()
        throws PreparationException, ValidationException {
      // Arrange
      transaction.prepare();
      transaction.validate();

      // Act Assert
      assertThatThrownBy(() -> transaction.validate()).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void validate_BeforePrepare_ShouldThrowIllegalStateException() {
      // Arrange

      // Act Assert
      assertThatThrownBy(() -> transaction.validate()).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void validate_AfterPrepareFailed_ShouldThrowIllegalStateException()
        throws PreparationException {
      // Arrange
      doThrow(PreparationException.class).when(wrappedTransaction).prepare();
      assertThatThrownBy(() -> transaction.prepare()).isInstanceOf(PreparationException.class);

      // Act Assert
      assertThatThrownBy(() -> transaction.validate()).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void commit_AfterPrepare_ShouldNotThrowAnyException() throws PreparationException {
      // Arrange
      transaction.prepare();

      // Act Assert
      assertThatCode(() -> transaction.commit()).doesNotThrowAnyException();
    }

    @Test
    public void commit_AfterValidate_ShouldNotThrowAnyException()
        throws PreparationException, ValidationException {
      // Arrange
      transaction.prepare();
      transaction.validate();

      // Act Assert
      assertThatCode(() -> transaction.commit()).doesNotThrowAnyException();
    }

    @Test
    public void commit_Twice_ShouldThrowIllegalStateException()
        throws PreparationException, CommitException, UnknownTransactionStatusException {
      // Arrange
      transaction.prepare();
      transaction.commit();

      // Act Assert
      assertThatThrownBy(() -> transaction.commit()).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void commit_AfterPrepareFailed_ShouldThrowIllegalStateException()
        throws PreparationException {
      // Arrange
      doThrow(PreparationException.class).when(wrappedTransaction).prepare();
      assertThatThrownBy(() -> transaction.prepare()).isInstanceOf(PreparationException.class);

      // Act Assert
      assertThatThrownBy(() -> transaction.commit()).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void commit_AfterValidateFailed_ShouldThrowIllegalStateException()
        throws PreparationException, ValidationException {
      // Arrange
      doThrow(ValidationException.class).when(wrappedTransaction).validate();

      transaction.prepare();
      assertThatThrownBy(() -> transaction.validate()).isInstanceOf(ValidationException.class);

      // Act Assert
      assertThatThrownBy(() -> transaction.commit()).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void commit_AfterRollback_ShouldThrowIllegalStateException() throws RollbackException {
      // Arrange
      transaction.rollback();

      // Act Assert
      assertThatThrownBy(() -> transaction.commit()).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void rollback_ShouldNotThrowAnyException() {
      // Arrange

      // Act Assert
      assertThatCode(() -> transaction.rollback()).doesNotThrowAnyException();
    }

    @Test
    public void rollback_AfterCommitFailed_ShouldNotThrowAnyException()
        throws PreparationException, CommitException, UnknownTransactionStatusException {
      // Arrange
      doThrow(CommitException.class).when(wrappedTransaction).commit();

      transaction.prepare();
      assertThatThrownBy(() -> transaction.commit()).isInstanceOf(CommitException.class);

      // Act Assert
      assertThatCode(() -> transaction.rollback()).doesNotThrowAnyException();
    }

    @Test
    public void rollback_AfterPrepareFailed_ShouldNotThrowAnyException()
        throws PreparationException {
      // Arrange
      doThrow(PreparationException.class).when(wrappedTransaction).prepare();
      assertThatThrownBy(() -> transaction.prepare()).isInstanceOf(PreparationException.class);

      // Act Assert
      assertThatCode(() -> transaction.rollback()).doesNotThrowAnyException();
    }

    @Test
    public void rollback_AfterValidateFailed_ShouldNotThrowAnyException()
        throws PreparationException, ValidationException {
      // Arrange
      doThrow(ValidationException.class).when(wrappedTransaction).validate();

      transaction.prepare();
      assertThatThrownBy(() -> transaction.validate()).isInstanceOf(ValidationException.class);

      // Act Assert
      assertThatCode(() -> transaction.rollback()).doesNotThrowAnyException();
    }

    @Test
    public void rollback_AfterCommit_ShouldThrowIllegalStateException()
        throws PreparationException, CommitException, UnknownTransactionStatusException {
      // Arrange
      transaction.prepare();
      transaction.commit();

      // Act Assert
      assertThatThrownBy(() -> transaction.rollback()).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void rollback_Twice_ShouldNotThrowAnyExceptionAndCallWrappedTransactionRollbackOnlyOnce()
        throws RollbackException {
      // Arrange
      transaction.rollback();

      // Act Assert
      assertThatCode(() -> transaction.rollback()).doesNotThrowAnyException();

      verify(wrappedTransaction, only()).rollback();
    }

    @Test
    public void abort_ShouldNotThrowAnyException() {
      // Arrange

      // Act Assert
      assertThatCode(() -> transaction.abort()).doesNotThrowAnyException();
    }

    @Test
    public void abort_AfterCommitFailed_ShouldNotThrowAnyException()
        throws PreparationException, CommitException, UnknownTransactionStatusException {
      // Arrange
      doThrow(CommitException.class).when(wrappedTransaction).commit();

      transaction.prepare();
      assertThatThrownBy(() -> transaction.commit()).isInstanceOf(CommitException.class);

      // Act Assert
      assertThatCode(() -> transaction.abort()).doesNotThrowAnyException();
    }

    @Test
    public void abort_AfterPrepareFailed_ShouldNotThrowAnyException() throws PreparationException {
      // Arrange
      doThrow(PreparationException.class).when(wrappedTransaction).prepare();
      assertThatThrownBy(() -> transaction.prepare()).isInstanceOf(PreparationException.class);

      // Act Assert
      assertThatCode(() -> transaction.abort()).doesNotThrowAnyException();
    }

    @Test
    public void abort_AfterValidateFailed_ShouldNotThrowAnyException()
        throws PreparationException, ValidationException {
      // Arrange
      doThrow(ValidationException.class).when(wrappedTransaction).validate();

      transaction.prepare();
      assertThatThrownBy(() -> transaction.validate()).isInstanceOf(ValidationException.class);

      // Act Assert
      assertThatCode(() -> transaction.abort()).doesNotThrowAnyException();
    }

    @Test
    public void abort_AfterCommit_ShouldThrowIllegalStateException()
        throws PreparationException, CommitException, UnknownTransactionStatusException {
      // Arrange
      transaction.prepare();
      transaction.commit();

      // Act Assert
      assertThatThrownBy(() -> transaction.abort()).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void abort_Twice_ShouldNotThrowAnyExceptionAndCallWrappedTransactionAbortOnlyOnce()
        throws AbortException {
      // Arrange
      transaction.abort();

      // Act Assert
      assertThatCode(() -> transaction.abort()).doesNotThrowAnyException();

      verify(wrappedTransaction, only()).abort();
    }
  }
}
