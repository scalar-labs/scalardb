package com.scalar.db.common;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.Get;
import com.scalar.db.api.Insert;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Update;
import com.scalar.db.api.Upsert;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class TransactionDecorationDistributedTransactionManagerTest {

  @SuppressFBWarnings("SIC_INNER_SHOULD_BE_STATIC")
  @SuppressWarnings("ClassCanBeStatic")
  @Nested
  public class StateManagedTransactionTest {

    @Mock private DistributedTransaction wrappedTransaction;

    private TransactionDecorationDistributedTransactionManager.StateManagedTransaction transaction;

    @BeforeEach
    public void setUp() throws Exception {
      MockitoAnnotations.openMocks(this).close();

      // Arrange
      transaction =
          new TransactionDecorationDistributedTransactionManager.StateManagedTransaction(
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
    public void crud_AfterCommit_ShouldThrowIllegalStateException()
        throws CommitException, UnknownTransactionStatusException {
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

      transaction.commit();

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
    public void commit_ShouldNotThrowAnyException() {
      // Arrange

      // Act Assert
      assertThatCode(() -> transaction.commit()).doesNotThrowAnyException();
    }

    @Test
    public void commit_Twice_ShouldThrowIllegalStateException()
        throws CommitException, UnknownTransactionStatusException {
      // Arrange
      transaction.commit();

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
        throws CommitException, UnknownTransactionStatusException {
      // Arrange
      doThrow(CommitException.class).when(wrappedTransaction).commit();
      assertThatThrownBy(() -> transaction.commit()).isInstanceOf(CommitException.class);

      // Act Assert
      assertThatCode(() -> transaction.rollback()).doesNotThrowAnyException();
    }

    @Test
    public void rollback_AfterCommit_ShouldThrowIllegalStateException()
        throws CommitException, UnknownTransactionStatusException {
      // Arrange
      transaction.commit();

      // Act Assert
      assertThatThrownBy(() -> transaction.rollback()).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void rollback_Twice_ShouldThrowIllegalStateException() throws RollbackException {
      // Arrange
      transaction.rollback();

      // Act Assert
      assertThatThrownBy(() -> transaction.rollback()).isInstanceOf(IllegalStateException.class);
    }
  }
}
