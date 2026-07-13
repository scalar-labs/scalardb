package com.scalar.db.common;

import com.scalar.db.api.TwoPhaseCommit;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.TransactionNotFoundException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A {@link TwoPhaseCommit.Coordinator} that forwards every method to a wrapped coordinator.
 *
 * <p>Base class for coordinator decorators: subclasses override only the methods they need and
 * inherit plain delegation for the rest. It is the coordinator-side counterpart of {@link
 * DecoratedTwoPhaseCommitParticipant}.
 */
public abstract class DecoratedTwoPhaseCommitCoordinator implements TwoPhaseCommit.Coordinator {

  private final TwoPhaseCommit.Coordinator coordinator;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  protected DecoratedTwoPhaseCommitCoordinator(TwoPhaseCommit.Coordinator coordinator) {
    this.coordinator = coordinator;
  }

  @Override
  public String begin(
      @Nullable String transactionId,
      boolean readOnly,
      Map<String, String> attributes,
      @Nullable TwoPhaseCommit.Participant participant)
      throws TransactionException {
    return coordinator.begin(transactionId, readOnly, attributes, participant);
  }

  @Override
  public void registerParticipant(String transactionId, TwoPhaseCommit.Participant participant)
      throws TransactionException {
    coordinator.registerParticipant(transactionId, participant);
  }

  @Override
  public void commit(String transactionId)
      throws CommitException, UnknownTransactionStatusException, TransactionNotFoundException {
    coordinator.commit(transactionId);
  }

  @Override
  public void rollback(String transactionId) throws RollbackException {
    coordinator.rollback(transactionId);
  }

  @Override
  public void releaseContext(String transactionId) {
    coordinator.releaseContext(transactionId);
  }

  @Override
  public void close() {
    coordinator.close();
  }
}
