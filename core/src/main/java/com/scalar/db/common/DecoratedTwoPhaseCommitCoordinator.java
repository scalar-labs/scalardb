package com.scalar.db.common;

import com.scalar.db.api.TwoPhaseCommitCoordinator;
import com.scalar.db.api.TwoPhaseCommitParticipant;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.TransactionNotFoundException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A {@link TwoPhaseCommitCoordinator} that forwards every method to a wrapped coordinator.
 *
 * <p>Base class for coordinator decorators: subclasses override only the methods they need and
 * inherit plain delegation for the rest. It is the coordinator-side counterpart of {@link
 * DecoratedTwoPhaseCommitParticipant}.
 */
public abstract class DecoratedTwoPhaseCommitCoordinator implements TwoPhaseCommitCoordinator {

  private final TwoPhaseCommitCoordinator coordinator;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  protected DecoratedTwoPhaseCommitCoordinator(TwoPhaseCommitCoordinator coordinator) {
    this.coordinator = coordinator;
  }

  @Override
  public String begin(
      @Nullable String transactionId,
      boolean readOnly,
      Map<String, String> attributes,
      @Nullable TwoPhaseCommitParticipant participant)
      throws TransactionException {
    return coordinator.begin(transactionId, readOnly, attributes, participant);
  }

  @Override
  public void registerParticipant(String transactionId, TwoPhaseCommitParticipant participant)
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
  public void releaseTransactionContext(String transactionId) throws TransactionException {
    coordinator.releaseTransactionContext(transactionId);
  }

  @Override
  public void close() {
    coordinator.close();
  }
}
