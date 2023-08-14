package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model;

import com.scalar.db.api.TransactionState;
import java.time.Instant;

public class CoordinatorState {
  public final String txId;
  public final TransactionState txState;
  public final Instant txCommittedAt;

  public CoordinatorState(String txId, TransactionState txState, Instant txCommittedAt) {
    this.txId = txId;
    this.txState = txState;
    this.txCommittedAt = txCommittedAt;
  }
}
