package com.scalar.db.transaction.consensuscommit;

import com.google.common.collect.ImmutableList;
import com.scalar.db.api.Selection;
import com.scalar.db.exception.transaction.CrudConflictException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;

public class UncommittedRecordException extends CrudConflictException {

  private final Selection selection;
  private final List<TransactionResult> results;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public UncommittedRecordException(
      Selection selection, TransactionResult result, String message, String transactionId) {
    super(message, transactionId);
    this.selection = selection;
    results = ImmutableList.of(result);
  }

  @SuppressFBWarnings("EI_EXPOSE_REP")
  public Selection getSelection() {
    return selection;
  }

  public List<TransactionResult> getResults() {
    return results;
  }
}
