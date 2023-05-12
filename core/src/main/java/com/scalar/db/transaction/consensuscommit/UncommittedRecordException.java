package com.scalar.db.transaction.consensuscommit;

import com.google.common.collect.ImmutableList;
import com.scalar.db.exception.transaction.CrudConflictException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class UncommittedRecordException extends CrudConflictException {
  private final List<TransactionResult> results;

  public UncommittedRecordException(
      TransactionResult result, String message, String transactionId) {
    this(result, message, null, transactionId);
  }

  public UncommittedRecordException(
      TransactionResult result, String message, Throwable cause, String transactionId) {
    super(message, cause, transactionId);
    results = Collections.singletonList(result);
  }

  public UncommittedRecordException(
      List<TransactionResult> results, String message, String transactionId) {
    this(results, message, null, transactionId);
  }

  public UncommittedRecordException(
      List<TransactionResult> results, String message, Throwable cause, String transactionId) {
    super(message, cause, transactionId);
    this.results = new ArrayList<>();
    this.results.addAll(results);
  }

  public List<TransactionResult> getResults() {
    return ImmutableList.copyOf(results);
  }
}
