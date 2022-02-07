package com.scalar.db.transaction.consensuscommit;

import com.google.common.collect.ImmutableList;
import com.scalar.db.exception.transaction.CrudConflictException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class UncommittedRecordException extends CrudConflictException {
  private final List<TransactionResult> results;

  public UncommittedRecordException(TransactionResult result, String message) {
    this(result, message, null);
  }

  public UncommittedRecordException(TransactionResult result, String message, Throwable cause) {
    super(message, cause);
    results = Collections.singletonList(result);
  }

  public UncommittedRecordException(List<TransactionResult> results, String message) {
    this(results, message, null);
  }

  public UncommittedRecordException(
      List<TransactionResult> results, String message, Throwable cause) {
    super(message, cause);
    this.results = new ArrayList<>();
    this.results.addAll(results);
  }

  public List<TransactionResult> getResults() {
    return ImmutableList.copyOf(results);
  }
}
