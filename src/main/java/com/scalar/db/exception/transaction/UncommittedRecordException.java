package com.scalar.db.exception.transaction;

import com.google.common.collect.ImmutableList;
import com.scalar.db.transaction.consensuscommit.TransactionResult;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** */
public class UncommittedRecordException extends CrudException {
  private final List<TransactionResult> results;

  public UncommittedRecordException(TransactionResult result, String message) {
    this(result, message, null);
  }

  public UncommittedRecordException(TransactionResult result, String message, Throwable cause) {
    super(message, cause);
    results = Arrays.asList(result);
  }

  public UncommittedRecordException(List<TransactionResult> results, String message) {
    this(results, message, null);
  }

  public UncommittedRecordException(
      List<TransactionResult> results, String message, Throwable cause) {
    super(message, cause);
    this.results = new ArrayList<>();
    results.forEach(r -> this.results.add(r));
  }

  public List<TransactionResult> getResults() {
    return ImmutableList.copyOf(results);
  }
}
