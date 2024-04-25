package com.scalar.db.storage.dynamo;

import com.scalar.db.api.Result;
import com.scalar.db.api.Scanner;
import com.scalar.db.common.ScannerIterator;
import com.scalar.db.storage.dynamo.request.PaginatedRequest;
import com.scalar.db.storage.dynamo.request.PaginatedRequestResponse;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class QueryScanner implements Scanner {

  private final PaginatedRequest request;
  private final ResultInterpreter resultInterpreter;

  private Iterator<Map<String, AttributeValue>> itemsIterator;
  @Nullable private Map<String, AttributeValue> lastEvaluatedKey;
  private int totalResultCount;

  private ScannerIterator scannerIterator;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public QueryScanner(PaginatedRequest request, ResultInterpreter resultInterpreter) {
    this.request = request;
    this.resultInterpreter = resultInterpreter;

    handleResponse(request.execute());
  }

  @Override
  @Nonnull
  public Optional<Result> one() {
    if (!hasNext()) {
      return Optional.empty();
    }

    return Optional.of(resultInterpreter.interpret(itemsIterator.next()));
  }

  private boolean hasNext() {
    if (itemsIterator.hasNext()) {
      return true;
    }
    if (lastEvaluatedKey != null) {
      handleResponse(request.execute(lastEvaluatedKey));
      return itemsIterator.hasNext();
    }
    return false;
  }

  private void handleResponse(PaginatedRequestResponse response) {
    List<Map<String, AttributeValue>> items = response.items();
    totalResultCount += items.size();
    itemsIterator = items.iterator();
    if ((request.limit() == null || totalResultCount < request.limit())
        && response.hasLastEvaluatedKey()) {
      lastEvaluatedKey = response.lastEvaluatedKey();
    } else {
      lastEvaluatedKey = null;
    }
  }

  @Override
  @Nonnull
  public List<Result> all() {
    List<Result> ret = new ArrayList<>();
    while (true) {
      Optional<Result> one = one();
      if (!one.isPresent()) {
        break;
      }
      ret.add(one.get());
    }
    return ret;
  }

  @Override
  @Nonnull
  public Iterator<Result> iterator() {
    if (scannerIterator == null) {
      scannerIterator = new ScannerIterator(this);
    }
    return scannerIterator;
  }

  @Override
  public void close() {}
}
