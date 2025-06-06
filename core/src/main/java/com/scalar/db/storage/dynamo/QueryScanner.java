package com.scalar.db.storage.dynamo;

import com.scalar.db.api.Result;
import com.scalar.db.common.AbstractScanner;
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
import javax.annotation.concurrent.NotThreadSafe;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

@NotThreadSafe
public class QueryScanner extends AbstractScanner {

  private final PaginatedRequest request;
  private final ResultInterpreter resultInterpreter;

  private Iterator<Map<String, AttributeValue>> itemsIterator;
  private final int fetchSize;
  @Nullable private Integer remainingLimit;
  @Nullable private Map<String, AttributeValue> lastEvaluatedKey;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public QueryScanner(
      PaginatedRequest request, int fetchSize, int limit, ResultInterpreter resultInterpreter) {
    this.request = request;
    this.fetchSize = fetchSize;

    if (limit > 0) {
      remainingLimit = limit;
      handleResponse(request.execute(Math.min(fetchSize, limit)));
    } else {
      remainingLimit = null;
      handleResponse(request.execute(fetchSize));
    }

    this.resultInterpreter = resultInterpreter;
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
    if (lastEvaluatedKey == null) {
      return false;
    }

    int nextFetchSize = remainingLimit != null ? Math.min(fetchSize, remainingLimit) : fetchSize;
    handleResponse(request.execute(lastEvaluatedKey, nextFetchSize));
    return itemsIterator.hasNext();
  }

  private void handleResponse(PaginatedRequestResponse response) {
    List<Map<String, AttributeValue>> items = response.items();
    if (remainingLimit != null) {
      remainingLimit -= items.size();
    }
    itemsIterator = items.iterator();

    boolean shouldContinue =
        (remainingLimit == null || remainingLimit > 0) && response.hasLastEvaluatedKey();
    lastEvaluatedKey = shouldContinue ? response.lastEvaluatedKey() : null;
  }

  @Override
  @Nonnull
  public List<Result> all() {
    List<Result> results = new ArrayList<>();
    Optional<Result> next;
    while ((next = one()).isPresent()) {
      results.add(next.get());
    }
    return results;
  }

  @Override
  public void close() {}
}
