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
  @Nullable private Integer remainingLimit;
  @Nullable private Map<String, AttributeValue> lastEvaluatedKey;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public QueryScanner(PaginatedRequest request, int limit, ResultInterpreter resultInterpreter) {
    this.request = request;

    if (limit > 0) {
      remainingLimit = limit;
      handleResponse(request.execute(limit));
    } else {
      remainingLimit = null;
      handleResponse(request.execute());
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
    if (lastEvaluatedKey != null) {
      if (remainingLimit != null) {
        handleResponse(request.execute(lastEvaluatedKey, remainingLimit));
      } else {
        handleResponse(request.execute(lastEvaluatedKey));
      }
      return itemsIterator.hasNext();
    }
    return false;
  }

  private void handleResponse(PaginatedRequestResponse response) {
    List<Map<String, AttributeValue>> items = response.items();
    if (remainingLimit != null) {
      remainingLimit -= items.size();
    }
    itemsIterator = items.iterator();
    if ((remainingLimit == null || remainingLimit > 0) && response.hasLastEvaluatedKey()) {
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
  public void close() {}
}
