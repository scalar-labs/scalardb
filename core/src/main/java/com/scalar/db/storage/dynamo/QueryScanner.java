package com.scalar.db.storage.dynamo;

import com.scalar.db.api.Result;
import com.scalar.db.api.Scanner;
import com.scalar.db.storage.common.ScannerIterator;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

public class QueryScanner implements Scanner {

  private final DynamoDbClient client;
  private final QueryRequest request;
  private final ResultInterpreter resultInterpreter;

  private Iterator<Map<String, AttributeValue>> itemsIterator;
  @Nullable private Map<String, AttributeValue> lastEvaluatedKey;
  private int totalResultCount;

  private ScannerIterator scannerIterator;

  public QueryScanner(
      DynamoDbClient client, QueryRequest request, ResultInterpreter resultInterpreter) {
    this.client = client;
    this.request = request;
    this.resultInterpreter = resultInterpreter;

    query(request);
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
      QueryRequest.Builder builder = request.toBuilder();
      builder.exclusiveStartKey(lastEvaluatedKey);
      query(builder.build());
      return itemsIterator.hasNext();
    }
    return false;
  }

  private void query(QueryRequest request) {
    QueryResponse response = client.query(request);
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
