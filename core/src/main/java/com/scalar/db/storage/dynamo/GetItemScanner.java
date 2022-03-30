package com.scalar.db.storage.dynamo;

import com.scalar.db.api.Result;
import com.scalar.db.api.Scanner;
import com.scalar.db.storage.common.ScannerIterator;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;

@NotThreadSafe
public final class GetItemScanner implements Scanner {
  private final Map<String, AttributeValue> item;
  private final ResultInterpreter resultInterpreter;

  private boolean hasNext;
  private ScannerIterator scannerIterator;

  public GetItemScanner(
      DynamoDbClient client, GetItemRequest request, ResultInterpreter resultInterpreter) {
    GetItemResponse response = client.getItem(request);
    if (response.hasItem()) {
      item = response.item();
      hasNext = true;
    } else {
      item = null;
      hasNext = false;
    }
    this.resultInterpreter = resultInterpreter;
  }

  @Override
  @Nonnull
  public Optional<Result> one() {
    if (!hasNext) {
      return Optional.empty();
    }

    Result result = resultInterpreter.interpret(item);
    hasNext = false;
    return Optional.of(result);
  }

  @Override
  @Nonnull
  public List<Result> all() {
    if (!hasNext) {
      return Collections.emptyList();
    }
    Result result = resultInterpreter.interpret(item);
    hasNext = false;
    return Collections.singletonList(result);
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
