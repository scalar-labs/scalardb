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
import javax.annotation.concurrent.NotThreadSafe;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

@NotThreadSafe
public final class ScannerImpl implements Scanner {
  private final List<Map<String, AttributeValue>> items;
  private final ResultInterpreter resultInterpreter;

  private ScannerIterator scannerIterator;

  public ScannerImpl(List<Map<String, AttributeValue>> items, ResultInterpreter resultInterpreter) {
    this.items = items;
    this.resultInterpreter = resultInterpreter;
  }

  @Override
  @Nonnull
  public Optional<Result> one() {
    if (items.isEmpty()) {
      return Optional.empty();
    }
    Map<String, AttributeValue> item = items.remove(0);

    return Optional.of(resultInterpreter.interpret(item));
  }

  @Override
  @Nonnull
  public List<Result> all() {
    List<Result> results = new ArrayList<>();
    items.forEach(i -> results.add(resultInterpreter.interpret(i)));
    items.clear();
    return results;
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
