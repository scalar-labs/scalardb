package com.scalar.db.storage.dynamo;

import static com.google.common.base.Preconditions.checkNotNull;

import com.scalar.db.api.Result;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.Selection;
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
  private final Selection selection;
  private final TableMetadata metadata;

  public ScannerImpl(
      List<Map<String, AttributeValue>> items, Selection selection, TableMetadata metadata) {
    this.items = checkNotNull(items);
    this.selection = selection;
    this.metadata = checkNotNull(metadata);
  }

  @Override
  @Nonnull
  public Optional<Result> one() {
    if (items.isEmpty()) {
      return Optional.empty();
    }
    Map<String, AttributeValue> item = items.remove(0);

    return Optional.of(new ResultImpl(item, selection, metadata));
  }

  @Override
  @Nonnull
  public List<Result> all() {
    List<Result> results = new ArrayList<>();
    items.forEach(i -> results.add(new ResultImpl(i, selection, metadata)));
    return results;
  }

  @Override
  public Iterator<Result> iterator() {
    return new ScannerIterator(items.iterator(), selection, metadata);
  }

  @Override
  public void close() {}
}
