package com.scalar.db.storage.dynamo;

import static com.google.common.base.Preconditions.checkNotNull;

import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.Selection;
import com.scalar.db.exception.storage.UnsupportedTypeException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

@NotThreadSafe
public final class ScannerImpl implements Scanner {
  private final Selection selection;
  private final TableMetadata metadata;
  private List<Map<String, AttributeValue>> items;

  public ScannerImpl(
      List<Map<String, AttributeValue>> items, Selection selection, TableMetadata metadata) {
    this.items = checkNotNull(items);
    this.selection = selection;
    this.metadata = checkNotNull(metadata);
    sort();
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

  private void sort() {
    Scan scan = (Scan) selection;
    DynamoOperation dynamoOperation = new DynamoOperation(scan, metadata);
    if (dynamoOperation.isSingleClusteringKey()) {
      // the ordering and the limitation already are applied in DynamoDB
      return;
    }

    if (!scan.getOrderings().isEmpty()) {
      Collections.sort(
          items,
          new Comparator<Map<String, AttributeValue>>() {
            public int compare(Map<String, AttributeValue> o1, Map<String, AttributeValue> o2) {
              int compareResult = 0;
              for (Scan.Ordering ordering : scan.getOrderings()) {
                String name = ordering.getName();
                compareResult =
                    compareAttributeValues(
                        metadata.getColumns().get(name), o1.get(name), o2.get(name));
                compareResult *= ordering.getOrder() == Scan.Ordering.Order.ASC ? 1 : -1;
                if (compareResult != 0) {
                  break;
                }
              }

              return compareResult;
            }
          });
    }

    int limit = scan.getLimit();
    if (limit > 0 && limit < items.size()) {
      items = items.subList(0, limit);
    }
  }

  private int compareAttributeValues(String type, AttributeValue a1, AttributeValue a2)
      throws UnsupportedTypeException {
    if (a1 == null && a2 == null) {
      return 0;
    } else if (a1 == null) {
      return -1;
    } else if (a2 == null) {
      return 1;
    }

    switch (type) {
      case "boolean":
        return a1.bool().compareTo(a2.bool());
      case "int":
        return Integer.valueOf(a1.n()).compareTo(Integer.valueOf(a2.n()));
      case "bigint":
        return Long.valueOf(a1.n()).compareTo(Long.valueOf(a2.n()));
      case "float":
        return Float.valueOf(a1.n()).compareTo(Float.valueOf(a2.n()));
      case "double":
        return Double.valueOf(a1.n()).compareTo(Double.valueOf(a2.n()));
      case "text":
        return a1.s().compareTo(a2.s());
      case "blob":
        return a1.b().asByteBuffer().compareTo(a2.b().asByteBuffer());
      default:
        throw new UnsupportedTypeException(type);
    }
  }
}
