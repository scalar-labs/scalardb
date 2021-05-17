package com.scalar.db.storage.dynamo;

import com.scalar.db.api.Result;
import com.scalar.db.api.Selection;
import com.scalar.db.api.TableMetadata;
import java.util.Iterator;
import java.util.Map;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

@NotThreadSafe
public final class ScannerIterator implements Iterator<Result> {
  private final Iterator<Map<String, AttributeValue>> iterator;
  private final Selection selection;
  private final TableMetadata metadata;

  public ScannerIterator(
      Iterator<Map<String, AttributeValue>> iterator, Selection selection, TableMetadata metadata) {
    this.iterator = iterator;
    this.selection = selection;
    this.metadata = metadata;
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  @Nullable
  public Result next() {
    Map<String, AttributeValue> item = iterator.next();
    if (item == null) {
      return null;
    }

    return new ResultImpl(item, selection, metadata);
  }
}
