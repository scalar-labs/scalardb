package com.scalar.db.storage.cosmos;

import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.BlobColumn;
import com.scalar.db.io.BooleanColumn;
import com.scalar.db.io.ColumnVisitor;
import com.scalar.db.io.DateColumn;
import com.scalar.db.io.DoubleColumn;
import com.scalar.db.io.FloatColumn;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.TextColumn;
import com.scalar.db.io.TimeColumn;
import com.scalar.db.io.TimestampColumn;
import com.scalar.db.io.TimestampTZColumn;
import com.scalar.db.util.TimeRelatedColumnEncodingUtils;
import java.util.Base64;
import java.util.function.Consumer;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A visitor class to bind {@code Value}s to a {@link StringBuilder}.
 *
 * @author Yuji Ito
 */
@NotThreadSafe
public final class ValueBinder implements ColumnVisitor {
  private Consumer<Object> consumer;

  public ValueBinder() {}

  public void set(Consumer<Object> consumer) {
    this.consumer = consumer;
  }

  @Override
  public void visit(BooleanColumn column) {
    consumer.accept(column.getValueAsObject());
  }

  @Override
  public void visit(IntColumn column) {
    consumer.accept(column.getValueAsObject());
  }

  @Override
  public void visit(BigIntColumn column) {
    consumer.accept(column.getValueAsObject());
  }

  @Override
  public void visit(FloatColumn column) {
    consumer.accept(column.getValueAsObject());
  }

  @Override
  public void visit(DoubleColumn column) {
    consumer.accept(column.getValueAsObject());
  }

  @Override
  public void visit(TextColumn column) {
    consumer.accept(column.getValueAsObject());
  }

  @Override
  public void visit(BlobColumn column) {
    if (column.hasNullValue()) {
      consumer.accept(null);
    } else {
      consumer.accept(Base64.getEncoder().encodeToString(column.getBlobValueAsBytes()));
    }
  }

  @Override
  public void visit(DateColumn column) {
    if (column.hasNullValue()) {
      consumer.accept(null);
    } else {
      consumer.accept(TimeRelatedColumnEncodingUtils.encode(column));
    }
  }

  @Override
  public void visit(TimeColumn column) {
    if (column.hasNullValue()) {
      consumer.accept(null);
    } else {
      consumer.accept(TimeRelatedColumnEncodingUtils.encode(column));
    }
  }

  @Override
  public void visit(TimestampColumn column) {
    if (column.hasNullValue()) {
      consumer.accept(null);
    } else {
      consumer.accept(TimeRelatedColumnEncodingUtils.encode(column));
    }
  }

  @Override
  public void visit(TimestampTZColumn column) {
    if (column.hasNullValue()) {
      consumer.accept(null);
    } else {
      consumer.accept(TimeRelatedColumnEncodingUtils.encode(column));
    }
  }
}
