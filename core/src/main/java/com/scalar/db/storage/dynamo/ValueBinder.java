package com.scalar.db.storage.dynamo;

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
import com.scalar.db.storage.ColumnEncodingUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * A visitor class to bind {@code Value}s to a condition expression
 *
 * @author Yuji Ito
 */
@NotThreadSafe
public final class ValueBinder implements ColumnVisitor {
  private final Map<String, AttributeValue> values;
  private final String alias;
  private int i;

  public ValueBinder(String alias) {
    this.values = new HashMap<>();
    this.alias = alias;
    this.i = 0;
  }

  @SuppressFBWarnings("EI_EXPOSE_REP")
  @Nonnull
  public Map<String, AttributeValue> build() {
    return values;
  }

  @Override
  public void visit(BooleanColumn column) {
    values.put(
        alias + i,
        column.hasNullValue()
            ? AttributeValue.builder().nul(true).build()
            : AttributeValue.builder().bool(column.getBooleanValue()).build());
    i++;
  }

  @Override
  public void visit(IntColumn column) {
    values.put(
        alias + i,
        column.hasNullValue()
            ? AttributeValue.builder().nul(true).build()
            : AttributeValue.builder().n(String.valueOf(column.getIntValue())).build());
    i++;
  }

  @Override
  public void visit(BigIntColumn column) {
    values.put(
        alias + i,
        column.hasNullValue()
            ? AttributeValue.builder().nul(true).build()
            : AttributeValue.builder().n(String.valueOf(column.getBigIntValue())).build());
    i++;
  }

  @Override
  public void visit(FloatColumn column) {
    values.put(
        alias + i,
        column.hasNullValue()
            ? AttributeValue.builder().nul(true).build()
            : AttributeValue.builder().n(String.valueOf(column.getFloatValue())).build());
    i++;
  }

  @Override
  public void visit(DoubleColumn column) {
    values.put(
        alias + i,
        column.hasNullValue()
            ? AttributeValue.builder().nul(true).build()
            : AttributeValue.builder().n(String.valueOf(column.getDoubleValue())).build());
    i++;
  }

  @Override
  public void visit(TextColumn column) {
    if (column.hasNullValue()) {
      values.put(alias + i, AttributeValue.builder().nul(true).build());
    } else {
      assert column.getTextValue() != null;
      values.put(alias + i, AttributeValue.builder().s(column.getTextValue()).build());
    }
    i++;
  }

  @Override
  public void visit(BlobColumn column) {
    if (column.hasNullValue()) {
      values.put(alias + i, AttributeValue.builder().nul(true).build());
    } else {
      assert column.getBlobValue() != null;
      values.put(
          alias + i,
          AttributeValue.builder().b(SdkBytes.fromByteBuffer(column.getBlobValue())).build());
    }
    i++;
  }

  @Override
  public void visit(DateColumn column) {
    if (column.hasNullValue()) {
      values.put(alias + i, AttributeValue.builder().nul(true).build());
    } else {
      values.put(
          alias + i,
          AttributeValue.builder().n(String.valueOf(ColumnEncodingUtils.encode(column))).build());
    }
    i++;
  }

  @Override
  public void visit(TimeColumn column) {
    if (column.hasNullValue()) {
      values.put(alias + i, AttributeValue.builder().nul(true).build());
    } else {
      values.put(
          alias + i,
          AttributeValue.builder().n(String.valueOf(ColumnEncodingUtils.encode(column))).build());
    }
    i++;
  }

  @Override
  public void visit(TimestampColumn column) {
    if (column.hasNullValue()) {
      values.put(alias + i, AttributeValue.builder().nul(true).build());
    } else {
      values.put(alias + i, AttributeValue.builder().s(ColumnEncodingUtils.encode(column)).build());
    }
    i++;
  }

  @Override
  public void visit(TimestampTZColumn column) {
    if (column.hasNullValue()) {
      values.put(alias + i, AttributeValue.builder().nul(true).build());
    } else {
      values.put(alias + i, AttributeValue.builder().s(ColumnEncodingUtils.encode(column)).build());
    }
    i++;
  }
}
