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
import com.scalar.db.util.TimeRelatedColumnEncodingUtils;
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
  private final boolean bindNullValues;
  private int i;

  public ValueBinder(String alias, boolean bindNullValues) {
    this.values = new HashMap<>();
    this.alias = alias;
    this.bindNullValues = bindNullValues;
    this.i = 0;
  }

  @SuppressFBWarnings("EI_EXPOSE_REP")
  @Nonnull
  public Map<String, AttributeValue> build() {
    return values;
  }

  @Override
  public void visit(BooleanColumn column) {
    if (!column.hasNullValue()) {
      values.put(alias + i, AttributeValue.builder().bool(column.getBooleanValue()).build());
    } else if (bindNullValues) {
      values.put(alias + i, AttributeValue.builder().nul(true).build());
    }
    i++;
  }

  @Override
  public void visit(IntColumn column) {
    if (!column.hasNullValue()) {
      values.put(
          alias + i, AttributeValue.builder().n(String.valueOf(column.getIntValue())).build());
    } else if (bindNullValues) {
      values.put(alias + i, AttributeValue.builder().nul(true).build());
    }
    i++;
  }

  @Override
  public void visit(BigIntColumn column) {
    if (!column.hasNullValue()) {
      values.put(
          alias + i, AttributeValue.builder().n(String.valueOf(column.getBigIntValue())).build());
    } else if (bindNullValues) {
      values.put(alias + i, AttributeValue.builder().nul(true).build());
    }
    i++;
  }

  @Override
  public void visit(FloatColumn column) {
    if (!column.hasNullValue()) {
      values.put(
          alias + i, AttributeValue.builder().n(String.valueOf(column.getFloatValue())).build());
    } else if (bindNullValues) {
      values.put(alias + i, AttributeValue.builder().nul(true).build());
    }
    i++;
  }

  @Override
  public void visit(DoubleColumn column) {
    if (!column.hasNullValue()) {
      values.put(
          alias + i, AttributeValue.builder().n(String.valueOf(column.getDoubleValue())).build());
    } else if (bindNullValues) {
      values.put(alias + i, AttributeValue.builder().nul(true).build());
    }
    i++;
  }

  @Override
  public void visit(TextColumn column) {
    if (!column.hasNullValue()) {
      assert column.getTextValue() != null;
      values.put(alias + i, AttributeValue.builder().s(column.getTextValue()).build());
    } else if (bindNullValues) {
      values.put(alias + i, AttributeValue.builder().nul(true).build());
    }
    i++;
  }

  @Override
  public void visit(BlobColumn column) {
    if (!column.hasNullValue()) {
      assert column.getBlobValue() != null;
      values.put(
          alias + i,
          AttributeValue.builder().b(SdkBytes.fromByteBuffer(column.getBlobValue())).build());
    } else if (bindNullValues) {
      values.put(alias + i, AttributeValue.builder().nul(true).build());
    }
    i++;
  }

  @Override
  public void visit(DateColumn column) {
    if (!column.hasNullValue()) {
      values.put(
          alias + i,
          AttributeValue.builder()
              .n(String.valueOf(TimeRelatedColumnEncodingUtils.encode(column)))
              .build());
    } else if (bindNullValues) {
      values.put(alias + i, AttributeValue.builder().nul(true).build());
    }
    i++;
  }

  @Override
  public void visit(TimeColumn column) {
    if (!column.hasNullValue()) {
      values.put(
          alias + i,
          AttributeValue.builder()
              .n(String.valueOf(TimeRelatedColumnEncodingUtils.encode(column)))
              .build());
    } else if (bindNullValues) {
      values.put(alias + i, AttributeValue.builder().nul(true).build());
    }
    i++;
  }

  @Override
  public void visit(TimestampColumn column) {
    if (!column.hasNullValue()) {
      values.put(
          alias + i,
          AttributeValue.builder()
              .n(String.valueOf(TimeRelatedColumnEncodingUtils.encode(column)))
              .build());
    } else if (bindNullValues) {
      values.put(alias + i, AttributeValue.builder().nul(true).build());
    }
    i++;
  }

  @Override
  public void visit(TimestampTZColumn column) {
    if (!column.hasNullValue()) {
      values.put(
          alias + i,
          AttributeValue.builder()
              .n(String.valueOf(TimeRelatedColumnEncodingUtils.encode(column)))
              .build());
    } else if (bindNullValues) {
      values.put(alias + i, AttributeValue.builder().nul(true).build());
    }
    i++;
  }
}
