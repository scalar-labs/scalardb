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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A visitor to make a map to be used to create {@link Record}
 *
 * @author Yuji Ito
 */
@NotThreadSafe
public class MapVisitor implements ColumnVisitor {
  private final Map<String, Object> values = new HashMap<>();

  @SuppressFBWarnings("EI_EXPOSE_REP")
  public Map<String, Object> get() {
    return values;
  }

  @Override
  public void visit(BooleanColumn column) {
    values.put(column.getName(), column.hasNullValue() ? null : column.getBooleanValue());
  }

  @Override
  public void visit(IntColumn column) {
    values.put(column.getName(), column.hasNullValue() ? null : column.getIntValue());
  }

  @Override
  public void visit(BigIntColumn column) {
    values.put(column.getName(), column.hasNullValue() ? null : column.getBigIntValue());
  }

  @Override
  public void visit(FloatColumn column) {
    values.put(column.getName(), column.hasNullValue() ? null : column.getFloatValue());
  }

  @Override
  public void visit(DoubleColumn column) {
    values.put(column.getName(), column.hasNullValue() ? null : column.getDoubleValue());
  }

  @Override
  public void visit(TextColumn column) {
    values.put(column.getName(), column.hasNullValue() ? null : column.getTextValue());
  }

  @Override
  public void visit(BlobColumn column) {
    values.put(column.getName(), column.hasNullValue() ? null : column.getBlobValue());
  }

  @Override
  public void visit(DateColumn column) {
    values.put(
        column.getName(),
        column.hasNullValue() ? null : TimeRelatedColumnEncodingUtils.encode(column));
  }

  @Override
  public void visit(TimeColumn column) {
    values.put(
        column.getName(),
        column.hasNullValue() ? null : TimeRelatedColumnEncodingUtils.encode(column));
  }

  @Override
  public void visit(TimestampColumn column) {
    values.put(
        column.getName(),
        column.hasNullValue() ? null : TimeRelatedColumnEncodingUtils.encode(column));
  }

  @Override
  public void visit(TimestampTZColumn column) {
    values.put(
        column.getName(),
        column.hasNullValue() ? null : TimeRelatedColumnEncodingUtils.encode(column));
  }
}
