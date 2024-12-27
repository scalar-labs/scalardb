package com.scalar.db.storage.cassandra;

import static com.google.common.base.Preconditions.checkNotNull;

import com.datastax.driver.core.BoundStatement;
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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.Date;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A visitor class to bind {@code Value}s to a {@link BoundStatement}
 *
 * @author Hiroyuki Yamada
 */
@NotThreadSafe
public final class ValueBinder implements ColumnVisitor {
  private final BoundStatement bound;
  private int i;

  /**
   * Constructs {@code ValueBinder} with the specified {@code BoundStatement}
   *
   * @param bound a {@code BoundStatement} to be bound
   */
  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public ValueBinder(BoundStatement bound) {
    this.bound = checkNotNull(bound);
    i = 0;
  }

  @Override
  public void visit(BooleanColumn column) {
    if (column.hasNullValue()) {
      bound.setToNull(i++);
    } else {
      bound.setBool(i++, column.getBooleanValue());
    }
  }

  @Override
  public void visit(IntColumn column) {
    if (column.hasNullValue()) {
      bound.setToNull(i++);
    } else {
      bound.setInt(i++, column.getIntValue());
    }
  }

  @Override
  public void visit(BigIntColumn column) {
    if (column.hasNullValue()) {
      bound.setToNull(i++);
    } else {
      bound.setLong(i++, column.getBigIntValue());
    }
  }

  @Override
  public void visit(FloatColumn column) {
    if (column.hasNullValue()) {
      bound.setToNull(i++);
    } else {
      bound.setFloat(i++, column.getFloatValue());
    }
  }

  @Override
  public void visit(DoubleColumn column) {
    if (column.hasNullValue()) {
      bound.setToNull(i++);
    } else {
      bound.setDouble(i++, column.getDoubleValue());
    }
  }

  @Override
  public void visit(TextColumn column) {
    if (column.hasNullValue()) {
      bound.setToNull(i++);
    } else {
      assert column.getTextValue() != null;
      bound.setString(i++, column.getTextValue());
    }
  }

  @Override
  public void visit(BlobColumn column) {
    if (column.hasNullValue()) {
      bound.setToNull(i++);
    } else {
      assert column.getBlobValueAsBytes() != null;
      byte[] b = column.getBlobValueAsBytes();
      bound.setBytes(i++, (ByteBuffer) ByteBuffer.allocate(b.length).put(b).flip());
    }
  }

  @Override
  public void visit(DateColumn column) {
    if (column.hasNullValue()) {
      bound.setToNull(i++);
    } else {
      assert column.getDateValue() != null;
      LocalDate date = column.getDateValue();
      bound.setDate(
          i++,
          com.datastax.driver.core.LocalDate.fromYearMonthDay(
              date.getYear(), date.getMonthValue(), date.getDayOfMonth()));
    }
  }

  @Override
  public void visit(TimeColumn column) {
    if (column.hasNullValue()) {
      bound.setToNull(i++);
    } else {
      assert column.getTimeValue() != null;
      bound.setTime(i++, column.getTimeValue().toNanoOfDay());
    }
  }

  @Override
  public void visit(TimestampColumn column) {
    throw new UnsupportedOperationException("The TIMESTAMP type is not supported with Cassandra");
  }

  @Override
  public void visit(TimestampTZColumn column) {
    if (column.hasNullValue()) {
      bound.setToNull(i++);
    } else {
      assert column.getTimestampTZValue() != null;
      bound.setTimestamp(i++, Date.from(column.getTimestampTZValue()));
    }
  }
}
