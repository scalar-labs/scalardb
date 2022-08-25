package com.scalar.db.storage.cassandra;

import static com.google.common.base.Preconditions.checkNotNull;

import com.datastax.driver.core.BoundStatement;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.BlobColumn;
import com.scalar.db.io.BooleanColumn;
import com.scalar.db.io.ColumnVisitor;
import com.scalar.db.io.DoubleColumn;
import com.scalar.db.io.FloatColumn;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.TextColumn;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.nio.ByteBuffer;
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
}
