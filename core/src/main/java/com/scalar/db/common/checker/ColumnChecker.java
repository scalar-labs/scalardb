package com.scalar.db.common.checker;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.BlobColumn;
import com.scalar.db.io.BooleanColumn;
import com.scalar.db.io.Column;
import com.scalar.db.io.ColumnVisitor;
import com.scalar.db.io.DataType;
import com.scalar.db.io.DateColumn;
import com.scalar.db.io.DoubleColumn;
import com.scalar.db.io.FloatColumn;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.TextColumn;
import com.scalar.db.io.TimeColumn;
import com.scalar.db.io.TimestampColumn;
import com.scalar.db.io.TimestampTZColumn;
import javax.annotation.concurrent.NotThreadSafe;

/** A checker for the columns of a table for the storage abstraction. */
@NotThreadSafe
public class ColumnChecker implements ColumnVisitor {
  private final TableMetadata tableMetadata;
  private final boolean requireNotNull;
  private final boolean requireNull;
  private final boolean requireNotEmpty;
  private final boolean requireNotPrimaryKey;
  private boolean isValid;

  public ColumnChecker(
      TableMetadata tableMetadata,
      boolean requireNotNull,
      boolean requireNull,
      boolean requireNotEmpty,
      boolean requireNotPrimaryKey) {
    this.tableMetadata = tableMetadata;
    this.requireNotNull = requireNotNull;
    this.requireNull = requireNull;
    this.requireNotEmpty = requireNotEmpty;
    this.requireNotPrimaryKey = requireNotPrimaryKey;
  }

  public boolean check(Column<?> column) {
    // Check if the column exists
    if (!tableMetadata.getColumnNames().contains(column.getName())) {
      return false;
    }

    if (requireNotPrimaryKey) {
      // Check if the column is primary key or not
      if (tableMetadata.getPartitionKeyNames().contains(column.getName())
          || tableMetadata.getClusteringKeyNames().contains(column.getName())) {
        return false;
      }
    }

    // Check if the column data type is correct and the column value is null or empty
    column.accept(this);
    return isValid;
  }

  @Override
  public void visit(BooleanColumn column) {
    if (requireNotNull && column.hasNullValue()) {
      isValid = false;
      return;
    }
    if (requireNull && !column.hasNullValue()) {
      isValid = false;
      return;
    }
    isValid = tableMetadata.getColumnDataType(column.getName()) == DataType.BOOLEAN;
  }

  @Override
  public void visit(IntColumn column) {
    if (requireNotNull && column.hasNullValue()) {
      isValid = false;
      return;
    }
    if (requireNull && !column.hasNullValue()) {
      isValid = false;
      return;
    }
    isValid = tableMetadata.getColumnDataType(column.getName()) == DataType.INT;
  }

  @Override
  public void visit(BigIntColumn column) {
    if (requireNotNull && column.hasNullValue()) {
      isValid = false;
      return;
    }
    if (requireNull && !column.hasNullValue()) {
      isValid = false;
      return;
    }
    isValid = tableMetadata.getColumnDataType(column.getName()) == DataType.BIGINT;
  }

  @Override
  public void visit(FloatColumn column) {
    if (requireNotNull && column.hasNullValue()) {
      isValid = false;
      return;
    }
    if (requireNull && !column.hasNullValue()) {
      isValid = false;
      return;
    }
    isValid = tableMetadata.getColumnDataType(column.getName()) == DataType.FLOAT;
  }

  @Override
  public void visit(DoubleColumn column) {
    if (requireNotNull && column.hasNullValue()) {
      isValid = false;
      return;
    }
    if (requireNull && !column.hasNullValue()) {
      isValid = false;
      return;
    }
    isValid = tableMetadata.getColumnDataType(column.getName()) == DataType.DOUBLE;
  }

  @Override
  public void visit(TextColumn column) {
    if (requireNotNull && column.hasNullValue()) {
      isValid = false;
      return;
    }
    if (requireNull && !column.hasNullValue()) {
      isValid = false;
      return;
    }
    if (requireNotEmpty) {
      String textValue = column.getTextValue();
      if (textValue != null && textValue.isEmpty()) {
        isValid = false;
        return;
      }
    }
    isValid = tableMetadata.getColumnDataType(column.getName()) == DataType.TEXT;
  }

  @Override
  public void visit(BlobColumn column) {
    if (requireNotNull && column.hasNullValue()) {
      isValid = false;
      return;
    }
    if (requireNull && !column.hasNullValue()) {
      isValid = false;
      return;
    }
    if (requireNotEmpty) {
      byte[] blobValue = column.getBlobValueAsBytes();
      if (blobValue != null && blobValue.length == 0) {
        isValid = false;
        return;
      }
    }
    isValid = tableMetadata.getColumnDataType(column.getName()) == DataType.BLOB;
  }

  @Override
  public void visit(DateColumn column) {
    if (requireNotNull && column.hasNullValue()) {
      isValid = false;
      return;
    }
    if (requireNull && !column.hasNullValue()) {
      isValid = false;
      return;
    }

    isValid = tableMetadata.getColumnDataType(column.getName()) == DataType.DATE;
  }

  @Override
  public void visit(TimeColumn column) {
    if (requireNotNull && column.hasNullValue()) {
      isValid = false;
      return;
    }
    if (requireNull && !column.hasNullValue()) {
      isValid = false;
      return;
    }

    isValid = tableMetadata.getColumnDataType(column.getName()) == DataType.TIME;
  }

  @Override
  public void visit(TimestampColumn column) {
    if (requireNotNull && column.hasNullValue()) {
      isValid = false;
      return;
    }
    if (requireNull && !column.hasNullValue()) {
      isValid = false;
      return;
    }

    isValid = tableMetadata.getColumnDataType(column.getName()) == DataType.TIMESTAMP;
  }

  @Override
  public void visit(TimestampTZColumn column) {
    if (requireNotNull && column.hasNullValue()) {
      isValid = false;
      return;
    }
    if (requireNull && !column.hasNullValue()) {
      isValid = false;
      return;
    }

    isValid = tableMetadata.getColumnDataType(column.getName()) == DataType.TIMESTAMPTZ;
  }
}
