package com.scalar.db.storage.common.checker;

import com.scalar.db.io.BigIntValue;
import com.scalar.db.io.BlobValue;
import com.scalar.db.io.BooleanValue;
import com.scalar.db.io.DoubleValue;
import com.scalar.db.io.FloatValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.Value;
import com.scalar.db.io.ValueVisitor;
import com.scalar.db.storage.common.metadata.DataType;
import com.scalar.db.storage.common.metadata.TableMetadata;

import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
class ColumnChecker implements ValueVisitor {
  private final TableMetadata tableMetadata;
  private String name;
  private boolean isValid;

  public ColumnChecker(TableMetadata tableMetadata) {
    this.tableMetadata = tableMetadata;
  }

  public boolean check(Value value) {
    // Check if the column exists
    if (!tableMetadata.getColumnNames().contains(getName(value))) {
      return false;
    }

    // Check if the column data type is correct
    value.accept(this);
    return isValid;
  }

  public boolean check(String name, Value value) {
    this.name = name;
    return check(value);
  }

  private String getName(Value value) {
    return name != null ? name : value.getName();
  }

  @Override
  public void visit(BooleanValue value) {
    isValid = tableMetadata.getColumnDataType(getName(value)) == DataType.BOOLEAN;
  }

  @Override
  public void visit(IntValue value) {
    isValid = tableMetadata.getColumnDataType(getName(value)) == DataType.INT;
  }

  @Override
  public void visit(BigIntValue value) {
    isValid = tableMetadata.getColumnDataType(getName(value)) == DataType.BIGINT;
  }

  @Override
  public void visit(FloatValue value) {
    isValid = tableMetadata.getColumnDataType(getName(value)) == DataType.FLOAT;
  }

  @Override
  public void visit(DoubleValue value) {
    isValid = tableMetadata.getColumnDataType(getName(value)) == DataType.DOUBLE;
  }

  @Override
  public void visit(TextValue value) {
    isValid = tableMetadata.getColumnDataType(getName(value)) == DataType.TEXT;
  }

  @Override
  public void visit(BlobValue value) {
    isValid = tableMetadata.getColumnDataType(getName(value)) == DataType.BLOB;
  }
}
