package com.scalar.db.storage.jdbc.checker;

import com.scalar.db.io.BigIntValue;
import com.scalar.db.io.BlobValue;
import com.scalar.db.io.BooleanValue;
import com.scalar.db.io.DoubleValue;
import com.scalar.db.io.FloatValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.Value;
import com.scalar.db.io.ValueVisitor;
import com.scalar.db.storage.jdbc.metadata.DataType;
import com.scalar.db.storage.jdbc.metadata.JdbcTableMetadata;

import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
class ColumnDataTypeChecker implements ValueVisitor {
  private final JdbcTableMetadata tableMetadata;
  private String name;
  private boolean isValid;

  public ColumnDataTypeChecker(JdbcTableMetadata tableMetadata) {
    this.tableMetadata = tableMetadata;
  }

  public boolean check(Value value) {
    value.accept(this);
    return isValid;
  }

  public boolean check(String name, Value value) {
    this.name = name;
    value.accept(this);
    return isValid;
  }

  private String getName(Value value) {
    return name != null ? name : value.getName();
  }

  @Override
  public void visit(BooleanValue value) {
    isValid = tableMetadata.getDataType(getName(value)) == DataType.BOOLEAN;
  }

  @Override
  public void visit(IntValue value) {
    isValid = tableMetadata.getDataType(getName(value)) == DataType.INT;
  }

  @Override
  public void visit(BigIntValue value) {
    isValid = tableMetadata.getDataType(getName(value)) == DataType.BIGINT;
  }

  @Override
  public void visit(FloatValue value) {
    isValid = tableMetadata.getDataType(getName(value)) == DataType.FLOAT;
  }

  @Override
  public void visit(DoubleValue value) {
    isValid = tableMetadata.getDataType(getName(value)) == DataType.DOUBLE;
  }

  @Override
  public void visit(TextValue value) {
    isValid = tableMetadata.getDataType(getName(value)) == DataType.TEXT;
  }

  @Override
  public void visit(BlobValue value) {
    isValid = tableMetadata.getDataType(getName(value)) == DataType.BLOB;
  }
}
