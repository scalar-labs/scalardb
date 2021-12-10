package com.scalar.db.storage.common.checker;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.BigIntValue;
import com.scalar.db.io.BlobValue;
import com.scalar.db.io.BooleanValue;
import com.scalar.db.io.DataType;
import com.scalar.db.io.DoubleValue;
import com.scalar.db.io.FloatValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.Value;
import com.scalar.db.io.ValueVisitor;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
class ColumnChecker implements ValueVisitor {
  private final TableMetadata tableMetadata;
  private final boolean notNull;
  private final boolean notEmpty;
  private final boolean notPrimaryKey;
  private String name;
  private boolean isValid;

  public ColumnChecker(
      TableMetadata tableMetadata, boolean notNull, boolean notEmpty, boolean notPrimaryKey) {
    this.tableMetadata = tableMetadata;
    this.notNull = notNull;
    this.notEmpty = notEmpty;
    this.notPrimaryKey = notPrimaryKey;
  }

  public boolean check(Value<?> value) {
    String name = getName(value);

    // Check if the column exists
    if (!tableMetadata.getColumnNames().contains(name)) {
      return false;
    }

    if (notPrimaryKey) {
      // Check if the column is primary key or not
      if (tableMetadata.getPartitionKeyNames().contains(name)
          || tableMetadata.getClusteringKeyNames().contains(name)) {
        return false;
      }
    }

    // Check if the column data type is correct and the column value is null or empty
    value.accept(this);
    return isValid;
  }

  public boolean check(String name, Value<?> value) {
    this.name = name;
    return check(value);
  }

  private String getName(Value<?> value) {
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
    if (notNull && !value.getAsString().isPresent()) {
      isValid = false;
      return;
    }
    if (notEmpty && (value.getAsString().isPresent() && value.getAsString().get().isEmpty())) {
      isValid = false;
      return;
    }

    isValid = tableMetadata.getColumnDataType(getName(value)) == DataType.TEXT;
  }

  @Override
  public void visit(BlobValue value) {
    if (notNull && !value.getAsBytes().isPresent()) {
      isValid = false;
      return;
    }
    if (notEmpty && (value.getAsBytes().isPresent() && value.getAsBytes().get().length == 0)) {
      isValid = false;
      return;
    }
    isValid = tableMetadata.getColumnDataType(getName(value)) == DataType.BLOB;
  }
}
