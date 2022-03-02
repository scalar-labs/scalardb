package com.scalar.db.sql;

import com.google.common.collect.ImmutableSet;
import java.nio.ByteBuffer;
import java.util.Set;
import javax.annotation.Nullable;

public class ConditionalMutationResultRecord implements Record {

  private static final String COLUMN_NAME = "result";
  private static final ImmutableSet<String> CONTAINED_COLUMN_NAMES = ImmutableSet.of(COLUMN_NAME);

  private final boolean result;

  public ConditionalMutationResultRecord(boolean result) {
    this.result = result;
  }

  @Override
  public boolean isNull(String columnName) {
    checkColumnName(columnName);
    return false;
  }

  @Override
  public boolean isNull(int i) {
    checkIndex(i);
    return false;
  }

  @Override
  public boolean getBoolean(String columnName) {
    checkColumnName(columnName);
    return result;
  }

  @Override
  public boolean getBoolean(int i) {
    checkIndex(i);
    return result;
  }

  @Override
  public int getInt(String columnName) {
    checkColumnName(columnName);
    throw new UnsupportedOperationException("The data type of the column is BOOLEAN");
  }

  @Override
  public int getInt(int i) {
    checkIndex(i);
    throw new UnsupportedOperationException("The data type of the column is BOOLEAN");
  }

  @Override
  public long getBigInt(String columnName) {
    checkColumnName(columnName);
    throw new UnsupportedOperationException("The data type of the column is BOOLEAN");
  }

  @Override
  public long getBigInt(int i) {
    checkIndex(i);
    throw new UnsupportedOperationException("The data type of the column is BOOLEAN");
  }

  @Override
  public float getFloat(String columnName) {
    checkColumnName(columnName);
    throw new UnsupportedOperationException("The data type of the column is BOOLEAN");
  }

  @Override
  public float getFloat(int i) {
    checkIndex(i);
    throw new UnsupportedOperationException("The data type of the column is BOOLEAN");
  }

  @Override
  public double getDouble(String columnName) {
    checkColumnName(columnName);
    throw new UnsupportedOperationException("The data type of the column is BOOLEAN");
  }

  @Override
  public double getDouble(int i) {
    checkIndex(i);
    throw new UnsupportedOperationException("The data type of the column is BOOLEAN");
  }

  @Nullable
  @Override
  public String getText(String columnName) {
    checkColumnName(columnName);
    throw new UnsupportedOperationException("The data type of the column is BOOLEAN");
  }

  @Nullable
  @Override
  public String getText(int i) {
    checkIndex(i);
    throw new UnsupportedOperationException("The data type of the column is BOOLEAN");
  }

  @Nullable
  @Override
  public ByteBuffer getBlobAsByteBuffer(String columnName) {
    checkColumnName(columnName);
    throw new UnsupportedOperationException("The data type of the column is BOOLEAN");
  }

  @Nullable
  @Override
  public ByteBuffer getBlobAsByteBuffer(int i) {
    checkIndex(i);
    throw new UnsupportedOperationException("The data type of the column is BOOLEAN");
  }

  @Nullable
  @Override
  public byte[] getBlobAsBytes(String columnName) {
    checkColumnName(columnName);
    throw new UnsupportedOperationException("The data type of the column is BOOLEAN");
  }

  @Nullable
  @Override
  public byte[] getBlobAsBytes(int i) {
    checkIndex(i);
    throw new UnsupportedOperationException("The data type of the column is BOOLEAN");
  }

  @Nullable
  @Override
  public Object getAsObject(String columnName) {
    checkColumnName(columnName);
    throw new UnsupportedOperationException("The data type of the column is BOOLEAN");
  }

  @Nullable
  @Override
  public Object getAsObject(int i) {
    checkIndex(i);
    return result;
  }

  private void checkColumnName(String columnName) {
    if (!columnName.equals(COLUMN_NAME)) {
      throw new IllegalArgumentException(columnName + " doesn't exist");
    }
  }

  private void checkIndex(int i) {
    if (i != 0) {
      throw new IndexOutOfBoundsException("Index: " + i + ", Size: 1");
    }
  }

  @Override
  public boolean contains(String columnName) {
    return CONTAINED_COLUMN_NAMES.contains(columnName);
  }

  @Override
  public Set<String> getContainedColumnNames() {
    return CONTAINED_COLUMN_NAMES;
  }

  @Override
  public int size() {
    return 1;
  }
}
