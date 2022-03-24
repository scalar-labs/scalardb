package com.scalar.db.sql;

import com.google.common.base.MoreObjects;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.Result;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

@Immutable
public class ResultRecord implements Record {

  private final Result result;
  private final Supplier<ImmutableMap<Integer, String>> projectedColumnNamesMap;

  public ResultRecord(Result result, ImmutableList<String> projectedColumnNames) {
    this.result = Objects.requireNonNull(result);

    Objects.requireNonNull(projectedColumnNames);
    projectedColumnNamesMap =
        Suppliers.memoize(
            () -> {
              ImmutableMap.Builder<Integer, String> builder = ImmutableMap.builder();
              for (int i = 0; i < projectedColumnNames.size(); i++) {
                builder.put(i, projectedColumnNames.get(i));
              }
              return builder.build();
            });
  }

  @Override
  public boolean isNull(String columnName) {
    return result.isNull(columnName);
  }

  @Override
  public boolean isNull(int i) {
    return result.isNull(getColumnName(i));
  }

  @Override
  public boolean getBoolean(String columnName) {
    return result.getBoolean(columnName);
  }

  @Override
  public boolean getBoolean(int i) {
    return result.getBoolean(getColumnName(i));
  }

  @Override
  public int getInt(String columnName) {
    return result.getInt(columnName);
  }

  @Override
  public int getInt(int i) {
    return result.getInt(getColumnName(i));
  }

  @Override
  public long getBigInt(String columnName) {
    return result.getBigInt(columnName);
  }

  @Override
  public long getBigInt(int i) {
    return result.getBigInt(getColumnName(i));
  }

  @Override
  public float getFloat(String columnName) {
    return result.getFloat(columnName);
  }

  @Override
  public float getFloat(int i) {
    return result.getFloat(getColumnName(i));
  }

  @Override
  public double getDouble(String columnName) {
    return result.getDouble(columnName);
  }

  @Override
  public double getDouble(int i) {
    return result.getDouble(getColumnName(i));
  }

  @Nullable
  @Override
  public String getText(String columnName) {
    return result.getText(columnName);
  }

  @Nullable
  @Override
  public String getText(int i) {
    return result.getText(getColumnName(i));
  }

  @Nullable
  @Override
  public ByteBuffer getBlobAsByteBuffer(String columnName) {
    return result.getBlobAsByteBuffer(columnName);
  }

  @Nullable
  @Override
  public ByteBuffer getBlobAsByteBuffer(int i) {
    return result.getBlobAsByteBuffer(getColumnName(i));
  }

  @Nullable
  @Override
  public byte[] getBlobAsBytes(String columnName) {
    return result.getBlobAsBytes(columnName);
  }

  @Nullable
  @Override
  public byte[] getBlobAsBytes(int i) {
    return result.getBlobAsBytes(getColumnName(i));
  }

  @Nullable
  @Override
  public Object getAsObject(String columnName) {
    return result.getAsObject(columnName);
  }

  @Nullable
  @Override
  public Object getAsObject(int i) {
    return result.getAsObject(getColumnName(i));
  }

  @Override
  public boolean contains(String columnName) {
    return result.contains(columnName);
  }

  @Override
  public Set<String> getContainedColumnNames() {
    return result.getContainedColumnNames();
  }

  @Override
  public int size() {
    return projectedColumnNamesMap.get().size();
  }

  private String getColumnName(int i) {
    if (!projectedColumnNamesMap.get().containsKey(i)) {
      throw new IndexOutOfBoundsException("Index: " + i + ", Size: " + size());
    }
    return projectedColumnNamesMap.get().get(i);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("columns", result.getColumns())
        .add("projectedColumnNamesMap", projectedColumnNamesMap)
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ResultRecord)) {
      return false;
    }
    ResultRecord that = (ResultRecord) o;
    return Objects.equals(result, that.result);
  }

  @Override
  public int hashCode() {
    return Objects.hash(result);
  }
}
