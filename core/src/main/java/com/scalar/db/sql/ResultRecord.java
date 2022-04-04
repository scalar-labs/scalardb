package com.scalar.db.sql;

import com.google.common.base.MoreObjects;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.Result;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

@Immutable
public class ResultRecord implements Record {

  private final Result result;
  private final Supplier<ImmutableMap<String, String>> projectionAliasMap;
  private final Supplier<ImmutableMap<Integer, String>> projectionIndexMap;

  ResultRecord(Result result, List<Projection> projections) {
    this.result = Objects.requireNonNull(result);

    Objects.requireNonNull(projections);

    projectionAliasMap =
        Suppliers.memoize(
            () ->
                ImmutableMap.copyOf(
                    projections.stream()
                        .collect(
                            Collectors.toMap(
                                p -> p.alias != null ? p.alias : p.columnName,
                                p -> p.columnName))));

    projectionIndexMap =
        Suppliers.memoize(
            () -> {
              ImmutableMap.Builder<Integer, String> builder = ImmutableMap.builder();
              for (int i = 0; i < projections.size(); i++) {
                builder.put(i, projections.get(i).columnName);
              }
              return builder.build();
            });
  }

  @Override
  public boolean isNull(String columnNameOrAlias) {
    return result.isNull(getActualColumnName(columnNameOrAlias));
  }

  @Override
  public boolean isNull(int i) {
    return result.isNull(getColumnNameByIndex(i));
  }

  @Override
  public boolean getBoolean(String columnNameOrAlias) {
    return result.getBoolean(getActualColumnName(columnNameOrAlias));
  }

  @Override
  public boolean getBoolean(int i) {
    return result.getBoolean(getColumnNameByIndex(i));
  }

  @Override
  public int getInt(String columnNameOrAlias) {
    return result.getInt(getActualColumnName(columnNameOrAlias));
  }

  @Override
  public int getInt(int i) {
    return result.getInt(getColumnNameByIndex(i));
  }

  @Override
  public long getBigInt(String columnNameOrAlias) {
    return result.getBigInt(getActualColumnName(columnNameOrAlias));
  }

  @Override
  public long getBigInt(int i) {
    return result.getBigInt(getColumnNameByIndex(i));
  }

  @Override
  public float getFloat(String columnNameOrAlias) {
    return result.getFloat(getActualColumnName(columnNameOrAlias));
  }

  @Override
  public float getFloat(int i) {
    return result.getFloat(getColumnNameByIndex(i));
  }

  @Override
  public double getDouble(String columnNameOrAlias) {
    return result.getDouble(getActualColumnName(columnNameOrAlias));
  }

  @Override
  public double getDouble(int i) {
    return result.getDouble(getColumnNameByIndex(i));
  }

  @Nullable
  @Override
  public String getText(String columnNameOrAlias) {
    return result.getText(getActualColumnName(columnNameOrAlias));
  }

  @Nullable
  @Override
  public String getText(int i) {
    return result.getText(getColumnNameByIndex(i));
  }

  @Nullable
  @Override
  public ByteBuffer getBlobAsByteBuffer(String columnNameOrAlias) {
    return result.getBlobAsByteBuffer(getActualColumnName(columnNameOrAlias));
  }

  @Nullable
  @Override
  public ByteBuffer getBlobAsByteBuffer(int i) {
    return result.getBlobAsByteBuffer(getColumnNameByIndex(i));
  }

  @Nullable
  @Override
  public byte[] getBlobAsBytes(String columnNameOrAlias) {
    return result.getBlobAsBytes(getActualColumnName(columnNameOrAlias));
  }

  @Nullable
  @Override
  public byte[] getBlobAsBytes(int i) {
    return result.getBlobAsBytes(getColumnNameByIndex(i));
  }

  @Nullable
  @Override
  public Object getAsObject(String columnNameOrAlias) {
    return result.getAsObject(getActualColumnName(columnNameOrAlias));
  }

  @Nullable
  @Override
  public Object getAsObject(int i) {
    return result.getAsObject(getColumnNameByIndex(i));
  }

  @Override
  public boolean contains(String columnNameOrAlias) {
    return result.contains(getActualColumnName(columnNameOrAlias));
  }

  @Override
  public Set<String> getContainedColumnNames() {
    return projectionAliasMap.get().keySet();
  }

  @Override
  public int size() {
    return projectionIndexMap.get().size();
  }

  private String getActualColumnName(String columnNameOrAlias) {
    if (!projectionAliasMap.get().containsKey(columnNameOrAlias)) {
      throw new IllegalArgumentException(columnNameOrAlias + " doesn't exist");
    }
    return projectionAliasMap.get().get(columnNameOrAlias);
  }

  private String getColumnNameByIndex(int i) {
    if (!projectionIndexMap.get().containsKey(i)) {
      throw new IndexOutOfBoundsException("Index: " + i + ", Size: " + size());
    }
    return projectionIndexMap.get().get(i);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("result", result)
        .add("projectionAliasMap", projectionAliasMap)
        .add("projectionIndexMap", projectionIndexMap)
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
