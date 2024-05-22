package com.scalar.db.common;

import com.google.common.collect.ImmutableSet;
import com.scalar.db.api.Result;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.io.Column;
import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

/** An implementation of {@code Result} that only includes projected columns. */
@Immutable
public class ProjectedResult extends AbstractResult {

  private final Result original;
  private final ImmutableSet<String> containedColumnNames;

  public ProjectedResult(Result original, List<String> projections) {
    this.original = Objects.requireNonNull(original);

    ImmutableSet.Builder<String> builder = ImmutableSet.builder();
    original.getContainedColumnNames().stream()
        .filter(c -> projections.isEmpty() || projections.contains(c))
        .forEach(builder::add);
    containedColumnNames = builder.build();
  }

  /** @deprecated As of release 3.8.0. Will be removed in release 5.0.0 */
  @Deprecated
  @Override
  public Optional<Key> getPartitionKey() {
    return getKey(original.getPartitionKey());
  }

  /** @deprecated As of release 3.8.0. Will be removed in release 5.0.0 */
  @Deprecated
  @Override
  public Optional<Key> getClusteringKey() {
    return getKey(original.getClusteringKey());
  }

  private Optional<Key> getKey(Optional<Key> key) {
    if (!key.isPresent()) {
      return Optional.empty();
    }
    for (Value<?> value : key.get()) {
      if (!containedColumnNames.contains(value.getName())) {
        throw new IllegalStateException(CoreError.COLUMN_NOT_FOUND.buildMessage(value.getName()));
      }
    }
    return key;
  }

  @Override
  public boolean isNull(String columnName) {
    checkIfExists(columnName);
    return original.isNull(columnName);
  }

  @Override
  public boolean getBoolean(String columnName) {
    checkIfExists(columnName);
    return original.getBoolean(columnName);
  }

  @Override
  public int getInt(String columnName) {
    checkIfExists(columnName);
    return original.getInt(columnName);
  }

  @Override
  public long getBigInt(String columnName) {
    checkIfExists(columnName);
    return original.getBigInt(columnName);
  }

  @Override
  public float getFloat(String columnName) {
    checkIfExists(columnName);
    return original.getFloat(columnName);
  }

  @Override
  public double getDouble(String columnName) {
    checkIfExists(columnName);
    return original.getDouble(columnName);
  }

  @Nullable
  @Override
  public String getText(String columnName) {
    checkIfExists(columnName);
    return original.getText(columnName);
  }

  @Nullable
  @Override
  public ByteBuffer getBlobAsByteBuffer(String columnName) {
    checkIfExists(columnName);
    return original.getBlobAsByteBuffer(columnName);
  }

  @Nullable
  @Override
  public byte[] getBlobAsBytes(String columnName) {
    checkIfExists(columnName);
    return original.getBlobAsBytes(columnName);
  }

  @Nullable
  @Override
  public Object getAsObject(String columnName) {
    checkIfExists(columnName);
    return original.getAsObject(columnName);
  }

  @Override
  public boolean contains(String columnName) {
    return containedColumnNames.contains(columnName);
  }

  @Override
  public Set<String> getContainedColumnNames() {
    return containedColumnNames;
  }

  @Override
  public Map<String, Column<?>> getColumns() {
    return original.getColumns().entrySet().stream()
        .filter(e -> containedColumnNames.contains(e.getKey()))
        .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
  }
}
