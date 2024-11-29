package com.scalar.db.transaction.consensuscommit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.scalar.db.api.Result;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.AbstractResult;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.io.Column;
import com.scalar.db.io.Key;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

/**
 * An implementation of {@code Result} to filter out unprojected columns and transaction columns.
 */
@Immutable
public class FilteredResult extends AbstractResult {

  private final Result original;
  private final ImmutableSet<String> containedColumnNames;

  public FilteredResult(
      Result original,
      List<String> projections,
      TableMetadata metadata,
      boolean isIncludeMetadataEnabled) {
    this.original = Objects.requireNonNull(original);

    ImmutableSet.Builder<String> builder = ImmutableSet.builder();
    original.getContainedColumnNames().stream()
        .filter(c -> projections.isEmpty() || projections.contains(c))
        .filter(
            c ->
                isIncludeMetadataEnabled
                    || !ConsensusCommitUtils.isTransactionMetaColumn(c, metadata))
        .forEach(builder::add);
    containedColumnNames = builder.build();
  }

  /** @deprecated As of release 3.8.0. Will be removed in release 5.0.0 */
  @Deprecated
  @Override
  public Optional<Key> getPartitionKey() {
    Optional<Key> partitionKey = original.getPartitionKey();
    checkKey(partitionKey);
    return partitionKey;
  }

  /** @deprecated As of release 3.8.0. Will be removed in release 5.0.0 */
  @Deprecated
  @Override
  public Optional<Key> getClusteringKey() {
    Optional<Key> clusteringKey = original.getClusteringKey();
    checkKey(clusteringKey);
    return clusteringKey;
  }

  private void checkKey(Optional<Key> key) {
    if (!key.isPresent()) {
      return;
    }
    for (Column<?> column : key.get().getColumns()) {
      if (!containedColumnNames.contains(column.getName())) {
        throw new IllegalStateException(CoreError.COLUMN_NOT_FOUND.buildMessage(column.getName()));
      }
    }
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
  public LocalDate getDate(String columnName) {
    checkIfExists(columnName);
    return original.getDate(columnName);
  }

  @Nullable
  @Override
  public LocalTime getTime(String columnName) {
    checkIfExists(columnName);
    return original.getTime(columnName);
  }

  @Nullable
  @Override
  public LocalDateTime getTimestamp(String columnName) {
    checkIfExists(columnName);
    return original.getTimestamp(columnName);
  }

  @Nullable
  @Override
  public Instant getTimestampTZ(String columnName) {
    checkIfExists(columnName);
    return original.getTimestampTZ(columnName);
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

  @VisibleForTesting
  Result getOriginalResult() {
    return original;
  }

  @Override
  public Map<String, Column<?>> getColumns() {
    return original.getColumns().entrySet().stream()
        .filter(e -> containedColumnNames.contains(e.getKey()))
        .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
  }
}
