package com.scalar.db.transaction.consensuscommit;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.Put;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.AbstractResult;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.BlobColumn;
import com.scalar.db.io.BooleanColumn;
import com.scalar.db.io.Column;
import com.scalar.db.io.DoubleColumn;
import com.scalar.db.io.FloatColumn;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

@Immutable
public class MergedResult extends AbstractResult {
  private final Optional<TransactionResult> result;
  private final Put put;
  private final Map<String, Column<?>> putColumns;
  private final TableMetadata metadata;

  public MergedResult(Optional<TransactionResult> result, Put put, TableMetadata metadata) {
    // assume that all the columns are projected to the result
    this.result = result;
    this.put = put;

    putColumns = new HashMap<>();
    putColumns.putAll(put.getColumns());
    put.getPartitionKey().getColumns().forEach(c -> putColumns.put(c.getName(), c));
    put.getClusteringKey()
        .ifPresent(k -> k.getColumns().forEach(c -> putColumns.put(c.getName(), c)));

    this.metadata = metadata;
  }

  /** @deprecated As of release 3.8.0. Will be removed in release 5.0.0 */
  @Deprecated
  @Override
  public Optional<Key> getPartitionKey() {
    return Optional.of(put.getPartitionKey());
  }

  /** @deprecated As of release 3.8.0. Will be removed in release 5.0.0 */
  @Deprecated
  @Override
  public Optional<Key> getClusteringKey() {
    return put.getClusteringKey();
  }

  @Override
  public boolean isNull(String columnName) {
    checkIfExists(columnName);
    if (putColumns.containsKey(columnName)) {
      return putColumns.get(columnName).hasNullValue();
    }
    return result.map(transactionResult -> transactionResult.isNull(columnName)).orElse(true);
  }

  @Override
  public boolean getBoolean(String columnName) {
    checkIfExists(columnName);
    if (putColumns.containsKey(columnName)) {
      return putColumns.get(columnName).getBooleanValue();
    }
    return result.map(r -> r.getBoolean(columnName)).orElse(false);
  }

  @Override
  public int getInt(String columnName) {
    checkIfExists(columnName);
    if (putColumns.containsKey(columnName)) {
      return putColumns.get(columnName).getIntValue();
    }
    return result.map(r -> r.getInt(columnName)).orElse(0);
  }

  @Override
  public long getBigInt(String columnName) {
    checkIfExists(columnName);
    if (putColumns.containsKey(columnName)) {
      return putColumns.get(columnName).getBigIntValue();
    }
    return result.map(r -> r.getBigInt(columnName)).orElse(0L);
  }

  @Override
  public float getFloat(String columnName) {
    checkIfExists(columnName);
    if (putColumns.containsKey(columnName)) {
      return putColumns.get(columnName).getFloatValue();
    }
    return result.map(r -> r.getFloat(columnName)).orElse(0.0F);
  }

  @Override
  public double getDouble(String columnName) {
    checkIfExists(columnName);
    if (putColumns.containsKey(columnName)) {
      return putColumns.get(columnName).getDoubleValue();
    }
    return result.map(r -> r.getDouble(columnName)).orElse(0.0D);
  }

  @Nullable
  @Override
  public String getText(String columnName) {
    checkIfExists(columnName);
    if (putColumns.containsKey(columnName)) {
      return putColumns.get(columnName).getTextValue();
    }
    return result.map(r -> r.getText(columnName)).orElse(null);
  }

  @Nullable
  @Override
  public ByteBuffer getBlobAsByteBuffer(String columnName) {
    checkIfExists(columnName);
    if (putColumns.containsKey(columnName)) {
      return putColumns.get(columnName).getBlobValueAsByteBuffer();
    }
    return result.map(r -> r.getBlobAsByteBuffer(columnName)).orElse(null);
  }

  @Nullable
  @Override
  public byte[] getBlobAsBytes(String columnName) {
    checkIfExists(columnName);
    if (putColumns.containsKey(columnName)) {
      return putColumns.get(columnName).getBlobValueAsBytes();
    }
    return result.map(r -> r.getBlobAsBytes(columnName)).orElse(null);
  }

  @Nullable
  @Override
  public Object getAsObject(String columnName) {
    checkIfExists(columnName);
    if (isNull(columnName)) {
      return null;
    }

    switch (metadata.getColumnDataType(columnName)) {
      case BOOLEAN:
        return getBoolean(columnName);
      case INT:
        return getInt(columnName);
      case BIGINT:
        return getBigInt(columnName);
      case FLOAT:
        return getFloat(columnName);
      case DOUBLE:
        return getDouble(columnName);
      case TEXT:
        return getText(columnName);
      case BLOB:
        return getBlob(columnName);
      default:
        throw new AssertionError();
    }
  }

  @Override
  public boolean contains(String columnName) {
    return result
        .map(r -> r.getContainedColumnNames().contains(columnName))
        .orElse(metadata.getColumnNames().contains(columnName));
  }

  @Override
  public Set<String> getContainedColumnNames() {
    return result.map(TransactionResult::getContainedColumnNames).orElse(metadata.getColumnNames());
  }

  @Override
  public Map<String, Column<?>> getColumns() {
    ImmutableMap.Builder<String, Column<?>> builder = ImmutableMap.builder();
    if (result.isPresent()) {
      result.get().getColumns().forEach((k, v) -> builder.put(k, putColumns.getOrDefault(k, v)));
    } else {
      for (String columnName : metadata.getColumnNames()) {
        if (putColumns.containsKey(columnName)) {
          builder.put(columnName, putColumns.get(columnName));
        } else {
          builder.put(columnName, getNullColumn(columnName));
        }
      }
    }
    return builder.build();
  }

  private Column<?> getNullColumn(String columnName) {
    switch (metadata.getColumnDataType(columnName)) {
      case BOOLEAN:
        return BooleanColumn.ofNull(columnName);
      case INT:
        return IntColumn.ofNull(columnName);
      case BIGINT:
        return BigIntColumn.ofNull(columnName);
      case FLOAT:
        return FloatColumn.ofNull(columnName);
      case DOUBLE:
        return DoubleColumn.ofNull(columnName);
      case TEXT:
        return TextColumn.ofNull(columnName);
      case BLOB:
        return BlobColumn.ofNull(columnName);
      default:
        throw new AssertionError();
    }
  }
}
