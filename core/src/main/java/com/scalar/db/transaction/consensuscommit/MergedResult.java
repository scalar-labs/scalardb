package com.scalar.db.transaction.consensuscommit;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.Put;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.Value;
import com.scalar.db.util.AbstractResult;
import com.scalar.db.util.ScalarDbUtils;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

@Immutable
public class MergedResult extends AbstractResult {
  private final Optional<TransactionResult> result;
  private final Put put;
  private final Map<String, Optional<Value<?>>> putValues;
  private final TableMetadata metadata;

  private final Supplier<Map<String, Value<?>>> valuesWithDefaultValues;

  public MergedResult(Optional<TransactionResult> result, Put put, TableMetadata metadata) {
    // assume that all the columns are projected to the result
    this.result = result;
    this.put = put;

    putValues = new HashMap<>();
    putValues.putAll(put.getNullableValues());
    put.getPartitionKey().get().forEach(v -> putValues.put(v.getName(), Optional.of(v)));
    put.getClusteringKey()
        .ifPresent(k -> k.forEach(v -> putValues.put(v.getName(), Optional.of(v))));

    this.metadata = metadata;

    // lazy loading
    valuesWithDefaultValues =
        Suppliers.memoize(
            () -> {
              ImmutableMap.Builder<String, Value<?>> builder = ImmutableMap.builder();
              if (result.isPresent()) {
                result
                    .get()
                    .getValues()
                    .forEach(
                        (k, v) -> {
                          if (putValues.containsKey(k)) {
                            Optional<Value<?>> value = putValues.get(k);
                            if (value.isPresent()) {
                              builder.put(k, value.get());
                            } else {
                              builder.put(k, ScalarDbUtils.getDefaultValue(k, metadata));
                            }
                          } else {
                            builder.put(k, v);
                          }
                        });
              } else {
                for (String columnName : metadata.getColumnNames()) {
                  if (putValues.containsKey(columnName) && putValues.get(columnName).isPresent()) {
                    builder.put(columnName, putValues.get(columnName).get());
                  } else {
                    builder.put(columnName, ScalarDbUtils.getDefaultValue(columnName, metadata));
                  }
                }
              }
              return builder.build();
            });
  }

  @Override
  public Optional<com.scalar.db.io.Key> getPartitionKey() {
    return Optional.of(put.getPartitionKey());
  }

  @Override
  public Optional<com.scalar.db.io.Key> getClusteringKey() {
    return put.getClusteringKey();
  }

  @Deprecated
  @Override
  public Optional<Value<?>> getValue(String columnName) {
    return Optional.of(valuesWithDefaultValues.get().get(columnName));
  }

  @Deprecated
  @Override
  public Map<String, Value<?>> getValues() {
    return valuesWithDefaultValues.get();
  }

  @Override
  public boolean isNull(String columnName) {
    checkIfExists(columnName);
    if (putValues.containsKey(columnName)) {
      return !putValues.get(columnName).isPresent();
    }
    return result.map(transactionResult -> transactionResult.isNull(columnName)).orElse(true);
  }

  @Override
  public boolean getBoolean(String columnName) {
    checkIfExists(columnName);
    if (putValues.containsKey(columnName)) {
      return putValues.get(columnName).map(Value::getAsBoolean).orElse(false);
    }
    return result.map(r -> r.getBoolean(columnName)).orElse(false);
  }

  @Override
  public int getInt(String columnName) {
    checkIfExists(columnName);
    if (putValues.containsKey(columnName)) {
      return putValues.get(columnName).map(Value::getAsInt).orElse(0);
    }
    return result.map(r -> r.getInt(columnName)).orElse(0);
  }

  @Override
  public long getBigInt(String columnName) {
    checkIfExists(columnName);
    if (putValues.containsKey(columnName)) {
      return putValues.get(columnName).map(Value::getAsLong).orElse(0L);
    }
    return result.map(r -> r.getBigInt(columnName)).orElse(0L);
  }

  @Override
  public float getFloat(String columnName) {
    checkIfExists(columnName);
    if (putValues.containsKey(columnName)) {
      return putValues.get(columnName).map(Value::getAsFloat).orElse(0.0F);
    }
    return result.map(r -> r.getFloat(columnName)).orElse(0.0F);
  }

  @Override
  public double getDouble(String columnName) {
    checkIfExists(columnName);
    if (putValues.containsKey(columnName)) {
      return putValues.get(columnName).map(Value::getAsDouble).orElse(0.0D);
    }
    return result.map(r -> r.getDouble(columnName)).orElse(0.0D);
  }

  @Nullable
  @Override
  public String getText(String columnName) {
    checkIfExists(columnName);
    if (putValues.containsKey(columnName)) {
      return putValues.get(columnName).flatMap(Value::getAsString).orElse(null);
    }
    return result.map(r -> r.getText(columnName)).orElse(null);
  }

  @Nullable
  @Override
  public ByteBuffer getBlobAsByteBuffer(String columnName) {
    checkIfExists(columnName);
    if (putValues.containsKey(columnName)) {
      return putValues.get(columnName).flatMap(Value::getAsByteBuffer).orElse(null);
    }
    return result.map(r -> r.getBlobAsByteBuffer(columnName)).orElse(null);
  }

  @Nullable
  @Override
  public byte[] getBlobAsBytes(String columnName) {
    checkIfExists(columnName);
    if (putValues.containsKey(columnName)) {
      return putValues.get(columnName).flatMap(Value::getAsBytes).orElse(null);
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
}
