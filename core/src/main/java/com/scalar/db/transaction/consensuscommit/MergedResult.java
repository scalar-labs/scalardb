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
    // assume that all the values are projected to the result
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

  @Override
  public Optional<Value<?>> getValue(String name) {
    return Optional.of(valuesWithDefaultValues.get().get(name));
  }

  @Override
  public Map<String, Value<?>> getValues() {
    return valuesWithDefaultValues.get();
  }

  @Override
  public boolean isNull(String name) {
    checkIfExists(name);
    if (putValues.containsKey(name)) {
      return !putValues.get(name).isPresent();
    }
    return result.map(transactionResult -> transactionResult.isNull(name)).orElse(true);
  }

  @Override
  public boolean getBoolean(String name) {
    checkIfExists(name);
    if (putValues.containsKey(name)) {
      return putValues.get(name).map(Value::getAsBoolean).orElse(false);
    }
    return result.map(r -> r.getBoolean(name)).orElse(false);
  }

  @Override
  public int getInt(String name) {
    checkIfExists(name);
    if (putValues.containsKey(name)) {
      return putValues.get(name).map(Value::getAsInt).orElse(0);
    }
    return result.map(r -> r.getInt(name)).orElse(0);
  }

  @Override
  public long getBigInt(String name) {
    checkIfExists(name);
    if (putValues.containsKey(name)) {
      return putValues.get(name).map(Value::getAsLong).orElse(0L);
    }
    return result.map(r -> r.getBigInt(name)).orElse(0L);
  }

  @Override
  public float getFloat(String name) {
    checkIfExists(name);
    if (putValues.containsKey(name)) {
      return putValues.get(name).map(Value::getAsFloat).orElse(0.0F);
    }
    return result.map(r -> r.getFloat(name)).orElse(0.0F);
  }

  @Override
  public double getDouble(String name) {
    checkIfExists(name);
    if (putValues.containsKey(name)) {
      return putValues.get(name).map(Value::getAsDouble).orElse(0.0D);
    }
    return result.map(r -> r.getDouble(name)).orElse(0.0D);
  }

  @Nullable
  @Override
  public String getText(String name) {
    checkIfExists(name);
    if (putValues.containsKey(name)) {
      return putValues.get(name).flatMap(Value::getAsString).orElse(null);
    }
    return result.map(r -> r.getText(name)).orElse(null);
  }

  @Nullable
  @Override
  public ByteBuffer getBlobAsByteBuffer(String name) {
    checkIfExists(name);
    if (putValues.containsKey(name)) {
      return putValues.get(name).flatMap(Value::getAsByteBuffer).orElse(null);
    }
    return result.map(r -> r.getBlobAsByteBuffer(name)).orElse(null);
  }

  @Nullable
  @Override
  public byte[] getBlobAsBytes(String name) {
    checkIfExists(name);
    if (putValues.containsKey(name)) {
      return putValues.get(name).flatMap(Value::getAsBytes).orElse(null);
    }
    return result.map(r -> r.getBlobAsBytes(name)).orElse(null);
  }

  @Nullable
  @Override
  public Object get(String name) {
    checkIfExists(name);
    if (isNull(name)) {
      return null;
    }

    switch (metadata.getColumnDataType(name)) {
      case BOOLEAN:
        return getBoolean(name);
      case INT:
        return getInt(name);
      case BIGINT:
        return getBigInt(name);
      case FLOAT:
        return getFloat(name);
      case DOUBLE:
        return getDouble(name);
      case TEXT:
        return getText(name);
      case BLOB:
        return getBlob(name);
      default:
        throw new AssertionError();
    }
  }

  @Override
  public boolean contains(String name) {
    return result
        .map(r -> r.getContainedColumnNames().contains(name))
        .orElse(metadata.getColumnNames().contains(name));
  }

  @Override
  public Set<String> getContainedColumnNames() {
    return result.map(TransactionResult::getContainedColumnNames).orElse(metadata.getColumnNames());
  }
}
