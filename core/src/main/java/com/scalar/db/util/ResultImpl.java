package com.scalar.db.util;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.BlobValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.Value;
import java.nio.ByteBuffer;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Immutable
public class ResultImpl extends AbstractResult {
  private static final Logger LOGGER = LoggerFactory.getLogger(ResultImpl.class);

  private final Map<String, Optional<Value<?>>> values;
  private final TableMetadata metadata;

  private final Supplier<Map<String, Value<?>>> valuesWithDefaultValues;

  public ResultImpl(Map<String, Optional<Value<?>>> values, TableMetadata metadata) {
    this.values = ImmutableMap.copyOf(Objects.requireNonNull(values));
    this.metadata = Objects.requireNonNull(metadata);

    // lazy loading
    valuesWithDefaultValues =
        Suppliers.memoize(
            () -> {
              ImmutableMap.Builder<String, Value<?>> builder = ImmutableMap.builder();
              values.forEach(
                  (k, v) -> {
                    if (v.isPresent()) {
                      builder.put(k, v.get());
                    } else {
                      builder.put(k, ScalarDbUtils.getDefaultValue(k, metadata));
                    }
                  });
              return builder.build();
            });
  }

  @Override
  public Optional<Key> getPartitionKey() {
    return getKey(metadata.getPartitionKeyNames());
  }

  @Override
  public Optional<Key> getClusteringKey() {
    return getKey(metadata.getClusteringKeyNames());
  }

  private Optional<Key> getKey(LinkedHashSet<String> names) {
    if (names.isEmpty()) {
      return Optional.empty();
    }
    Key.Builder builder = Key.newBuilder();
    for (String name : names) {
      Optional<Value<?>> value = values.get(name);
      if (value == null || !value.isPresent()) {
        LOGGER.warn("full key doesn't seem to be projected into the result");
        return Optional.empty();
      }
      builder.add(value.get());
    }
    return Optional.of(builder.build());
  }

  @Override
  public Optional<Value<?>> getValue(String name) {
    return Optional.ofNullable(valuesWithDefaultValues.get().get(name));
  }

  @Override
  @Nonnull
  public Map<String, Value<?>> getValues() {
    return valuesWithDefaultValues.get();
  }

  @Override
  public boolean isNull(String name) {
    checkIfExists(name);

    Optional<Value<?>> value = values.get(name);
    if (value.isPresent()) {
      if (value.get() instanceof TextValue) {
        return !value.get().getAsString().isPresent();
      } else if (value.get() instanceof BlobValue) {
        return !value.get().getAsBytes().isPresent();
      }
    }

    return !value.isPresent();
  }

  @Override
  public boolean getBoolean(String name) {
    checkIfExists(name);

    if (isNull(name)) {
      // default value
      return false;
    }
    assert values.get(name).isPresent();
    return values.get(name).get().getAsBoolean();
  }

  @Override
  public int getInt(String name) {
    checkIfExists(name);

    if (isNull(name)) {
      // default value
      return 0;
    }
    assert values.get(name).isPresent();
    return values.get(name).get().getAsInt();
  }

  @Override
  public long getBigInt(String name) {
    checkIfExists(name);

    if (isNull(name)) {
      // default value
      return 0L;
    }
    assert values.get(name).isPresent();
    return values.get(name).get().getAsLong();
  }

  @Override
  public float getFloat(String name) {
    checkIfExists(name);

    if (isNull(name)) {
      // default value
      return 0.0F;
    }
    assert values.get(name).isPresent();
    return values.get(name).get().getAsFloat();
  }

  @Override
  public double getDouble(String name) {
    checkIfExists(name);

    if (isNull(name)) {
      // default value
      return 0.0D;
    }
    assert values.get(name).isPresent();
    return values.get(name).get().getAsDouble();
  }

  @Nullable
  @Override
  public String getText(String name) {
    checkIfExists(name);

    if (isNull(name)) {
      // default value
      return null;
    }
    assert values.get(name).isPresent();
    return values.get(name).get().getAsString().orElse(null);
  }

  @Nullable
  @Override
  public ByteBuffer getBlobAsByteBuffer(String name) {
    checkIfExists(name);

    if (isNull(name)) {
      // default value
      return null;
    }
    assert values.get(name).isPresent();
    return values.get(name).get().getAsByteBuffer().orElse(null);
  }

  @Nullable
  @Override
  public byte[] getBlobAsBytes(String name) {
    checkIfExists(name);

    if (isNull(name)) {
      // default value
      return null;
    }
    assert values.get(name).isPresent();
    return values.get(name).get().getAsBytes().orElse(null);
  }

  @Nullable
  @Override
  public Object getAsObject(String name) {
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
    return values.containsKey(name);
  }

  @Override
  public Set<String> getContainedColumnNames() {
    return values.keySet();
  }
}
