package com.scalar.db.util;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.Result;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Immutable
public class ResultImpl implements Result {
  private static final Logger LOGGER = LoggerFactory.getLogger(ResultImpl.class);

  private final Map<String, Value<?>> values;
  private final TableMetadata metadata;

  public ResultImpl(Map<String, Value<?>> values, TableMetadata metadata) {
    this.values = Objects.requireNonNull(values);
    this.metadata = Objects.requireNonNull(metadata);
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
      Value<?> value = values.get(name);
      if (value == null) {
        LOGGER.warn("full key doesn't seem to be projected into the result");
        return Optional.empty();
      }
      builder.add(value);
    }
    return Optional.of(builder.build());
  }

  @Override
  public Optional<Value<?>> getValue(String name) {
    return Optional.ofNullable(values.get(name));
  }

  @Override
  @Nonnull
  public Map<String, Value<?>> getValues() {
    return ImmutableMap.copyOf(values);
  }

  @Override
  public int hashCode() {
    return Objects.hash(values);
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof ResultImpl)) {
      return false;
    }
    ResultImpl other = (ResultImpl) o;
    return this.values.equals(other.values);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("values", values).toString();
  }
}
