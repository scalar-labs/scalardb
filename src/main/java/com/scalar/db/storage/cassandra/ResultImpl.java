package com.scalar.db.storage.cassandra;

import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.scalar.db.api.Result;
import com.scalar.db.exception.storage.UnsupportedTypeException;
import com.scalar.db.io.BigIntValue;
import com.scalar.db.io.BlobValue;
import com.scalar.db.io.BooleanValue;
import com.scalar.db.io.DoubleValue;
import com.scalar.db.io.FloatValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

@Immutable
public class ResultImpl implements Result {
  private static final Logger LOGGER = LoggerFactory.getLogger(ResultImpl.class);
  private final CassandraTableMetadata metadata;
  private Map<String, Value> values;

  public ResultImpl(Row row, CassandraTableMetadata metadata) {
    checkNotNull(row);
    this.metadata = checkNotNull(metadata);
    interpret(row);
  }

  /**
   * constructor only for testing purposes
   *
   * @param values
   */
  @VisibleForTesting
  ResultImpl(Collection<Value> values, CassandraTableMetadata metadata) {
    this.metadata = metadata;
    this.values = new HashMap<>();
    values.forEach(v -> this.values.put(v.getName(), v));
  }

  @Override
  public Optional<Key> getPartitionKey() {
    return getKey(metadata.getPartitionKeyNames());
  }

  @Override
  public Optional<Key> getClusteringKey() {
    return getKey(metadata.getClusteringKeyNames());
  }

  @Override
  public Optional<Value> getValue(String name) {
    return Optional.ofNullable(values.get(name));
  }

  @Override
  @Nonnull
  public Map<String, Value> getValues() {
    return Collections.unmodifiableMap(values);
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
    if (this.values.equals(other.values)) {
      return true;
    }
    return false;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("values", values).toString();
  }

  @VisibleForTesting
  void interpret(Row row) {
    values = new HashMap<>();
    getColumnDefinitions(row)
        .forEach(
            (name, type) -> {
              Value value = convert(row, name, type.getName());
              values.put(name, value);
            });
  }

  @VisibleForTesting
  Map<String, DataType> getColumnDefinitions(Row row) {
    return row.getColumnDefinitions().asList().stream()
        .collect(
            Collectors.toMap(
                Definition::getName,
                Definition::getType,
                (d1, d2) -> {
                  return d1;
                }));
  }

  private Optional<Key> getKey(LinkedHashSet<String> names) {
    List<Value> list = new ArrayList<>();
    for (String name : names) {
      Value value = values.get(name);
      if (value == null) {
        LOGGER.warn("full key doesn't seem to be projected into the result");
        return Optional.empty();
      }
      list.add(value);
    }
    return Optional.of(new Key(list));
  }

  private static Value convert(Row row, String name, DataType.Name type)
      throws UnsupportedTypeException {
    switch (type) {
      case BOOLEAN:
        return new BooleanValue(name, row.getBool(name));
      case INT:
        return new IntValue(name, row.getInt(name));
      case BIGINT:
        return new BigIntValue(name, row.getLong(name));
      case FLOAT:
        return new FloatValue(name, row.getFloat(name));
      case DOUBLE:
        return new DoubleValue(name, row.getDouble(name));
      case TEXT: // for backwards compatibility
      case VARCHAR:
        return new TextValue(name, row.getString(name));
      case BLOB:
        ByteBuffer buffer = row.getBytes(name);
        return new BlobValue(name, buffer == null ? null : buffer.array());
      default:
        throw new UnsupportedTypeException(type.toString());
    }
  }
}
