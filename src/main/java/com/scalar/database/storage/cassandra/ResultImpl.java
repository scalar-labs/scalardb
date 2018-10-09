package com.scalar.database.storage.cassandra;

import static com.google.common.base.Preconditions.checkNotNull;

import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TableMetadata;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.scalar.database.api.Result;
import com.scalar.database.exception.storage.UnsupportedTypeException;
import com.scalar.database.io.BigIntValue;
import com.scalar.database.io.BlobValue;
import com.scalar.database.io.BooleanValue;
import com.scalar.database.io.DoubleValue;
import com.scalar.database.io.FloatValue;
import com.scalar.database.io.IntValue;
import com.scalar.database.io.Key;
import com.scalar.database.io.TextValue;
import com.scalar.database.io.Value;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Immutable
public class ResultImpl implements Result {
  private static final Logger LOGGER = LoggerFactory.getLogger(ResultImpl.class);
  private final TableMetadata metadata;
  private Map<String, Value> values;

  public ResultImpl(Row row, TableMetadata metadata) {
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
  ResultImpl(Collection<Value> values, TableMetadata metadata) {
    this.metadata = metadata;
    this.values = new HashMap<>();
    values.forEach(v -> this.values.put(v.getName(), v));
  }

  @Override
  public Optional<Key> getPartitionKey() {
    return getKey(metadata.getPartitionKey());
  }

  @Override
  public Optional<Key> getClusteringKey() {
    return getKey(metadata.getClusteringColumns());
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
    return row.getColumnDefinitions()
        .asList()
        .stream()
        .collect(
            Collectors.toMap(
                Definition::getName,
                Definition::getType,
                (d1, d2) -> {
                  return d1;
                }));
  }

  private Optional<Key> getKey(List<ColumnMetadata> metadata) {
    List<Value> list = new ArrayList<>();
    for (ColumnMetadata m : metadata) {
      Value value = values.get(m.getName());
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
