package com.scalar.database.storage.cassandra;

import static com.google.common.base.Preconditions.checkNotNull;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
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
import java.util.Set;
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
    return getKey(metadata.getPartitionKeyNames());
  }

  @Override
  public Optional<Key> getClusteringKey() {
    return getKey(metadata.getClusteringColumnNames());
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
              Value value = convert(row, name, type);
              values.put(name, value);
            });
  }

  @VisibleForTesting
  Map<String, DataType> getColumnDefinitions(Row row) {
    Map<String, DataType> definitions = new HashMap<>();
    row.getColumnDefinitions().forEach(c -> definitions.put(c.getName().toString(), c.getType()));
    return definitions;
  }

  private Optional<Key> getKey(Set<String> names) {
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

  private static Value convert(Row row, String name, DataType type)
      throws UnsupportedTypeException {
    int typeCode = type.getProtocolCode();
    if (typeCode == DataTypes.BOOLEAN.getProtocolCode()) {
      return new BooleanValue(name, row.getBoolean(name));
    } else if (typeCode == DataTypes.INT.getProtocolCode()) {
      return new IntValue(name, row.getInt(name));
    } else if (typeCode == DataTypes.BIGINT.getProtocolCode()) {
      return new BigIntValue(name, row.getLong(name));
    } else if (typeCode == DataTypes.FLOAT.getProtocolCode()) {
      return new FloatValue(name, row.getFloat(name));
    } else if (typeCode == DataTypes.DOUBLE.getProtocolCode()) {
      return new DoubleValue(name, row.getDouble(name));
    } else if (typeCode == DataTypes.TEXT.getProtocolCode()) {
      return new TextValue(name, row.getString(name));
    } else if (typeCode == DataTypes.BLOB.getProtocolCode()) {
      ByteBuffer buffer = row.getByteBuffer(name);
      return new BlobValue(name, buffer == null ? null : buffer.array());
    } else {
      throw new UnsupportedTypeException(type.toString());
    }
  }
}
