package com.scalar.db.storage.jdbc;

import com.google.common.base.MoreObjects;
import com.scalar.db.api.Result;
import com.scalar.db.io.BigIntValue;
import com.scalar.db.io.BlobValue;
import com.scalar.db.io.BooleanValue;
import com.scalar.db.io.DoubleValue;
import com.scalar.db.io.FloatValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.Value;
import com.scalar.db.storage.jdbc.metadata.DataType;
import com.scalar.db.storage.jdbc.metadata.TableMetadata;

import javax.annotation.concurrent.Immutable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

@Immutable
public class ResultImpl implements Result {

  private final TableMetadata tableMetadata;
  private final Map<String, Value> values;

  public ResultImpl(TableMetadata tableMetadata, List<String> projections, ResultSet resultSet)
      throws SQLException {
    this.tableMetadata = tableMetadata;

    values = new HashMap<>();
    if (projections.isEmpty()) {
      for (String projection : tableMetadata.getColumns()) {
        values.put(projection, getValue(tableMetadata, projection, resultSet));
      }
    } else {
      for (String projection : projections) {
        values.put(projection, getValue(tableMetadata, projection, resultSet));
      }
    }
  }

  private Value getValue(TableMetadata tableMetadata, String name, ResultSet resultSet)
      throws SQLException {
    DataType dataType = tableMetadata.getDataType(name);
    switch (dataType) {
      case BOOLEAN:
        return new BooleanValue(name, resultSet.getBoolean(name));

      case INT:
        return new IntValue(name, resultSet.getInt(name));

      case BIGINT:
        return new BigIntValue(name, resultSet.getLong(name));

      case FLOAT:
        return new FloatValue(name, resultSet.getFloat(name));

      case DOUBLE:
        return new DoubleValue(name, resultSet.getDouble(name));

      case TEXT:
        return new TextValue(name, resultSet.getString(name));

      case BLOB:
        return new BlobValue(name, resultSet.getBytes(name));

      default:
        throw new AssertionError();
    }
  }

  @Override
  public Optional<Key> getPartitionKey() {
    return getKey(tableMetadata.getPartitionKeys());
  }

  @Override
  public Optional<Key> getClusteringKey() {
    return getKey(tableMetadata.getClusteringKeys());
  }

  private Optional<Key> getKey(List<String> names) {
    List<Value> list = new ArrayList<>();
    for (String name : names) {
      Value value = values.get(name);
      if (value == null) {
        return Optional.empty();
      }
      list.add(value);
    }
    return Optional.of(new Key(list));
  }

  @Override
  public Optional<Value> getValue(String name) {
    return Optional.ofNullable(values.get(name));
  }

  @Override
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
    return this.values.equals(other.values);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("values", values).toString();
  }
}
