package com.scalar.db.api;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.OperationBuilder.Attribute;
import com.scalar.db.api.OperationBuilder.ClearAttribute;
import com.scalar.db.api.OperationBuilder.ClearClusteringKey;
import com.scalar.db.api.OperationBuilder.ClearNamespace;
import com.scalar.db.api.OperationBuilder.ClearValues;
import com.scalar.db.api.OperationBuilder.ClusteringKey;
import com.scalar.db.api.OperationBuilder.PartitionKeyBuilder;
import com.scalar.db.api.OperationBuilder.TableBuilder;
import com.scalar.db.api.OperationBuilder.Values;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.BlobColumn;
import com.scalar.db.io.BooleanColumn;
import com.scalar.db.io.Column;
import com.scalar.db.io.DateColumn;
import com.scalar.db.io.DoubleColumn;
import com.scalar.db.io.FloatColumn;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import com.scalar.db.io.TimeColumn;
import com.scalar.db.io.TimestampColumn;
import com.scalar.db.io.TimestampTZColumn;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.annotation.Nullable;

public class InsertBuilder {

  public static class Namespace
      implements OperationBuilder.Namespace<Table>, OperationBuilder.Table<PartitionKey> {

    Namespace() {}

    @Override
    public Table namespace(String namespaceName) {
      checkNotNull(namespaceName);
      return new Table(namespaceName);
    }

    @Override
    public PartitionKey table(String tableName) {
      checkNotNull(tableName);
      return new PartitionKey(null, tableName);
    }
  }

  public static class Table extends TableBuilder<PartitionKey> {

    private Table(String namespaceName) {
      super(namespaceName);
    }

    @Override
    public PartitionKey table(String tableName) {
      checkNotNull(tableName);
      return new PartitionKey(namespace, tableName);
    }
  }

  public static class PartitionKey extends PartitionKeyBuilder<Buildable> {

    private PartitionKey(@Nullable String namespaceName, String tableName) {
      super(namespaceName, tableName);
    }

    @Override
    public Buildable partitionKey(Key partitionKey) {
      checkNotNull(partitionKey);
      return new Buildable(namespaceName, tableName, partitionKey);
    }
  }

  public static class Buildable extends OperationBuilder.Buildable<Insert>
      implements ClusteringKey<Buildable>, Values<Buildable>, Attribute<Buildable> {
    final Map<String, Column<?>> columns = new LinkedHashMap<>();
    @Nullable Key clusteringKey;
    final Map<String, String> attributes = new HashMap<>();

    private Buildable(@Nullable String namespace, String table, Key partitionKey) {
      super(namespace, table, partitionKey);
    }

    @Override
    public Buildable clusteringKey(Key clusteringKey) {
      checkNotNull(clusteringKey);
      this.clusteringKey = clusteringKey;
      return this;
    }

    @Override
    public Buildable attribute(String name, String value) {
      checkNotNull(name);
      checkNotNull(value);
      attributes.put(name, value);
      return this;
    }

    @Override
    public Buildable attributes(Map<String, String> attributes) {
      checkNotNull(attributes);
      this.attributes.putAll(attributes);
      return this;
    }

    @Override
    public Buildable booleanValue(String columnName, boolean value) {
      columns.put(columnName, BooleanColumn.of(columnName, value));
      return this;
    }

    @Override
    public Buildable booleanValue(String columnName, @Nullable Boolean value) {
      if (value != null) {
        return booleanValue(columnName, value.booleanValue());
      }
      columns.put(columnName, BooleanColumn.ofNull(columnName));
      return this;
    }

    @Override
    public Buildable intValue(String columnName, int value) {
      columns.put(columnName, IntColumn.of(columnName, value));
      return this;
    }

    @Override
    public Buildable intValue(String columnName, @Nullable Integer value) {
      if (value != null) {
        return intValue(columnName, value.intValue());
      }
      columns.put(columnName, IntColumn.ofNull(columnName));
      return this;
    }

    @Override
    public Buildable bigIntValue(String columnName, long value) {
      columns.put(columnName, BigIntColumn.of(columnName, value));
      return this;
    }

    @Override
    public Buildable bigIntValue(String columnName, @Nullable Long value) {
      if (value != null) {
        return bigIntValue(columnName, value.longValue());
      }
      columns.put(columnName, BigIntColumn.ofNull(columnName));
      return this;
    }

    @Override
    public Buildable floatValue(String columnName, float value) {
      columns.put(columnName, FloatColumn.of(columnName, value));
      return this;
    }

    @Override
    public Buildable floatValue(String columnName, @Nullable Float value) {
      if (value != null) {
        return floatValue(columnName, value.floatValue());
      }
      columns.put(columnName, FloatColumn.ofNull(columnName));
      return this;
    }

    @Override
    public Buildable doubleValue(String columnName, double value) {
      columns.put(columnName, DoubleColumn.of(columnName, value));
      return this;
    }

    @Override
    public Buildable doubleValue(String columnName, @Nullable Double value) {
      if (value != null) {
        return doubleValue(columnName, value.doubleValue());
      }
      columns.put(columnName, DoubleColumn.ofNull(columnName));
      return this;
    }

    @Override
    public Buildable textValue(String columnName, @Nullable String value) {
      columns.put(columnName, TextColumn.of(columnName, value));
      return this;
    }

    @Override
    public Buildable blobValue(String columnName, @Nullable byte[] value) {
      columns.put(columnName, BlobColumn.of(columnName, value));
      return this;
    }

    @Override
    public Buildable blobValue(String columnName, @Nullable ByteBuffer value) {
      columns.put(columnName, BlobColumn.of(columnName, value));
      return this;
    }

    @Override
    public Buildable dateValue(String columnName, @Nullable LocalDate value) {
      columns.put(columnName, DateColumn.of(columnName, value));
      return this;
    }

    @Override
    public Buildable timeValue(String columnName, @Nullable LocalTime value) {
      columns.put(columnName, TimeColumn.of(columnName, value));
      return this;
    }

    @Override
    public Buildable timestampValue(String columnName, @Nullable LocalDateTime value) {
      columns.put(columnName, TimestampColumn.of(columnName, value));
      return this;
    }

    @Override
    public Buildable timestampTZValue(String columnName, @Nullable Instant value) {
      columns.put(columnName, TimestampTZColumn.of(columnName, value));
      return this;
    }

    @Override
    public Buildable value(Column<?> column) {
      columns.put(column.getName(), column);
      return this;
    }

    @Override
    public Insert build() {
      return new Insert(
          namespaceName,
          tableName,
          partitionKey,
          clusteringKey,
          ImmutableMap.copyOf(attributes),
          ImmutableMap.copyOf(columns));
    }
  }

  public static class BuildableFromExisting extends Buildable
      implements OperationBuilder.Namespace<BuildableFromExisting>,
          OperationBuilder.Table<BuildableFromExisting>,
          OperationBuilder.PartitionKey<BuildableFromExisting>,
          ClearClusteringKey<BuildableFromExisting>,
          ClearValues<BuildableFromExisting>,
          ClearNamespace<BuildableFromExisting>,
          ClearAttribute<BuildableFromExisting> {

    BuildableFromExisting(Insert insert) {
      super(
          insert.forNamespace().orElse(null),
          insert.forTable().orElse(null),
          insert.getPartitionKey());
      this.clusteringKey = insert.getClusteringKey().orElse(null);
      this.columns.putAll(insert.getColumns());
      this.attributes.putAll(insert.getAttributes());
    }

    @Override
    public BuildableFromExisting namespace(String namespaceName) {
      checkNotNull(namespaceName);
      this.namespaceName = namespaceName;
      return this;
    }

    @Override
    public BuildableFromExisting table(String tableName) {
      checkNotNull(tableName);
      this.tableName = tableName;
      return this;
    }

    @Override
    public BuildableFromExisting partitionKey(Key partitionKey) {
      checkNotNull(partitionKey);
      this.partitionKey = partitionKey;
      return this;
    }

    @Override
    public BuildableFromExisting clusteringKey(Key clusteringKey) {
      super.clusteringKey(clusteringKey);
      return this;
    }

    @Override
    public BuildableFromExisting attribute(String name, String value) {
      super.attribute(name, value);
      return this;
    }

    @Override
    public BuildableFromExisting attributes(Map<String, String> attributes) {
      super.attributes(attributes);
      return this;
    }

    @Override
    public BuildableFromExisting booleanValue(String columnName, boolean value) {
      super.booleanValue(columnName, value);
      return this;
    }

    @Override
    public BuildableFromExisting booleanValue(String columnName, @Nullable Boolean value) {
      super.booleanValue(columnName, value);
      return this;
    }

    @Override
    public BuildableFromExisting intValue(String columnName, int value) {
      super.intValue(columnName, value);
      return this;
    }

    @Override
    public BuildableFromExisting intValue(String columnName, @Nullable Integer value) {
      super.intValue(columnName, value);
      return this;
    }

    @Override
    public BuildableFromExisting bigIntValue(String columnName, long value) {
      super.bigIntValue(columnName, value);
      return this;
    }

    @Override
    public BuildableFromExisting bigIntValue(String columnName, @Nullable Long value) {
      super.bigIntValue(columnName, value);
      return this;
    }

    @Override
    public BuildableFromExisting floatValue(String columnName, float value) {
      super.floatValue(columnName, value);
      return this;
    }

    @Override
    public BuildableFromExisting floatValue(String columnName, @Nullable Float value) {
      super.floatValue(columnName, value);
      return this;
    }

    @Override
    public BuildableFromExisting doubleValue(String columnName, double value) {
      super.doubleValue(columnName, value);
      return this;
    }

    @Override
    public BuildableFromExisting doubleValue(String columnName, @Nullable Double value) {
      super.doubleValue(columnName, value);
      return this;
    }

    @Override
    public BuildableFromExisting textValue(String columnName, @Nullable String value) {
      super.textValue(columnName, value);
      return this;
    }

    @Override
    public BuildableFromExisting blobValue(String columnName, @Nullable byte[] value) {
      super.blobValue(columnName, value);
      return this;
    }

    @Override
    public BuildableFromExisting blobValue(String columnName, @Nullable ByteBuffer value) {
      super.blobValue(columnName, value);
      return this;
    }

    @Override
    public BuildableFromExisting dateValue(String columnName, @Nullable LocalDate value) {
      super.dateValue(columnName, value);
      return this;
    }

    @Override
    public BuildableFromExisting timeValue(String columnName, @Nullable LocalTime value) {
      super.timeValue(columnName, value);
      return this;
    }

    @Override
    public BuildableFromExisting timestampValue(String columnName, @Nullable LocalDateTime value) {
      super.timestampValue(columnName, value);
      return this;
    }

    @Override
    public BuildableFromExisting timestampTZValue(String columnName, @Nullable Instant value) {
      super.timestampTZValue(columnName, value);
      return this;
    }

    @Override
    public BuildableFromExisting value(Column<?> column) {
      super.value(column);
      return this;
    }

    @Override
    public BuildableFromExisting clearValues() {
      columns.clear();
      return this;
    }

    @Override
    public BuildableFromExisting clearValue(String columnName) {
      columns.remove(columnName);
      return this;
    }

    @Override
    public BuildableFromExisting clearClusteringKey() {
      this.clusteringKey = null;
      return this;
    }

    @Override
    public BuildableFromExisting clearNamespace() {
      this.namespaceName = null;
      return this;
    }

    @Override
    public BuildableFromExisting clearAttributes() {
      attributes.clear();
      return this;
    }

    @Override
    public BuildableFromExisting clearAttribute(String name) {
      attributes.remove(name);
      return this;
    }
  }
}
