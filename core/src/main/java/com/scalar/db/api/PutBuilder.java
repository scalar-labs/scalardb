package com.scalar.db.api;

import static com.google.common.base.Preconditions.checkNotNull;

import com.scalar.db.api.OperationBuilder.ClearClusteringKey;
import com.scalar.db.api.OperationBuilder.ClearCondition;
import com.scalar.db.api.OperationBuilder.ClearNamespace;
import com.scalar.db.api.OperationBuilder.ClearValues;
import com.scalar.db.api.OperationBuilder.ClusteringKey;
import com.scalar.db.api.OperationBuilder.Condition;
import com.scalar.db.api.OperationBuilder.Consistency;
import com.scalar.db.api.OperationBuilder.ImplicitPreReadEnabled;
import com.scalar.db.api.OperationBuilder.InsertModeEnabled;
import com.scalar.db.api.OperationBuilder.PartitionKeyBuilder;
import com.scalar.db.api.OperationBuilder.TableBuilder;
import com.scalar.db.api.OperationBuilder.Values;
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
import java.util.LinkedHashMap;
import java.util.Map;
import javax.annotation.Nullable;

public class PutBuilder {

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

  public static class Buildable extends OperationBuilder.Buildable<Put>
      implements ClusteringKey<Buildable>,
          Consistency<Buildable>,
          Condition<Buildable>,
          Values<Buildable>,
          ImplicitPreReadEnabled<Buildable>,
          InsertModeEnabled<Buildable> {
    final Map<String, Column<?>> columns = new LinkedHashMap<>();
    @Nullable Key clusteringKey;
    @Nullable com.scalar.db.api.Consistency consistency;
    @Nullable MutationCondition condition;
    boolean implicitPreReadEnabled;
    boolean insertModeEnabled;

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
    public Buildable condition(MutationCondition condition) {
      checkNotNull(condition);
      this.condition = condition;
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
    public Buildable value(Column<?> column) {
      columns.put(column.getName(), column);
      return this;
    }

    @Override
    public Buildable disableImplicitPreRead() {
      implicitPreReadEnabled = false;
      return this;
    }

    @Override
    public Buildable enableImplicitPreRead() {
      implicitPreReadEnabled = true;
      return this;
    }

    @Override
    public Buildable implicitPreReadEnabled(boolean implicitPreReadEnabled) {
      this.implicitPreReadEnabled = implicitPreReadEnabled;
      return this;
    }

    @Override
    public Buildable disableInsertMode() {
      insertModeEnabled = false;
      return this;
    }

    @Override
    public Buildable enableInsertMode() {
      insertModeEnabled = true;
      return this;
    }

    @Override
    public Buildable insertModeEnabled(boolean insertModeEnabled) {
      this.insertModeEnabled = insertModeEnabled;
      return this;
    }

    @Override
    public Put build() {
      return new Put(
          namespaceName,
          tableName,
          partitionKey,
          clusteringKey,
          consistency,
          columns,
          condition,
          implicitPreReadEnabled,
          insertModeEnabled);
    }

    @Override
    public Buildable consistency(com.scalar.db.api.Consistency consistency) {
      checkNotNull(consistency);
      this.consistency = consistency;
      return this;
    }
  }

  public static class BuildableFromExisting extends Buildable
      implements OperationBuilder.Namespace<BuildableFromExisting>,
          OperationBuilder.Table<BuildableFromExisting>,
          OperationBuilder.PartitionKey<BuildableFromExisting>,
          ClearClusteringKey<BuildableFromExisting>,
          ClearValues<BuildableFromExisting>,
          ClearCondition<BuildableFromExisting>,
          ClearNamespace<BuildableFromExisting> {

    BuildableFromExisting(Put put) {
      super(put.forNamespace().orElse(null), put.forTable().orElse(null), put.getPartitionKey());
      this.clusteringKey = put.getClusteringKey().orElse(null);
      this.columns.putAll(put.getColumns());
      this.consistency = put.getConsistency();
      this.condition = put.getCondition().orElse(null);
      this.implicitPreReadEnabled = put.isImplicitPreReadEnabled();
      this.insertModeEnabled = put.isInsertModeEnabled();
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
    public BuildableFromExisting consistency(com.scalar.db.api.Consistency consistency) {
      super.consistency(consistency);
      return this;
    }

    @Override
    public BuildableFromExisting condition(MutationCondition condition) {
      super.condition(condition);
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
    public BuildableFromExisting clearCondition() {
      this.condition = null;
      return this;
    }

    @Override
    public BuildableFromExisting clearNamespace() {
      this.namespaceName = null;
      return this;
    }

    @Override
    public BuildableFromExisting disableImplicitPreRead() {
      super.disableImplicitPreRead();
      return this;
    }

    @Override
    public BuildableFromExisting enableImplicitPreRead() {
      super.enableImplicitPreRead();
      return this;
    }

    @Override
    public BuildableFromExisting implicitPreReadEnabled(boolean implicitPreReadEnabled) {
      super.implicitPreReadEnabled(implicitPreReadEnabled);
      return this;
    }

    @Override
    public BuildableFromExisting disableInsertMode() {
      super.disableInsertMode();
      return this;
    }

    @Override
    public BuildableFromExisting enableInsertMode() {
      super.enableInsertMode();
      return this;
    }

    @Override
    public BuildableFromExisting insertModeEnabled(boolean insertModeEnabled) {
      super.insertModeEnabled(insertModeEnabled);
      return this;
    }
  }
}
