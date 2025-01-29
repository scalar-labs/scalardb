package com.scalar.db.api;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.OperationBuilder.AbacReadTagAttribute;
import com.scalar.db.api.OperationBuilder.AbacWriteTagAttribute;
import com.scalar.db.api.OperationBuilder.Attribute;
import com.scalar.db.api.OperationBuilder.ClearAbacReadTagAttribute;
import com.scalar.db.api.OperationBuilder.ClearAbacWriteTagAttribute;
import com.scalar.db.api.OperationBuilder.ClearAttribute;
import com.scalar.db.api.OperationBuilder.ClearClusteringKey;
import com.scalar.db.api.OperationBuilder.ClearCondition;
import com.scalar.db.api.OperationBuilder.ClearNamespace;
import com.scalar.db.api.OperationBuilder.ClusteringKey;
import com.scalar.db.api.OperationBuilder.Condition;
import com.scalar.db.api.OperationBuilder.Consistency;
import com.scalar.db.api.OperationBuilder.PartitionKeyBuilder;
import com.scalar.db.api.OperationBuilder.TableBuilder;
import com.scalar.db.io.Key;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

public class DeleteBuilder {

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

    private PartitionKey(@Nullable String namespace, String table) {
      super(namespace, table);
    }

    @Override
    public Buildable partitionKey(Key partitionKey) {
      checkNotNull(partitionKey);
      return new Buildable(namespaceName, tableName, partitionKey);
    }
  }

  public static class Buildable extends OperationBuilder.Buildable<Delete>
      implements ClusteringKey<Buildable>,
          Consistency<Buildable>,
          Condition<Buildable>,
          Attribute<Buildable>,
          AbacReadTagAttribute<Buildable>,
          AbacWriteTagAttribute<Buildable> {
    @Nullable Key clusteringKey;
    @Nullable com.scalar.db.api.Consistency consistency;
    @Nullable MutationCondition condition;
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
    public Buildable condition(MutationCondition condition) {
      checkNotNull(condition);
      this.condition = condition;
      return this;
    }

    @Override
    public Buildable consistency(com.scalar.db.api.Consistency consistency) {
      checkNotNull(consistency);
      this.consistency = consistency;
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
    public Buildable readTag(String policyName, String readTag) {
      checkNotNull(policyName);
      checkNotNull(readTag);
      AbacOperationAttributes.setReadTag(attributes, policyName, readTag);
      return this;
    }

    @Override
    public Buildable writeTag(String policyName, String writeTag) {
      checkNotNull(policyName);
      checkNotNull(writeTag);
      AbacOperationAttributes.setWriteTag(attributes, policyName, writeTag);
      return this;
    }

    @Override
    public Delete build() {
      return new Delete(
          namespaceName,
          tableName,
          partitionKey,
          clusteringKey,
          consistency,
          ImmutableMap.copyOf(attributes),
          condition);
    }
  }

  public static class BuildableFromExisting extends Buildable
      implements OperationBuilder.Namespace<BuildableFromExisting>,
          OperationBuilder.Table<BuildableFromExisting>,
          OperationBuilder.PartitionKey<BuildableFromExisting>,
          ClearCondition<BuildableFromExisting>,
          ClearClusteringKey<BuildableFromExisting>,
          ClearNamespace<BuildableFromExisting>,
          ClearAttribute<BuildableFromExisting>,
          ClearAbacReadTagAttribute<BuildableFromExisting>,
          ClearAbacWriteTagAttribute<BuildableFromExisting> {

    BuildableFromExisting(Delete delete) {
      super(
          delete.forNamespace().orElse(null),
          delete.forTable().orElse(null),
          delete.getPartitionKey());
      this.clusteringKey = delete.getClusteringKey().orElse(null);
      this.consistency = delete.getConsistency();
      this.condition = delete.getCondition().orElse(null);
      this.attributes.putAll(delete.getAttributes());
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
    public Buildable readTag(String policyName, String readTag) {
      super.readTag(policyName, readTag);
      return this;
    }

    @Override
    public Buildable writeTag(String policyName, String writeTag) {
      super.writeTag(policyName, writeTag);
      return this;
    }

    @Override
    public BuildableFromExisting condition(MutationCondition condition) {
      super.condition(condition);
      return this;
    }

    @Override
    public BuildableFromExisting clearCondition() {
      this.condition = null;
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
      this.attributes.clear();
      return this;
    }

    @Override
    public BuildableFromExisting clearAttribute(String name) {
      this.attributes.remove(name);
      return this;
    }

    @Override
    public BuildableFromExisting clearReadTag(String policyName) {
      AbacOperationAttributes.clearReadTag(attributes, policyName);
      return this;
    }

    @Override
    public BuildableFromExisting clearReadTags() {
      AbacOperationAttributes.clearReadTags(attributes);
      return this;
    }

    @Override
    public BuildableFromExisting clearWriteTag(String policyName) {
      AbacOperationAttributes.clearWriteTag(attributes, policyName);
      return this;
    }

    @Override
    public BuildableFromExisting clearWriteTags() {
      AbacOperationAttributes.clearWriteTags(attributes);
      return this;
    }
  }
}
