package com.scalar.db.api.builder;

import static com.google.common.base.Preconditions.checkNotNull;

import com.scalar.db.api.Delete;
import com.scalar.db.api.MutationCondition;
import com.scalar.db.api.builder.OperationBuilder.ClusteringKey;
import com.scalar.db.api.builder.OperationBuilder.Condition;
import com.scalar.db.api.builder.OperationBuilder.Consistency;
import com.scalar.db.api.builder.OperationBuilder.PartitionKeyBuilder;
import com.scalar.db.api.builder.OperationBuilder.TableBuilder;
import com.scalar.db.io.Key;
import javax.annotation.Nullable;

public class DeleteBuilder {

  public static class Namespace implements OperationBuilder.Namespace<Table> {
    @Override
    public Table namespace(String namespaceName) {
      checkNotNull(namespaceName);
      return new Table(namespaceName);
    }
  }

  public static class Table extends TableBuilder<PartitionKey> {

    public Table(String namespaceName) {
      super(namespaceName);
    }

    @Override
    public PartitionKey table(String tableName) {
      checkNotNull(tableName);
      return new PartitionKey(namespace, tableName);
    }
  }

  public static class PartitionKey extends PartitionKeyBuilder<Buildable> {
    public PartitionKey(String namespace, String table) {
      super(namespace, table);
    }

    @Override
    public Buildable partitionKey(Key partitionKey) {
      checkNotNull(partitionKey);
      return new Buildable(namespaceName, tableName, partitionKey);
    }
  }

  public static class Buildable extends OperationBuilder.Buildable<Delete>
      implements ClusteringKey<Buildable>, Consistency<Buildable>, Condition<Buildable> {
    @Nullable private Key clusteringKey;
    @Nullable private com.scalar.db.api.Consistency consistency;
    @Nullable private MutationCondition condition;

    public Buildable(String namespace, String table, Key partitionKey) {
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
    public Delete build() {
      Delete delete = new Delete(partitionKey, clusteringKey);
      delete.forNamespace(namespaceName).forTable(tableName);
      if (condition != null) {
        delete.withCondition(condition);
      }
      if (consistency != null) {
        delete.withConsistency(consistency);
      }

      return delete;
    }
  }

  public static class BuildableFromExisting
      implements OperationBuilder.Namespace<BuildableFromExisting>,
          OperationBuilder.Table<BuildableFromExisting>,
          OperationBuilder.PartitionKey<BuildableFromExisting>,
          ClusteringKey<BuildableFromExisting>,
          Condition<BuildableFromExisting>,
          Consistency<BuildableFromExisting> {
    @Nullable private String namespaceName;
    @Nullable private String tableName;
    private Key partitionKey;
    @Nullable private Key clusteringKey;
    @Nullable private MutationCondition condition;
    private com.scalar.db.api.Consistency consistency;

    public BuildableFromExisting(Delete delete) {
      this.namespaceName = delete.forNamespace().orElse(null);
      this.tableName = delete.forTable().orElse(null);
      this.partitionKey = delete.getPartitionKey();
      this.clusteringKey = delete.getClusteringKey().orElse(null);
      this.consistency = delete.getConsistency();
      this.condition = delete.getCondition().orElse(null);
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
    /**
     * Constructs the operation with the specified clustering {@link Key}.
     *
     * @param clusteringKey a clustering {@code Key} (it might be composed of multiple values). This
     *     can be set to {@code null} to clear the existing clustering key.
     * @return the operation builder
     */
    @Override
    public BuildableFromExisting clusteringKey(@Nullable Key clusteringKey) {
      this.clusteringKey = clusteringKey;
      return this;
    }

    @Override
    public BuildableFromExisting consistency(com.scalar.db.api.Consistency consistency) {
      checkNotNull(consistency);
      this.consistency = consistency;
      return this;
    }
    /**
     * Sets the specified {@link MutationCondition}
     *
     * @param condition a {@code MutationCondition}. This can be set to {@code null} to clear the
     *     existing condition.
     * @return the operation builder
     */
    @Override
    public BuildableFromExisting condition(@Nullable MutationCondition condition) {
      this.condition = condition;
      return this;
    }

    public Delete build() {
      Delete delete = new Delete(partitionKey, clusteringKey);
      delete.forNamespace(namespaceName);
      delete.forTable(tableName);
      delete.withConsistency(consistency);
      if (condition != null) {
        delete.withCondition(condition);
      }
      return delete;
    }
  }
}
