package com.scalar.db.api;

import static com.google.common.base.Preconditions.checkNotNull;

import com.scalar.db.api.OperationBuilder.ClusteringKey;
import com.scalar.db.api.OperationBuilder.Consistency;
import com.scalar.db.api.OperationBuilder.PartitionKeyBuilder;
import com.scalar.db.api.OperationBuilder.Projection;
import com.scalar.db.api.OperationBuilder.TableBuilder;
import com.scalar.db.io.Key;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import javax.annotation.Nullable;

public class GetBuilder {

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

  public static class Buildable extends OperationBuilder.Buildable<Get>
      implements ClusteringKey<Buildable>, Consistency<Buildable>, Projection<Buildable> {
    final List<String> projections = new ArrayList<>();
    @Nullable Key clusteringKey;
    @Nullable com.scalar.db.api.Consistency consistency;

    public Buildable(String namespace, String table, Key partitionKey) {
      super(namespace, table, partitionKey);
    }

    @Override
    public Buildable projection(String projection) {
      checkNotNull(projection);
      projections.add(projection);
      return this;
    }

    @Override
    public Buildable clusteringKey(Key clusteringKey) {
      checkNotNull(clusteringKey);
      this.clusteringKey = clusteringKey;
      return this;
    }

    @Override
    public Buildable projections(Collection<String> projections) {
      checkNotNull(projections);
      this.projections.addAll(projections);
      return this;
    }

    @Override
    public Get build() {
      Get get = new Get(partitionKey, clusteringKey);
      get.forNamespace(namespaceName).forTable(tableName);
      if (!projections.isEmpty()) {
        get.withProjections(projections);
      }
      if (consistency != null) {
        get.withConsistency(consistency);
      }

      return get;
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
          OperationBuilder.ClearProjections<BuildableFromExisting>,
          OperationBuilder.ClearClusteringKey<BuildableFromExisting> {

    public BuildableFromExisting(Get get) {
      super(get.forNamespace().orElse(null), get.forTable().orElse(null), get.getPartitionKey());
      this.clusteringKey = get.getClusteringKey().orElse(null);
      this.projections.addAll(get.getProjections());
      this.consistency = get.getConsistency();
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
    public BuildableFromExisting projection(String projection) {
      super.projection(projection);
      return this;
    }

    @Override
    public BuildableFromExisting projections(Collection<String> projections) {
      super.projections(projections);
      return this;
    }

    @Override
    public BuildableFromExisting clearProjections() {
      this.projections.clear();
      return this;
    }

    @Override
    public BuildableFromExisting clearClusteringKey() {
      this.clusteringKey = null;
      return this;
    }
  }
}
