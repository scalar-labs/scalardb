package com.scalar.db.api;

import static com.google.common.base.Preconditions.checkNotNull;

import com.scalar.db.api.OperationBuilder.Buildable;
import com.scalar.db.api.OperationBuilder.ClearClusteringKey;
import com.scalar.db.api.OperationBuilder.ClearNamespace;
import com.scalar.db.api.OperationBuilder.ClearProjections;
import com.scalar.db.api.OperationBuilder.ClusteringKey;
import com.scalar.db.api.OperationBuilder.Consistency;
import com.scalar.db.api.OperationBuilder.IndexKey;
import com.scalar.db.api.OperationBuilder.PartitionKey;
import com.scalar.db.api.OperationBuilder.PartitionKeyBuilder;
import com.scalar.db.api.OperationBuilder.Projection;
import com.scalar.db.api.OperationBuilder.TableBuilder;
import com.scalar.db.io.Key;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import javax.annotation.Nullable;

public class GetBuilder {

  public static class Namespace
      implements OperationBuilder.Namespace<Table>, OperationBuilder.Table<PartitionKeyOrIndexKey> {

    Namespace() {}

    @Override
    public Table namespace(String namespaceName) {
      checkNotNull(namespaceName);
      return new Table(namespaceName);
    }

    @Override
    public PartitionKeyOrIndexKey table(String tableName) {
      checkNotNull(tableName);
      return new PartitionKeyOrIndexKey(null, tableName);
    }
  }

  public static class Table extends TableBuilder<PartitionKeyOrIndexKey> {

    private Table(String namespaceName) {
      super(namespaceName);
    }

    @Override
    public PartitionKeyOrIndexKey table(String tableName) {
      checkNotNull(tableName);
      return new PartitionKeyOrIndexKey(namespace, tableName);
    }
  }

  public static class PartitionKeyOrIndexKey extends PartitionKeyBuilder<BuildableGet>
      implements IndexKey<BuildableGetWithIndex> {

    private PartitionKeyOrIndexKey(@Nullable String namespace, String table) {
      super(namespace, table);
    }

    @Override
    public BuildableGet partitionKey(Key partitionKey) {
      checkNotNull(partitionKey);
      return new BuildableGet(namespaceName, tableName, partitionKey);
    }

    @Override
    public BuildableGetWithIndex indexKey(Key indexKey) {
      return new BuildableGetWithIndex(namespaceName, tableName, indexKey);
    }
  }

  public static class BuildableGet extends Buildable<Get>
      implements ClusteringKey<BuildableGet>, Consistency<BuildableGet>, Projection<BuildableGet> {
    final List<String> projections = new ArrayList<>();
    @Nullable Key clusteringKey;
    @Nullable com.scalar.db.api.Consistency consistency;

    private BuildableGet(@Nullable String namespace, String table, Key partitionKey) {
      super(namespace, table, partitionKey);
    }

    @Override
    public BuildableGet clusteringKey(Key clusteringKey) {
      checkNotNull(clusteringKey);
      this.clusteringKey = clusteringKey;
      return this;
    }

    @Override
    public BuildableGet projection(String projection) {
      checkNotNull(projection);
      projections.add(projection);
      return this;
    }

    @Override
    public BuildableGet projections(Collection<String> projections) {
      checkNotNull(projections);
      this.projections.addAll(projections);
      return this;
    }

    @Override
    public BuildableGet projections(String... projections) {
      return projections(Arrays.asList(projections));
    }

    @Override
    public BuildableGet consistency(com.scalar.db.api.Consistency consistency) {
      checkNotNull(consistency);
      this.consistency = consistency;
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
  }

  public static class BuildableGetWithIndex
      implements Consistency<BuildableGetWithIndex>, Projection<BuildableGetWithIndex> {
    @Nullable private final String namespaceName;
    private final String tableName;
    private final Key indexKey;
    private final List<String> projections = new ArrayList<>();
    @Nullable private com.scalar.db.api.Consistency consistency;

    private BuildableGetWithIndex(@Nullable String namespace, String table, Key indexKey) {
      namespaceName = namespace;
      tableName = table;
      this.indexKey = indexKey;
    }

    @Override
    public BuildableGetWithIndex projection(String projection) {
      checkNotNull(projection);
      projections.add(projection);
      return this;
    }

    @Override
    public BuildableGetWithIndex projections(Collection<String> projections) {
      checkNotNull(projections);
      this.projections.addAll(projections);
      return this;
    }

    @Override
    public BuildableGetWithIndex projections(String... projections) {
      return projections(Arrays.asList(projections));
    }

    @Override
    public BuildableGetWithIndex consistency(com.scalar.db.api.Consistency consistency) {
      checkNotNull(consistency);
      this.consistency = consistency;
      return this;
    }

    public Get build() {
      GetWithIndex getWithIndex = new GetWithIndex(indexKey);
      getWithIndex.forNamespace(namespaceName).forTable(tableName);
      if (!projections.isEmpty()) {
        getWithIndex.withProjections(projections);
      }
      if (consistency != null) {
        getWithIndex.withConsistency(consistency);
      }
      return getWithIndex;
    }
  }

  public static class BuildableGetOrGetWithIndexFromExisting extends BuildableGet
      implements OperationBuilder.Namespace<BuildableGetOrGetWithIndexFromExisting>,
          OperationBuilder.Table<BuildableGetOrGetWithIndexFromExisting>,
          PartitionKey<BuildableGetOrGetWithIndexFromExisting>,
          IndexKey<BuildableGetOrGetWithIndexFromExisting>,
          ClearProjections<BuildableGetOrGetWithIndexFromExisting>,
          ClearClusteringKey<BuildableGetOrGetWithIndexFromExisting>,
          ClearNamespace<BuildableGetOrGetWithIndexFromExisting> {

    private Key indexKey;
    private final boolean isGetWithIndex;

    BuildableGetOrGetWithIndexFromExisting(Get get) {
      super(get.forNamespace().orElse(null), get.forTable().orElse(null), get.getPartitionKey());
      clusteringKey = get.getClusteringKey().orElse(null);
      projections.addAll(get.getProjections());
      consistency = get.getConsistency();
      isGetWithIndex = get instanceof GetWithIndex;
      if (isGetWithIndex) {
        indexKey = get.getPartitionKey();
      }
    }

    @Override
    public BuildableGetOrGetWithIndexFromExisting namespace(String namespaceName) {
      checkNotNull(namespaceName);
      this.namespaceName = namespaceName;
      return this;
    }

    @Override
    public BuildableGetOrGetWithIndexFromExisting table(String tableName) {
      checkNotNull(tableName);
      this.tableName = tableName;
      return this;
    }

    @Override
    public BuildableGetOrGetWithIndexFromExisting partitionKey(Key partitionKey) {
      checkNotGetWithIndex();
      checkNotNull(partitionKey);
      this.partitionKey = partitionKey;
      return this;
    }

    @Override
    public BuildableGetOrGetWithIndexFromExisting indexKey(Key indexKey) {
      checkNotGet();
      checkNotNull(indexKey);
      this.indexKey = indexKey;
      return this;
    }

    @Override
    public BuildableGetOrGetWithIndexFromExisting clusteringKey(Key clusteringKey) {
      checkNotGetWithIndex();
      super.clusteringKey(clusteringKey);
      return this;
    }

    @Override
    public BuildableGetOrGetWithIndexFromExisting consistency(
        com.scalar.db.api.Consistency consistency) {
      super.consistency(consistency);
      return this;
    }

    @Override
    public BuildableGetOrGetWithIndexFromExisting projection(String projection) {
      super.projection(projection);
      return this;
    }

    @Override
    public BuildableGetOrGetWithIndexFromExisting projections(Collection<String> projections) {
      super.projections(projections);
      return this;
    }

    @Override
    public BuildableGetOrGetWithIndexFromExisting projections(String... projections) {
      super.projections(projections);
      return this;
    }

    @Override
    public BuildableGetOrGetWithIndexFromExisting clearProjections() {
      this.projections.clear();
      return this;
    }

    @Override
    public BuildableGetOrGetWithIndexFromExisting clearClusteringKey() {
      checkNotGetWithIndex();
      this.clusteringKey = null;
      return this;
    }

    @Override
    public BuildableGetOrGetWithIndexFromExisting clearNamespace() {
      this.namespaceName = null;
      return this;
    }

    private void checkNotGet() {
      if (!isGetWithIndex) {
        throw new UnsupportedOperationException(
            "This operation is not supported when getting records of a database without using a secondary index");
      }
    }

    private void checkNotGetWithIndex() {
      if (isGetWithIndex) {
        throw new UnsupportedOperationException(
            "This operation is not supported when getting records of a database using a secondary index");
      }
    }

    @Override
    public Get build() {
      Get get;

      if (isGetWithIndex) {
        get = new GetWithIndex(indexKey);
      } else {
        get = new Get(partitionKey, clusteringKey);
      }

      get.forNamespace(namespaceName).forTable(tableName);
      if (!projections.isEmpty()) {
        get.withProjections(projections);
      }
      if (consistency != null) {
        get.withConsistency(consistency);
      }

      return get;
    }
  }
}
