package com.scalar.db.api;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.ImmutableSet;
import com.scalar.db.api.OperationBuilder.And;
import com.scalar.db.api.OperationBuilder.Buildable;
import com.scalar.db.api.OperationBuilder.ClearClusteringKey;
import com.scalar.db.api.OperationBuilder.ClearConditions;
import com.scalar.db.api.OperationBuilder.ClearNamespace;
import com.scalar.db.api.OperationBuilder.ClearProjections;
import com.scalar.db.api.OperationBuilder.ClusteringKey;
import com.scalar.db.api.OperationBuilder.Consistency;
import com.scalar.db.api.OperationBuilder.IndexKey;
import com.scalar.db.api.OperationBuilder.Or;
import com.scalar.db.api.OperationBuilder.PartitionKey;
import com.scalar.db.api.OperationBuilder.PartitionKeyBuilder;
import com.scalar.db.api.OperationBuilder.Projection;
import com.scalar.db.api.OperationBuilder.TableBuilder;
import com.scalar.db.api.OperationBuilder.WhereAnd;
import com.scalar.db.api.OperationBuilder.WhereOr;
import com.scalar.db.api.Selection.Conjunction;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.io.Key;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public class GetBuilder extends SelectionBuilder {

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

  public static class PartitionKeyOrIndexKey
      extends PartitionKeyBuilder<BuildableGetWithPartitionKey>
      implements IndexKey<BuildableGetWithIndex> {

    private PartitionKeyOrIndexKey(@Nullable String namespace, String table) {
      super(namespace, table);
    }

    @Override
    public BuildableGetWithPartitionKey partitionKey(Key partitionKey) {
      checkNotNull(partitionKey);
      return new BuildableGetWithPartitionKey(namespaceName, tableName, partitionKey);
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

    private BuildableGet(BuildableGet buildable) {
      this(buildable.namespaceName, buildable.tableName, buildable.partitionKey);
      this.clusteringKey = buildable.clusteringKey;
      this.projections.addAll(buildable.projections);
      this.consistency = buildable.consistency;
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
      return build(ImmutableSet.of());
    }

    private Get build(ImmutableSet<Conjunction> conjunctions) {
      return new Get(
          namespaceName,
          tableName,
          partitionKey,
          clusteringKey,
          consistency,
          projections,
          conjunctions);
    }
  }

  public static class BuildableGetWithPartitionKey extends BuildableGet
      implements OperationBuilder.Where<BuildableGetWithOngoingWhere>,
          WhereAnd<BuildableGetWithOngoingWhereAnd>,
          WhereOr<BuildableGetWithOngoingWhereOr> {

    private BuildableGetWithPartitionKey(
        @Nullable String namespace, String table, Key partitionKey) {
      super(namespace, table, partitionKey);
    }

    @Override
    public BuildableGetWithPartitionKey clusteringKey(Key clusteringKey) {
      super.clusteringKey(clusteringKey);
      return this;
    }

    @Override
    public BuildableGetWithPartitionKey projection(String projection) {
      super.projection(projection);
      return this;
    }

    @Override
    public BuildableGetWithPartitionKey projections(Collection<String> projections) {
      super.projections(projections);
      return this;
    }

    @Override
    public BuildableGetWithPartitionKey projections(String... projections) {
      return projections(Arrays.asList(projections));
    }

    @Override
    public BuildableGetWithPartitionKey consistency(com.scalar.db.api.Consistency consistency) {
      super.consistency(consistency);
      return this;
    }

    @Override
    public BuildableGetWithOngoingWhere where(ConditionalExpression condition) {
      checkNotNull(condition);
      return new BuildableGetWithOngoingWhere(this, condition);
    }

    @Override
    public BuildableGetWithOngoingWhereAnd where(OrConditionSet orConditionSet) {
      checkNotNull(orConditionSet);
      return new BuildableGetWithOngoingWhereAnd(this, orConditionSet);
    }

    @Override
    public BuildableGetWithOngoingWhereOr where(AndConditionSet andConditionSet) {
      checkNotNull(andConditionSet);
      return new BuildableGetWithOngoingWhereOr(this, andConditionSet);
    }

    @Override
    public BuildableGetWithOngoingWhereAnd whereAnd(Set<OrConditionSet> orConditionSets) {
      checkNotNull(orConditionSets);
      return new BuildableGetWithOngoingWhereAnd(this, orConditionSets);
    }

    @Override
    public BuildableGetWithOngoingWhereOr whereOr(Set<AndConditionSet> andConditionSets) {
      checkNotNull(andConditionSets);
      return new BuildableGetWithOngoingWhereOr(this, andConditionSets);
    }
  }

  public static class BuildableGetWithOngoingWhere extends BuildableGetWithWhere
      implements And<BuildableGetWithOngoingWhereAnd>, Or<BuildableGetWithOngoingWhereOr> {

    private BuildableGetWithOngoingWhere(BuildableGet buildable, ConditionalExpression condition) {
      super(buildable, condition);
    }

    @Override
    public BuildableGetWithOngoingWhereAnd and(ConditionalExpression condition) {
      checkNotNull(condition);
      where.and(condition);
      return new BuildableGetWithOngoingWhereAnd(this);
    }

    @Override
    public BuildableGetWithOngoingWhereAnd and(OrConditionSet orConditionSet) {
      checkNotNull(orConditionSet);
      where.and(orConditionSet);
      return new BuildableGetWithOngoingWhereAnd(this);
    }

    @Override
    public BuildableGetWithOngoingWhereOr or(ConditionalExpression condition) {
      checkNotNull(condition);
      where.or(condition);
      return new BuildableGetWithOngoingWhereOr(this);
    }

    @Override
    public BuildableGetWithOngoingWhereOr or(AndConditionSet andConditionSet) {
      checkNotNull(andConditionSet);
      where.or(andConditionSet);
      return new BuildableGetWithOngoingWhereOr(this);
    }
  }

  public static class BuildableGetWithOngoingWhereAnd extends BuildableGetWithWhere
      implements And<BuildableGetWithOngoingWhereAnd> {

    private BuildableGetWithOngoingWhereAnd(BuildableGetWithOngoingWhere buildable) {
      super(buildable);
    }

    private BuildableGetWithOngoingWhereAnd(BuildableGet buildable, OrConditionSet orConditionSet) {
      super(buildable);
      where.and(orConditionSet);
    }

    private BuildableGetWithOngoingWhereAnd(
        BuildableGet buildable, Set<OrConditionSet> orConditionSets) {
      super(buildable);
      where.and(orConditionSets);
    }

    @Override
    public BuildableGetWithOngoingWhereAnd and(ConditionalExpression condition) {
      checkNotNull(condition);
      where.and(condition);
      return this;
    }

    @Override
    public BuildableGetWithOngoingWhereAnd and(OrConditionSet orConditionSet) {
      checkNotNull(orConditionSet);
      where.and(orConditionSet);
      return this;
    }
  }

  public static class BuildableGetWithOngoingWhereOr extends BuildableGetWithWhere
      implements Or<BuildableGetWithOngoingWhereOr> {

    private BuildableGetWithOngoingWhereOr(BuildableGetWithOngoingWhere buildable) {
      super(buildable);
    }

    private BuildableGetWithOngoingWhereOr(
        BuildableGet buildable, AndConditionSet andConditionSet) {
      super(buildable);
      where.or(andConditionSet);
    }

    private BuildableGetWithOngoingWhereOr(
        BuildableGet buildable, Set<AndConditionSet> andConditionSets) {
      super(buildable);
      where.or(andConditionSets);
    }

    @Override
    public BuildableGetWithOngoingWhereOr or(ConditionalExpression condition) {
      checkNotNull(condition);
      where.or(condition);
      return this;
    }

    @Override
    public BuildableGetWithOngoingWhereOr or(AndConditionSet andConditionSet) {
      checkNotNull(andConditionSet);
      where.or(andConditionSet);
      return this;
    }
  }

  public static class BuildableGetWithWhere extends BuildableGet {

    final SelectionBuilder.Where where;

    private BuildableGetWithWhere(BuildableGet buildable) {
      this(buildable, null);
    }

    private BuildableGetWithWhere(
        BuildableGet buildable, @Nullable ConditionalExpression condition) {
      super(buildable);
      this.where = new SelectionBuilder.Where(condition);
    }

    private BuildableGetWithWhere(BuildableGetWithOngoingWhere buildable) {
      super(buildable);
      this.where = buildable.where;
    }

    @Override
    public Get build() {
      return super.build(getConjunctions(where));
    }
  }

  public static class BuildableGetWithIndex
      implements Consistency<BuildableGetWithIndex>,
          Projection<BuildableGetWithIndex>,
          OperationBuilder.Where<BuildableGetWithIndexOngoingWhere>,
          WhereAnd<BuildableGetWithIndexOngoingWhereAnd>,
          WhereOr<BuildableGetWithIndexOngoingWhereOr> {
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

    @Override
    public BuildableGetWithIndexOngoingWhere where(ConditionalExpression condition) {
      checkNotNull(condition);
      return new BuildableGetWithIndexOngoingWhere(this, condition);
    }

    @Override
    public BuildableGetWithIndexOngoingWhereAnd where(OrConditionSet orConditionSet) {
      checkNotNull(orConditionSet);
      return new BuildableGetWithIndexOngoingWhereAnd(this, orConditionSet);
    }

    @Override
    public BuildableGetWithIndexOngoingWhereOr where(AndConditionSet andConditionSet) {
      checkNotNull(andConditionSet);
      return new BuildableGetWithIndexOngoingWhereOr(this, andConditionSet);
    }

    @Override
    public BuildableGetWithIndexOngoingWhereAnd whereAnd(Set<OrConditionSet> orConditionSets) {
      checkNotNull(orConditionSets);
      return new BuildableGetWithIndexOngoingWhereAnd(this, orConditionSets);
    }

    @Override
    public BuildableGetWithIndexOngoingWhereOr whereOr(Set<AndConditionSet> andConditionSets) {
      checkNotNull(andConditionSets);
      return new BuildableGetWithIndexOngoingWhereOr(this, andConditionSets);
    }

    public Get build() {
      return build(ImmutableSet.of());
    }

    private Get build(ImmutableSet<Conjunction> conjunctions) {
      return new GetWithIndex(
          namespaceName, tableName, indexKey, consistency, projections, conjunctions);
    }
  }

  public static class BuildableGetWithIndexOngoingWhere extends BuildableGetWithIndexWhere
      implements And<BuildableGetWithIndexOngoingWhereAnd>,
          Or<BuildableGetWithIndexOngoingWhereOr> {

    private BuildableGetWithIndexOngoingWhere(
        BuildableGetWithIndex buildable, ConditionalExpression condition) {
      super(buildable, condition);
    }

    @Override
    public BuildableGetWithIndexOngoingWhereAnd and(ConditionalExpression condition) {
      checkNotNull(condition);
      where.and(condition);
      return new BuildableGetWithIndexOngoingWhereAnd(this);
    }

    @Override
    public BuildableGetWithIndexOngoingWhereAnd and(OrConditionSet orConditionSet) {
      checkNotNull(orConditionSet);
      where.and(orConditionSet);
      return new BuildableGetWithIndexOngoingWhereAnd(this);
    }

    @Override
    public BuildableGetWithIndexOngoingWhereOr or(ConditionalExpression condition) {
      checkNotNull(condition);
      where.or(condition);
      return new BuildableGetWithIndexOngoingWhereOr(this);
    }

    @Override
    public BuildableGetWithIndexOngoingWhereOr or(AndConditionSet andConditionSet) {
      checkNotNull(andConditionSet);
      where.or(andConditionSet);
      return new BuildableGetWithIndexOngoingWhereOr(this);
    }
  }

  public static class BuildableGetWithIndexOngoingWhereAnd extends BuildableGetWithIndexWhere
      implements And<BuildableGetWithIndexOngoingWhereAnd> {

    private BuildableGetWithIndexOngoingWhereAnd(BuildableGetWithIndexOngoingWhere buildable) {
      super(buildable);
    }

    private BuildableGetWithIndexOngoingWhereAnd(
        BuildableGetWithIndex buildable, OrConditionSet orConditionSet) {
      super(buildable);
      where.and(orConditionSet);
    }

    private BuildableGetWithIndexOngoingWhereAnd(
        BuildableGetWithIndex buildable, Set<OrConditionSet> orConditionSets) {
      super(buildable);
      where.and(orConditionSets);
    }

    @Override
    public BuildableGetWithIndexOngoingWhereAnd and(ConditionalExpression condition) {
      checkNotNull(condition);
      where.and(condition);
      return this;
    }

    @Override
    public BuildableGetWithIndexOngoingWhereAnd and(OrConditionSet orConditionSet) {
      checkNotNull(orConditionSet);
      where.and(orConditionSet);
      return this;
    }
  }

  public static class BuildableGetWithIndexOngoingWhereOr extends BuildableGetWithIndexWhere
      implements Or<BuildableGetWithIndexOngoingWhereOr> {

    private BuildableGetWithIndexOngoingWhereOr(BuildableGetWithIndexOngoingWhere buildable) {
      super(buildable);
    }

    private BuildableGetWithIndexOngoingWhereOr(
        BuildableGetWithIndex buildable, AndConditionSet andConditionSet) {
      super(buildable);
      where.or(andConditionSet);
    }

    private BuildableGetWithIndexOngoingWhereOr(
        BuildableGetWithIndex buildable, Set<AndConditionSet> andConditionSets) {
      super(buildable);
      where.or(andConditionSets);
    }

    @Override
    public BuildableGetWithIndexOngoingWhereOr or(ConditionalExpression condition) {
      checkNotNull(condition);
      where.or(condition);
      return this;
    }

    @Override
    public BuildableGetWithIndexOngoingWhereOr or(AndConditionSet andConditionSet) {
      checkNotNull(andConditionSet);
      where.or(andConditionSet);
      return this;
    }
  }

  public static class BuildableGetWithIndexWhere
      implements Consistency<BuildableGetWithIndexWhere>, Projection<BuildableGetWithIndexWhere> {

    BuildableGetWithIndex buildableGetWithIndex;
    final SelectionBuilder.Where where;

    private BuildableGetWithIndexWhere(BuildableGetWithIndex buildable) {
      this(buildable, null);
    }

    private BuildableGetWithIndexWhere(
        BuildableGetWithIndex buildable, @Nullable ConditionalExpression condition) {
      this.buildableGetWithIndex = buildable;
      this.where = new SelectionBuilder.Where(condition);
    }

    private BuildableGetWithIndexWhere(BuildableGetWithIndexOngoingWhere buildable) {
      this.buildableGetWithIndex = buildable.buildableGetWithIndex;
      this.where = buildable.where;
    }

    @Override
    public BuildableGetWithIndexWhere projection(String projection) {
      buildableGetWithIndex = buildableGetWithIndex.projections(projection);
      return this;
    }

    @Override
    public BuildableGetWithIndexWhere projections(Collection<String> projections) {
      buildableGetWithIndex = buildableGetWithIndex.projections(projections);
      return this;
    }

    @Override
    public BuildableGetWithIndexWhere projections(String... projections) {
      return projections(Arrays.asList(projections));
    }

    @Override
    public BuildableGetWithIndexWhere consistency(com.scalar.db.api.Consistency consistency) {
      buildableGetWithIndex = buildableGetWithIndex.consistency(consistency);
      return this;
    }

    public Get build() {
      return buildableGetWithIndex.build(getConjunctions(where));
    }
  }

  public static class BuildableGetOrGetWithIndexFromExisting extends BuildableGet
      implements OperationBuilder.Namespace<BuildableGetOrGetWithIndexFromExisting>,
          OperationBuilder.Table<BuildableGetOrGetWithIndexFromExisting>,
          PartitionKey<BuildableGetOrGetWithIndexFromExisting>,
          IndexKey<BuildableGetOrGetWithIndexFromExisting>,
          OperationBuilder.Where<BuildableGetFromExistingWithOngoingWhere>,
          WhereAnd<BuildableGetFromExistingWithOngoingWhereAnd>,
          WhereOr<BuildableGetFromExistingWithOngoingWhereOr>,
          ClearConditions<BuildableGetOrGetWithIndexFromExisting>,
          ClearProjections<BuildableGetOrGetWithIndexFromExisting>,
          ClearClusteringKey<BuildableGetOrGetWithIndexFromExisting>,
          ClearNamespace<BuildableGetOrGetWithIndexFromExisting> {

    private Key indexKey;
    private final boolean isGetWithIndex;
    final Set<Set<ConditionalExpression>> conjunctions = new HashSet<>();

    BuildableGetOrGetWithIndexFromExisting(Get get) {
      super(get.forNamespace().orElse(null), get.forTable().orElse(null), get.getPartitionKey());
      clusteringKey = get.getClusteringKey().orElse(null);
      projections.addAll(get.getProjections());
      consistency = get.getConsistency();
      isGetWithIndex = get instanceof GetWithIndex;
      if (isGetWithIndex) {
        indexKey = get.getPartitionKey();
      }
      conjunctions.addAll(
          get.getConjunctions().stream()
              .map(Conjunction::getConditions)
              .collect(Collectors.toSet()));
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
    public BuildableGetFromExistingWithOngoingWhere where(ConditionalExpression condition) {
      checkConditionsEmpty();
      checkNotNull(condition);
      return new BuildableGetFromExistingWithOngoingWhere(this, condition);
    }

    @Override
    public BuildableGetFromExistingWithOngoingWhereAnd where(OrConditionSet orConditionSet) {
      checkConditionsEmpty();
      checkNotNull(orConditionSet);
      return new BuildableGetFromExistingWithOngoingWhereAnd(this, orConditionSet);
    }

    @Override
    public BuildableGetFromExistingWithOngoingWhereOr where(AndConditionSet andConditionSet) {
      checkConditionsEmpty();
      checkNotNull(andConditionSet);
      return new BuildableGetFromExistingWithOngoingWhereOr(this, andConditionSet);
    }

    @Override
    public BuildableGetFromExistingWithOngoingWhereAnd whereAnd(
        Set<OrConditionSet> orConditionSets) {
      checkConditionsEmpty();
      checkNotNull(orConditionSets);
      return new BuildableGetFromExistingWithOngoingWhereAnd(this, orConditionSets);
    }

    @Override
    public BuildableGetFromExistingWithOngoingWhereOr whereOr(
        Set<AndConditionSet> andConditionSets) {
      checkConditionsEmpty();
      checkNotNull(andConditionSets);
      return new BuildableGetFromExistingWithOngoingWhereOr(this, andConditionSets);
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
    public BuildableGetOrGetWithIndexFromExisting clearConditions() {
      this.conjunctions.clear();
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
            CoreError
                .GET_BUILD_ERROR_OPERATION_NOT_SUPPORTED_WHEN_GETTING_RECORDS_OF_DATABASE_WITHOUT_USING_INDEX
                .buildMessage());
      }
    }

    private void checkNotGetWithIndex() {
      if (isGetWithIndex) {
        throw new UnsupportedOperationException(
            CoreError
                .GET_BUILD_ERROR_OPERATION_NOT_SUPPORTED_WHEN_GETTING_RECORDS_OF_DATABASE_USING_INDEX
                .buildMessage());
      }
    }

    private void checkConditionsEmpty() {
      if (!conjunctions.isEmpty()) {
        throw new IllegalStateException(
            CoreError.GET_BUILD_ERROR_OPERATION_SUPPORTED_ONLY_WHEN_NO_CONDITIONS_ARE_SPECIFIED
                .buildMessage());
      }
    }

    @Override
    public Get build() {
      return build(
          conjunctions.stream().map(Conjunction::of).collect(ImmutableSet.toImmutableSet()));
    }

    private Get build(ImmutableSet<Conjunction> conjunctions) {
      if (isGetWithIndex) {
        return new GetWithIndex(
            namespaceName, tableName, indexKey, consistency, projections, conjunctions);
      } else {
        return new Get(
            namespaceName,
            tableName,
            partitionKey,
            clusteringKey,
            consistency,
            projections,
            conjunctions);
      }
    }
  }

  public static class BuildableGetFromExistingWithWhere
      implements OperationBuilder.Namespace<BuildableGetFromExistingWithWhere>,
          OperationBuilder.Table<BuildableGetFromExistingWithWhere>,
          PartitionKey<BuildableGetFromExistingWithWhere>,
          ClusteringKey<BuildableGetFromExistingWithWhere>,
          IndexKey<BuildableGetFromExistingWithWhere>,
          Consistency<BuildableGetFromExistingWithWhere>,
          Projection<BuildableGetFromExistingWithWhere>,
          ClearProjections<BuildableGetFromExistingWithWhere>,
          ClearNamespace<BuildableGetFromExistingWithWhere> {

    private final BuildableGetOrGetWithIndexFromExisting BuildableGetFromExisting;
    final SelectionBuilder.Where where;

    private BuildableGetFromExistingWithWhere(BuildableGetFromExistingWithWhere buildable) {
      this.BuildableGetFromExisting = buildable.BuildableGetFromExisting;
      this.where = buildable.where;
    }

    private BuildableGetFromExistingWithWhere(
        BuildableGetOrGetWithIndexFromExisting buildable,
        @Nullable ConditionalExpression condition) {
      this.BuildableGetFromExisting = buildable;
      this.where = new SelectionBuilder.Where(condition);
    }

    private BuildableGetFromExistingWithWhere(BuildableGetOrGetWithIndexFromExisting buildable) {
      this(buildable, null);
    }

    @Override
    public BuildableGetFromExistingWithWhere namespace(String namespaceName) {
      BuildableGetFromExisting.namespace(namespaceName);
      return this;
    }

    @Override
    public BuildableGetFromExistingWithWhere table(String tableName) {
      BuildableGetFromExisting.table(tableName);
      return this;
    }

    @Override
    public BuildableGetFromExistingWithWhere partitionKey(Key partitionKey) {
      BuildableGetFromExisting.partitionKey(partitionKey);
      return this;
    }

    @Override
    public BuildableGetFromExistingWithWhere clusteringKey(Key clusteringKey) {
      BuildableGetFromExisting.clusteringKey(clusteringKey);
      return this;
    }

    @Override
    public BuildableGetFromExistingWithWhere indexKey(Key indexKey) {
      BuildableGetFromExisting.indexKey(indexKey);
      return this;
    }

    @Override
    public BuildableGetFromExistingWithWhere projection(String projection) {
      BuildableGetFromExisting.projections(projection);
      return this;
    }

    @Override
    public BuildableGetFromExistingWithWhere projections(Collection<String> projections) {
      BuildableGetFromExisting.projections(projections);
      return this;
    }

    @Override
    public BuildableGetFromExistingWithWhere projections(String... projections) {
      return projections(Arrays.asList(projections));
    }

    @Override
    public BuildableGetFromExistingWithWhere consistency(
        com.scalar.db.api.Consistency consistency) {
      BuildableGetFromExisting.consistency(consistency);
      return this;
    }

    @Override
    public BuildableGetFromExistingWithWhere clearProjections() {
      BuildableGetFromExisting.clearProjections();
      return this;
    }

    @Override
    public BuildableGetFromExistingWithWhere clearNamespace() {
      BuildableGetFromExisting.clearNamespace();
      return this;
    }

    public Get build() {
      return BuildableGetFromExisting.build(getConjunctions(where));
    }
  }

  public static class BuildableGetFromExistingWithOngoingWhere
      extends BuildableGetFromExistingWithWhere
      implements And<BuildableGetFromExistingWithOngoingWhereAnd>,
          Or<BuildableGetFromExistingWithOngoingWhereOr> {

    private BuildableGetFromExistingWithOngoingWhere(
        BuildableGetOrGetWithIndexFromExisting buildable) {
      super(buildable);
    }

    private BuildableGetFromExistingWithOngoingWhere(
        BuildableGetOrGetWithIndexFromExisting buildable, ConditionalExpression condition) {
      super(buildable, condition);
    }

    private BuildableGetFromExistingWithOngoingWhere(
        BuildableGetFromExistingWithOngoingWhere buildable) {
      super(buildable);
    }

    @Override
    public BuildableGetFromExistingWithOngoingWhereAnd and(ConditionalExpression condition) {
      checkNotNull(condition);
      where.and(condition);
      return new BuildableGetFromExistingWithOngoingWhereAnd(this);
    }

    @Override
    public BuildableGetFromExistingWithOngoingWhereAnd and(OrConditionSet orConditionSet) {
      checkNotNull(orConditionSet);
      where.and(orConditionSet);
      return new BuildableGetFromExistingWithOngoingWhereAnd(this);
    }

    @Override
    public BuildableGetFromExistingWithOngoingWhereOr or(ConditionalExpression condition) {
      checkNotNull(condition);
      where.or(condition);
      return new BuildableGetFromExistingWithOngoingWhereOr(this);
    }

    @Override
    public BuildableGetFromExistingWithOngoingWhereOr or(AndConditionSet andConditionSet) {
      checkNotNull(andConditionSet);
      where.or(andConditionSet);
      return new BuildableGetFromExistingWithOngoingWhereOr(this);
    }
  }

  public static class BuildableGetFromExistingWithOngoingWhereOr
      extends BuildableGetFromExistingWithOngoingWhere
      implements Or<BuildableGetFromExistingWithOngoingWhereOr> {

    private BuildableGetFromExistingWithOngoingWhereOr(
        BuildableGetFromExistingWithOngoingWhere buildable) {
      super(buildable);
    }

    private BuildableGetFromExistingWithOngoingWhereOr(
        BuildableGetOrGetWithIndexFromExisting buildable, AndConditionSet andConditionSet) {
      super(buildable);
      where.or(andConditionSet);
    }

    private BuildableGetFromExistingWithOngoingWhereOr(
        BuildableGetOrGetWithIndexFromExisting buildable, Set<AndConditionSet> andConditionSets) {
      super(buildable);
      where.or(andConditionSets);
    }

    @Override
    public BuildableGetFromExistingWithOngoingWhereOr or(ConditionalExpression condition) {
      checkNotNull(condition);
      where.or(condition);
      return this;
    }

    @Override
    public BuildableGetFromExistingWithOngoingWhereOr or(AndConditionSet andConditionSet) {
      checkNotNull(andConditionSet);
      where.or(andConditionSet);
      return this;
    }
  }

  public static class BuildableGetFromExistingWithOngoingWhereAnd
      extends BuildableGetFromExistingWithOngoingWhere
      implements And<BuildableGetFromExistingWithOngoingWhereAnd> {

    private BuildableGetFromExistingWithOngoingWhereAnd(
        BuildableGetFromExistingWithOngoingWhere buildable) {
      super(buildable);
    }

    private BuildableGetFromExistingWithOngoingWhereAnd(
        BuildableGetOrGetWithIndexFromExisting buildable, OrConditionSet orConditionSet) {
      super(buildable);
      where.and(orConditionSet);
    }

    private BuildableGetFromExistingWithOngoingWhereAnd(
        BuildableGetOrGetWithIndexFromExisting buildable, Set<OrConditionSet> orConditionSets) {
      super(buildable);
      where.and(orConditionSets);
    }

    @Override
    public BuildableGetFromExistingWithOngoingWhereAnd and(ConditionalExpression condition) {
      checkNotNull(condition);
      where.and(condition);
      return this;
    }

    @Override
    public BuildableGetFromExistingWithOngoingWhereAnd and(OrConditionSet orConditionSet) {
      checkNotNull(orConditionSet);
      where.and(orConditionSet);
      return this;
    }
  }
}
