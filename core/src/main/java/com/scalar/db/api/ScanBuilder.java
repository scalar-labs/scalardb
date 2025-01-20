package com.scalar.db.api;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.scalar.db.api.OperationBuilder.AbacReadTagAttribute;
import com.scalar.db.api.OperationBuilder.All;
import com.scalar.db.api.OperationBuilder.And;
import com.scalar.db.api.OperationBuilder.Attribute;
import com.scalar.db.api.OperationBuilder.Buildable;
import com.scalar.db.api.OperationBuilder.ClearAbacReadTagAttribute;
import com.scalar.db.api.OperationBuilder.ClearAttribute;
import com.scalar.db.api.OperationBuilder.ClearBoundaries;
import com.scalar.db.api.OperationBuilder.ClearConditions;
import com.scalar.db.api.OperationBuilder.ClearNamespace;
import com.scalar.db.api.OperationBuilder.ClearOrderings;
import com.scalar.db.api.OperationBuilder.ClearProjections;
import com.scalar.db.api.OperationBuilder.ClusteringKeyFiltering;
import com.scalar.db.api.OperationBuilder.Consistency;
import com.scalar.db.api.OperationBuilder.IndexKey;
import com.scalar.db.api.OperationBuilder.Limit;
import com.scalar.db.api.OperationBuilder.Or;
import com.scalar.db.api.OperationBuilder.Ordering;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public class ScanBuilder extends SelectionBuilder {

  public static class Namespace
      implements OperationBuilder.Namespace<Table>,
          OperationBuilder.Table<PartitionKeyOrIndexKeyOrAll> {

    Namespace() {}

    @Override
    public Table namespace(String namespaceName) {
      checkNotNull(namespaceName);
      return new Table(namespaceName);
    }

    @Override
    public PartitionKeyOrIndexKeyOrAll table(String tableName) {
      checkNotNull(tableName);
      return new PartitionKeyOrIndexKeyOrAll(null, tableName);
    }
  }

  public static class Table extends TableBuilder<PartitionKeyOrIndexKeyOrAll> {

    private Table(String namespaceName) {
      super(namespaceName);
    }

    @Override
    public PartitionKeyOrIndexKeyOrAll table(String tableName) {
      checkNotNull(tableName);
      return new PartitionKeyOrIndexKeyOrAll(namespace, tableName);
    }
  }

  public static class PartitionKeyOrIndexKeyOrAll
      extends PartitionKeyBuilder<BuildableScanWithPartitionKey>
      implements IndexKey<BuildableScanWithIndex>, All<BuildableScanAll> {

    private PartitionKeyOrIndexKeyOrAll(@Nullable String namespace, String table) {
      super(namespace, table);
    }

    @Override
    public BuildableScanWithPartitionKey partitionKey(Key partitionKey) {
      checkNotNull(partitionKey);
      return new BuildableScanWithPartitionKey(namespaceName, tableName, partitionKey);
    }

    @Override
    public BuildableScanWithIndex indexKey(Key indexKey) {
      checkNotNull(indexKey);
      return new BuildableScanWithIndex(namespaceName, tableName, indexKey);
    }

    @Override
    public BuildableScanAll all() {
      return new BuildableScanAll(namespaceName, tableName);
    }
  }

  public static class BuildableScan extends Buildable<Scan>
      implements ClusteringKeyFiltering<BuildableScan>,
          Ordering<BuildableScan>,
          Consistency<BuildableScan>,
          Projection<BuildableScan>,
          Limit<BuildableScan>,
          Attribute<BuildableScan>,
          AbacReadTagAttribute<BuildableScan> {
    final List<Scan.Ordering> orderings = new ArrayList<>();
    final List<String> projections = new ArrayList<>();
    @Nullable Key startClusteringKey;
    boolean startInclusive;
    @Nullable Key endClusteringKey;
    boolean endInclusive;
    int limit = 0;
    @Nullable com.scalar.db.api.Consistency consistency;
    final Map<String, String> attributes = new HashMap<>();

    private BuildableScan(@Nullable String namespace, String table, Key partitionKey) {
      super(namespace, table, partitionKey);
    }

    private BuildableScan(BuildableScan buildable) {
      this(buildable.namespaceName, buildable.tableName, buildable.partitionKey);
      this.orderings.addAll(buildable.orderings);
      this.projections.addAll(buildable.projections);
      this.startClusteringKey = buildable.startClusteringKey;
      this.startInclusive = buildable.startInclusive;
      this.endClusteringKey = buildable.endClusteringKey;
      this.endInclusive = buildable.endInclusive;
      this.limit = buildable.limit;
      this.consistency = buildable.consistency;
    }

    @Override
    public BuildableScan projection(String projection) {
      checkNotNull(projection);
      projections.add(projection);
      return this;
    }

    @Override
    public BuildableScan projections(Collection<String> projections) {
      checkNotNull(projections);
      this.projections.addAll(projections);
      return this;
    }

    @Override
    public BuildableScan projections(String... projections) {
      return projections(Arrays.asList(projections));
    }

    @Override
    public BuildableScan limit(int limit) {
      this.limit = limit;
      return this;
    }

    @Override
    public BuildableScan ordering(Scan.Ordering ordering) {
      checkNotNull(ordering);
      orderings.add(ordering);
      return this;
    }

    @Override
    public BuildableScan orderings(Collection<Scan.Ordering> orderings) {
      checkNotNull(orderings);
      this.orderings.addAll(orderings);
      return this;
    }

    @Override
    public BuildableScan orderings(Scan.Ordering... orderings) {
      return orderings(Arrays.asList(orderings));
    }

    @Override
    public BuildableScan start(Key clusteringKey, boolean inclusive) {
      checkNotNull(clusteringKey);
      startClusteringKey = clusteringKey;
      startInclusive = inclusive;
      return this;
    }

    @Override
    public BuildableScan end(Key clusteringKey, boolean inclusive) {
      checkNotNull(clusteringKey);
      endClusteringKey = clusteringKey;
      endInclusive = inclusive;
      return this;
    }

    @Override
    public BuildableScan consistency(com.scalar.db.api.Consistency consistency) {
      checkNotNull(consistency);
      this.consistency = consistency;
      return this;
    }

    @Override
    public BuildableScan attribute(String name, String value) {
      checkNotNull(name);
      checkNotNull(value);
      attributes.put(name, value);
      return this;
    }

    @Override
    public BuildableScan attributes(Map<String, String> attributes) {
      checkNotNull(attributes);
      this.attributes.putAll(attributes);
      return this;
    }

    @Override
    public BuildableScan readTag(String policyName, String readTag) {
      checkNotNull(policyName);
      checkNotNull(readTag);
      AbacOperationAttributes.setReadTag(attributes, policyName, readTag);
      return this;
    }

    @Override
    public Scan build() {
      return build(ImmutableSet.of());
    }

    private Scan build(ImmutableSet<Conjunction> conjunctions) {
      return new Scan(
          namespaceName,
          tableName,
          partitionKey,
          consistency,
          ImmutableMap.copyOf(attributes),
          projections,
          conjunctions,
          startClusteringKey,
          startInclusive,
          endClusteringKey,
          endInclusive,
          orderings,
          limit);
    }
  }

  public static class BuildableScanWithPartitionKey extends BuildableScan
      implements OperationBuilder.Where<BuildableScanWithOngoingWhere>,
          WhereAnd<BuildableScanWithOngoingWhereAnd>,
          WhereOr<BuildableScanWithOngoingWhereOr> {

    private BuildableScanWithPartitionKey(
        @Nullable String namespace, String table, Key partitionKey) {
      super(namespace, table, partitionKey);
    }

    @Override
    public BuildableScanWithPartitionKey start(Key clusteringKey, boolean inclusive) {
      super.start(clusteringKey, inclusive);
      return this;
    }

    @Override
    public BuildableScanWithPartitionKey end(Key clusteringKey, boolean inclusive) {
      super.end(clusteringKey, inclusive);
      return this;
    }

    @Override
    public BuildableScanWithPartitionKey start(Key clusteringKey) {
      super.start(clusteringKey);
      return this;
    }

    @Override
    public BuildableScanWithPartitionKey end(Key clusteringKey) {
      super.end(clusteringKey);
      return this;
    }

    @Override
    public BuildableScanWithPartitionKey projection(String projection) {
      super.projection(projection);
      return this;
    }

    @Override
    public BuildableScanWithPartitionKey projections(Collection<String> projections) {
      super.projections(projections);
      return this;
    }

    @Override
    public BuildableScanWithPartitionKey projections(String... projections) {
      return projections(Arrays.asList(projections));
    }

    @Override
    public BuildableScanWithPartitionKey ordering(Scan.Ordering ordering) {
      super.ordering(ordering);
      return this;
    }

    @Override
    public BuildableScanWithPartitionKey orderings(Collection<Scan.Ordering> orderings) {
      super.orderings(orderings);
      return this;
    }

    @Override
    public BuildableScanWithPartitionKey orderings(Scan.Ordering... orderings) {
      return orderings(Arrays.asList(orderings));
    }

    @Override
    public BuildableScanWithPartitionKey limit(int limit) {
      super.limit(limit);
      return this;
    }

    @Override
    public BuildableScanWithPartitionKey consistency(com.scalar.db.api.Consistency consistency) {
      super.consistency(consistency);
      return this;
    }

    @Override
    public BuildableScanWithPartitionKey attribute(String name, String value) {
      super.attribute(name, value);
      return this;
    }

    @Override
    public BuildableScanWithPartitionKey attributes(Map<String, String> attributes) {
      super.attributes(attributes);
      return this;
    }

    @Override
    public BuildableScan readTag(String policyName, String readTag) {
      super.readTag(policyName, readTag);
      return this;
    }

    @Override
    public BuildableScanWithOngoingWhere where(ConditionalExpression condition) {
      checkNotNull(condition);
      return new BuildableScanWithOngoingWhere(this, condition);
    }

    @Override
    public BuildableScanWithOngoingWhereAnd where(OrConditionSet orConditionSet) {
      checkNotNull(orConditionSet);
      return new BuildableScanWithOngoingWhereAnd(this, orConditionSet);
    }

    @Override
    public BuildableScanWithOngoingWhereOr where(AndConditionSet andConditionSet) {
      checkNotNull(andConditionSet);
      return new BuildableScanWithOngoingWhereOr(this, andConditionSet);
    }

    @Override
    public BuildableScanWithOngoingWhereAnd whereAnd(Set<OrConditionSet> orConditionSets) {
      checkNotNull(orConditionSets);
      return new BuildableScanWithOngoingWhereAnd(this, orConditionSets);
    }

    @Override
    public BuildableScanWithOngoingWhereOr whereOr(Set<AndConditionSet> andConditionSets) {
      checkNotNull(andConditionSets);
      return new BuildableScanWithOngoingWhereOr(this, andConditionSets);
    }
  }

  public static class BuildableScanWithOngoingWhere extends BuildableScanWithWhere
      implements And<BuildableScanWithOngoingWhereAnd>, Or<BuildableScanWithOngoingWhereOr> {

    private BuildableScanWithOngoingWhere(
        BuildableScan buildable, ConditionalExpression condition) {
      super(buildable, condition);
    }

    @Override
    public BuildableScanWithOngoingWhereAnd and(ConditionalExpression condition) {
      checkNotNull(condition);
      where.and(condition);
      return new BuildableScanWithOngoingWhereAnd(this);
    }

    @Override
    public BuildableScanWithOngoingWhereAnd and(OrConditionSet orConditionSet) {
      checkNotNull(orConditionSet);
      where.and(orConditionSet);
      return new BuildableScanWithOngoingWhereAnd(this);
    }

    @Override
    public BuildableScanWithOngoingWhereOr or(ConditionalExpression condition) {
      checkNotNull(condition);
      where.or(condition);
      return new BuildableScanWithOngoingWhereOr(this);
    }

    @Override
    public BuildableScanWithOngoingWhereOr or(AndConditionSet andConditionSet) {
      checkNotNull(andConditionSet);
      where.or(andConditionSet);
      return new BuildableScanWithOngoingWhereOr(this);
    }
  }

  public static class BuildableScanWithOngoingWhereAnd extends BuildableScanWithWhere
      implements And<BuildableScanWithOngoingWhereAnd> {

    private BuildableScanWithOngoingWhereAnd(BuildableScanWithOngoingWhere buildable) {
      super(buildable);
    }

    private BuildableScanWithOngoingWhereAnd(
        BuildableScan buildable, OrConditionSet orConditionSet) {
      super(buildable);
      where.and(orConditionSet);
    }

    private BuildableScanWithOngoingWhereAnd(
        BuildableScan buildable, Set<OrConditionSet> orConditionSets) {
      super(buildable);
      where.and(orConditionSets);
    }

    @Override
    public BuildableScanWithOngoingWhereAnd and(ConditionalExpression condition) {
      checkNotNull(condition);
      where.and(condition);
      return this;
    }

    @Override
    public BuildableScanWithOngoingWhereAnd and(OrConditionSet orConditionSet) {
      checkNotNull(orConditionSet);
      where.and(orConditionSet);
      return this;
    }
  }

  public static class BuildableScanWithOngoingWhereOr extends BuildableScanWithWhere
      implements Or<BuildableScanWithOngoingWhereOr> {

    private BuildableScanWithOngoingWhereOr(BuildableScanWithOngoingWhere buildable) {
      super(buildable);
    }

    private BuildableScanWithOngoingWhereOr(
        BuildableScan buildable, AndConditionSet andConditionSet) {
      super(buildable);
      where.or(andConditionSet);
    }

    private BuildableScanWithOngoingWhereOr(
        BuildableScan buildable, Set<AndConditionSet> andConditionSets) {
      super(buildable);
      where.or(andConditionSets);
    }

    @Override
    public BuildableScanWithOngoingWhereOr or(ConditionalExpression condition) {
      checkNotNull(condition);
      where.or(condition);
      return this;
    }

    @Override
    public BuildableScanWithOngoingWhereOr or(AndConditionSet andConditionSet) {
      checkNotNull(andConditionSet);
      where.or(andConditionSet);
      return this;
    }
  }

  public static class BuildableScanWithWhere extends BuildableScan {

    final Where where;

    private BuildableScanWithWhere(BuildableScan buildable) {
      this(buildable, null);
    }

    private BuildableScanWithWhere(
        BuildableScan buildable, @Nullable ConditionalExpression condition) {
      super(buildable);
      this.where = new Where(condition);
    }

    private BuildableScanWithWhere(BuildableScanWithOngoingWhere buildable) {
      super(buildable);
      this.where = buildable.where;
    }

    @Override
    public Scan build() {
      return super.build(getConjunctions(where));
    }
  }

  public static class BuildableScanWithIndex
      implements Consistency<BuildableScanWithIndex>,
          Projection<BuildableScanWithIndex>,
          OperationBuilder.Where<BuildableScanWithIndexOngoingWhere>,
          WhereAnd<BuildableScanWithIndexOngoingWhereAnd>,
          WhereOr<BuildableScanWithIndexOngoingWhereOr>,
          Limit<BuildableScanWithIndex>,
          Attribute<BuildableScanWithIndex>,
          AbacReadTagAttribute<BuildableScanWithIndex> {
    @Nullable private final String namespaceName;
    private final String tableName;
    private final Key indexKey;
    private final List<String> projections = new ArrayList<>();
    private int limit = 0;
    @Nullable private com.scalar.db.api.Consistency consistency;
    private final Map<String, String> attributes = new HashMap<>();

    private BuildableScanWithIndex(@Nullable String namespaceName, String tableName, Key indexKey) {
      this.namespaceName = namespaceName;
      this.tableName = tableName;
      this.indexKey = indexKey;
    }

    @Override
    public BuildableScanWithIndex projection(String projection) {
      checkNotNull(projection);
      projections.add(projection);
      return this;
    }

    @Override
    public BuildableScanWithIndex projections(Collection<String> projections) {
      checkNotNull(projections);
      this.projections.addAll(projections);
      return this;
    }

    @Override
    public BuildableScanWithIndex projections(String... projections) {
      return projections(Arrays.asList(projections));
    }

    @Override
    public BuildableScanWithIndex limit(int limit) {
      this.limit = limit;
      return this;
    }

    @Override
    public BuildableScanWithIndex consistency(com.scalar.db.api.Consistency consistency) {
      checkNotNull(consistency);
      this.consistency = consistency;
      return this;
    }

    @Override
    public BuildableScanWithIndex attribute(String name, String value) {
      checkNotNull(name);
      checkNotNull(value);
      attributes.put(name, value);
      return this;
    }

    @Override
    public BuildableScanWithIndex attributes(Map<String, String> attributes) {
      checkNotNull(attributes);
      this.attributes.putAll(attributes);
      return this;
    }

    @Override
    public BuildableScanWithIndex readTag(String policyName, String readTag) {
      checkNotNull(policyName);
      checkNotNull(readTag);
      AbacOperationAttributes.setReadTag(attributes, policyName, readTag);
      return this;
    }

    @Override
    public BuildableScanWithIndexOngoingWhere where(ConditionalExpression condition) {
      checkNotNull(condition);
      return new BuildableScanWithIndexOngoingWhere(this, condition);
    }

    @Override
    public BuildableScanWithIndexOngoingWhereAnd where(OrConditionSet orConditionSet) {
      checkNotNull(orConditionSet);
      return new BuildableScanWithIndexOngoingWhereAnd(this, orConditionSet);
    }

    @Override
    public BuildableScanWithIndexOngoingWhereOr where(AndConditionSet andConditionSet) {
      checkNotNull(andConditionSet);
      return new BuildableScanWithIndexOngoingWhereOr(this, andConditionSet);
    }

    @Override
    public BuildableScanWithIndexOngoingWhereAnd whereAnd(Set<OrConditionSet> orConditionSets) {
      checkNotNull(orConditionSets);
      return new BuildableScanWithIndexOngoingWhereAnd(this, orConditionSets);
    }

    @Override
    public BuildableScanWithIndexOngoingWhereOr whereOr(Set<AndConditionSet> andConditionSets) {
      checkNotNull(andConditionSets);
      return new BuildableScanWithIndexOngoingWhereOr(this, andConditionSets);
    }

    public Scan build() {
      return build(ImmutableSet.of());
    }

    private Scan build(ImmutableSet<Conjunction> conjunctions) {
      return new ScanWithIndex(
          namespaceName,
          tableName,
          indexKey,
          consistency,
          ImmutableMap.copyOf(attributes),
          projections,
          conjunctions,
          limit);
    }
  }

  public static class BuildableScanWithIndexOngoingWhere extends BuildableScanWithIndexWhere
      implements And<BuildableScanWithIndexOngoingWhereAnd>,
          Or<BuildableScanWithIndexOngoingWhereOr> {

    private BuildableScanWithIndexOngoingWhere(
        BuildableScanWithIndex buildable, ConditionalExpression condition) {
      super(buildable, condition);
    }

    @Override
    public BuildableScanWithIndexOngoingWhereAnd and(ConditionalExpression condition) {
      checkNotNull(condition);
      where.and(condition);
      return new BuildableScanWithIndexOngoingWhereAnd(this);
    }

    @Override
    public BuildableScanWithIndexOngoingWhereAnd and(OrConditionSet orConditionSet) {
      checkNotNull(orConditionSet);
      where.and(orConditionSet);
      return new BuildableScanWithIndexOngoingWhereAnd(this);
    }

    @Override
    public BuildableScanWithIndexOngoingWhereOr or(ConditionalExpression condition) {
      checkNotNull(condition);
      where.or(condition);
      return new BuildableScanWithIndexOngoingWhereOr(this);
    }

    @Override
    public BuildableScanWithIndexOngoingWhereOr or(AndConditionSet andConditionSet) {
      checkNotNull(andConditionSet);
      where.or(andConditionSet);
      return new BuildableScanWithIndexOngoingWhereOr(this);
    }
  }

  public static class BuildableScanWithIndexOngoingWhereAnd extends BuildableScanWithIndexWhere
      implements And<BuildableScanWithIndexOngoingWhereAnd> {

    private BuildableScanWithIndexOngoingWhereAnd(BuildableScanWithIndexOngoingWhere buildable) {
      super(buildable);
    }

    private BuildableScanWithIndexOngoingWhereAnd(
        BuildableScanWithIndex buildable, OrConditionSet orConditionSet) {
      super(buildable);
      where.and(orConditionSet);
    }

    private BuildableScanWithIndexOngoingWhereAnd(
        BuildableScanWithIndex buildable, Set<OrConditionSet> orConditionSets) {
      super(buildable);
      where.and(orConditionSets);
    }

    @Override
    public BuildableScanWithIndexOngoingWhereAnd and(ConditionalExpression condition) {
      checkNotNull(condition);
      where.and(condition);
      return this;
    }

    @Override
    public BuildableScanWithIndexOngoingWhereAnd and(OrConditionSet orConditionSet) {
      checkNotNull(orConditionSet);
      where.and(orConditionSet);
      return this;
    }
  }

  public static class BuildableScanWithIndexOngoingWhereOr extends BuildableScanWithIndexWhere
      implements Or<BuildableScanWithIndexOngoingWhereOr> {

    private BuildableScanWithIndexOngoingWhereOr(BuildableScanWithIndexOngoingWhere buildable) {
      super(buildable);
    }

    private BuildableScanWithIndexOngoingWhereOr(
        BuildableScanWithIndex buildable, AndConditionSet andConditionSet) {
      super(buildable);
      where.or(andConditionSet);
    }

    private BuildableScanWithIndexOngoingWhereOr(
        BuildableScanWithIndex buildable, Set<AndConditionSet> andConditionSets) {
      super(buildable);
      where.or(andConditionSets);
    }

    @Override
    public BuildableScanWithIndexOngoingWhereOr or(ConditionalExpression condition) {
      checkNotNull(condition);
      where.or(condition);
      return this;
    }

    @Override
    public BuildableScanWithIndexOngoingWhereOr or(AndConditionSet andConditionSet) {
      checkNotNull(andConditionSet);
      where.or(andConditionSet);
      return this;
    }
  }

  public static class BuildableScanWithIndexWhere
      implements Consistency<BuildableScanWithIndexWhere>,
          Projection<BuildableScanWithIndexWhere>,
          Limit<BuildableScanWithIndexWhere>,
          Attribute<BuildableScanWithIndexWhere>,
          AbacReadTagAttribute<BuildableScanWithIndexWhere> {

    BuildableScanWithIndex buildableScanWithIndex;
    final Where where;

    private BuildableScanWithIndexWhere(BuildableScanWithIndex buildable) {
      this(buildable, null);
    }

    private BuildableScanWithIndexWhere(
        BuildableScanWithIndex buildable, @Nullable ConditionalExpression condition) {
      this.buildableScanWithIndex = buildable;
      this.where = new Where(condition);
    }

    private BuildableScanWithIndexWhere(BuildableScanWithIndexOngoingWhere buildable) {
      this.buildableScanWithIndex = buildable.buildableScanWithIndex;
      this.where = buildable.where;
    }

    @Override
    public BuildableScanWithIndexWhere projection(String projection) {
      buildableScanWithIndex = buildableScanWithIndex.projections(projection);
      return this;
    }

    @Override
    public BuildableScanWithIndexWhere projections(Collection<String> projections) {
      buildableScanWithIndex = buildableScanWithIndex.projections(projections);
      return this;
    }

    @Override
    public BuildableScanWithIndexWhere projections(String... projections) {
      return projections(Arrays.asList(projections));
    }

    @Override
    public BuildableScanWithIndexWhere limit(int limit) {
      buildableScanWithIndex = buildableScanWithIndex.limit(limit);
      return this;
    }

    @Override
    public BuildableScanWithIndexWhere consistency(com.scalar.db.api.Consistency consistency) {
      buildableScanWithIndex = buildableScanWithIndex.consistency(consistency);
      return this;
    }

    @Override
    public BuildableScanWithIndexWhere attribute(String name, String value) {
      buildableScanWithIndex = buildableScanWithIndex.attribute(name, value);
      return this;
    }

    @Override
    public BuildableScanWithIndexWhere attributes(Map<String, String> attributes) {
      buildableScanWithIndex = buildableScanWithIndex.attributes(attributes);
      return this;
    }

    @Override
    public BuildableScanWithIndexWhere readTag(String policyName, String readTag) {
      buildableScanWithIndex = buildableScanWithIndex.readTag(policyName, readTag);
      return this;
    }

    public Scan build() {
      return buildableScanWithIndex.build(getConjunctions(where));
    }
  }

  public static class BuildableScanAll
      implements Ordering<BuildableScanAll>,
          Consistency<BuildableScanAll>,
          Projection<BuildableScanAll>,
          OperationBuilder.Where<BuildableScanAllWithOngoingWhere>,
          WhereAnd<BuildableScanAllWithOngoingWhereAnd>,
          WhereOr<BuildableScanAllWithOngoingWhereOr>,
          Limit<BuildableScanAll>,
          Attribute<BuildableScanAll>,
          AbacReadTagAttribute<BuildableScanAll> {
    private final String namespaceName;
    private final String tableName;
    private final List<Scan.Ordering> orderings = new ArrayList<>();
    private final List<String> projections = new ArrayList<>();
    private int limit = 0;
    @Nullable private com.scalar.db.api.Consistency consistency;
    private final Map<String, String> attributes = new HashMap<>();

    private BuildableScanAll(String namespaceName, String tableName) {
      this.namespaceName = namespaceName;
      this.tableName = tableName;
    }

    @Override
    public BuildableScanAll projection(String projection) {
      checkNotNull(projection);
      projections.add(projection);
      return this;
    }

    @Override
    public BuildableScanAll projections(Collection<String> projections) {
      checkNotNull(projections);
      this.projections.addAll(projections);
      return this;
    }

    @Override
    public BuildableScanAll projections(String... projections) {
      return projections(Arrays.asList(projections));
    }

    @Override
    public BuildableScanAll limit(int limit) {
      this.limit = limit;
      return this;
    }

    @Override
    public BuildableScanAll ordering(Scan.Ordering ordering) {
      checkNotNull(ordering);
      orderings.add(ordering);
      return this;
    }

    @Override
    public BuildableScanAll orderings(Collection<Scan.Ordering> orderings) {
      checkNotNull(orderings);
      this.orderings.addAll(orderings);
      return this;
    }

    @Override
    public BuildableScanAll orderings(Scan.Ordering... orderings) {
      return orderings(Arrays.asList(orderings));
    }

    @Override
    public BuildableScanAll consistency(com.scalar.db.api.Consistency consistency) {
      checkNotNull(consistency);
      this.consistency = consistency;
      return this;
    }

    @Override
    public BuildableScanAll attribute(String name, String value) {
      checkNotNull(name);
      checkNotNull(value);
      attributes.put(name, value);
      return this;
    }

    @Override
    public BuildableScanAll attributes(Map<String, String> attributes) {
      checkNotNull(attributes);
      this.attributes.putAll(attributes);
      return this;
    }

    @Override
    public BuildableScanAll readTag(String policyName, String readTag) {
      checkNotNull(policyName);
      checkNotNull(readTag);
      AbacOperationAttributes.setReadTag(attributes, policyName, readTag);
      return this;
    }

    @Override
    public BuildableScanAllWithOngoingWhere where(ConditionalExpression condition) {
      checkNotNull(condition);
      return new BuildableScanAllWithOngoingWhere(this, condition);
    }

    @Override
    public BuildableScanAllWithOngoingWhereAnd where(OrConditionSet orConditionSet) {
      checkNotNull(orConditionSet);
      return new BuildableScanAllWithOngoingWhereAnd(this, orConditionSet);
    }

    @Override
    public BuildableScanAllWithOngoingWhereOr where(AndConditionSet andConditionSet) {
      checkNotNull(andConditionSet);
      return new BuildableScanAllWithOngoingWhereOr(this, andConditionSet);
    }

    @Override
    public BuildableScanAllWithOngoingWhereAnd whereAnd(Set<OrConditionSet> orConditionSets) {
      checkNotNull(orConditionSets);
      return new BuildableScanAllWithOngoingWhereAnd(this, orConditionSets);
    }

    @Override
    public BuildableScanAllWithOngoingWhereOr whereOr(Set<AndConditionSet> andConditionSets) {
      checkNotNull(andConditionSets);
      return new BuildableScanAllWithOngoingWhereOr(this, andConditionSets);
    }

    public Scan build() {
      return build(ImmutableSet.of());
    }

    private Scan build(ImmutableSet<Conjunction> conjunctions) {
      return new ScanAll(
          namespaceName,
          tableName,
          consistency,
          ImmutableMap.copyOf(attributes),
          projections,
          conjunctions,
          orderings,
          limit);
    }
  }

  public static class BuildableScanAllWithOngoingWhere extends BuildableScanAllWithWhere
      implements And<BuildableScanAllWithOngoingWhereAnd>, Or<BuildableScanAllWithOngoingWhereOr> {

    private BuildableScanAllWithOngoingWhere(
        BuildableScanAll buildable, ConditionalExpression condition) {
      super(buildable, condition);
    }

    @Override
    public BuildableScanAllWithOngoingWhereAnd and(ConditionalExpression condition) {
      checkNotNull(condition);
      where.and(condition);
      return new BuildableScanAllWithOngoingWhereAnd(this);
    }

    @Override
    public BuildableScanAllWithOngoingWhereAnd and(OrConditionSet orConditionSet) {
      checkNotNull(orConditionSet);
      where.and(orConditionSet);
      return new BuildableScanAllWithOngoingWhereAnd(this);
    }

    @Override
    public BuildableScanAllWithOngoingWhereOr or(ConditionalExpression condition) {
      checkNotNull(condition);
      where.or(condition);
      return new BuildableScanAllWithOngoingWhereOr(this);
    }

    @Override
    public BuildableScanAllWithOngoingWhereOr or(AndConditionSet andConditionSet) {
      checkNotNull(andConditionSet);
      where.or(andConditionSet);
      return new BuildableScanAllWithOngoingWhereOr(this);
    }
  }

  public static class BuildableScanAllWithOngoingWhereAnd extends BuildableScanAllWithWhere
      implements And<BuildableScanAllWithOngoingWhereAnd> {

    private BuildableScanAllWithOngoingWhereAnd(BuildableScanAllWithOngoingWhere buildable) {
      super(buildable);
    }

    private BuildableScanAllWithOngoingWhereAnd(
        BuildableScanAll buildable, OrConditionSet orConditionSet) {
      super(buildable);
      where.and(orConditionSet);
    }

    private BuildableScanAllWithOngoingWhereAnd(
        BuildableScanAll buildable, Set<OrConditionSet> orConditionSets) {
      super(buildable);
      where.and(orConditionSets);
    }

    @Override
    public BuildableScanAllWithOngoingWhereAnd and(ConditionalExpression condition) {
      checkNotNull(condition);
      where.and(condition);
      return this;
    }

    @Override
    public BuildableScanAllWithOngoingWhereAnd and(OrConditionSet orConditionSet) {
      checkNotNull(orConditionSet);
      where.and(orConditionSet);
      return this;
    }
  }

  public static class BuildableScanAllWithOngoingWhereOr extends BuildableScanAllWithWhere
      implements Or<BuildableScanAllWithOngoingWhereOr> {

    private BuildableScanAllWithOngoingWhereOr(BuildableScanAllWithOngoingWhere buildable) {
      super(buildable);
    }

    private BuildableScanAllWithOngoingWhereOr(
        BuildableScanAll buildable, AndConditionSet andConditionSet) {
      super(buildable);
      where.or(andConditionSet);
    }

    private BuildableScanAllWithOngoingWhereOr(
        BuildableScanAll buildable, Set<AndConditionSet> andConditionSets) {
      super(buildable);
      where.or(andConditionSets);
    }

    @Override
    public BuildableScanAllWithOngoingWhereOr or(ConditionalExpression condition) {
      checkNotNull(condition);
      where.or(condition);
      return this;
    }

    @Override
    public BuildableScanAllWithOngoingWhereOr or(AndConditionSet andConditionSet) {
      checkNotNull(andConditionSet);
      where.or(andConditionSet);
      return this;
    }
  }

  public static class BuildableScanAllWithWhere
      implements Consistency<BuildableScanAllWithWhere>,
          Projection<BuildableScanAllWithWhere>,
          Ordering<BuildableScanAllWithWhere>,
          Limit<BuildableScanAllWithWhere>,
          Attribute<BuildableScanAllWithWhere>,
          AbacReadTagAttribute<BuildableScanAllWithWhere> {

    final BuildableScanAll buildableScanAll;
    final Where where;

    private BuildableScanAllWithWhere(BuildableScanAll buildable) {
      this(buildable, null);
    }

    private BuildableScanAllWithWhere(
        BuildableScanAll buildable, @Nullable ConditionalExpression condition) {
      this.buildableScanAll = buildable;
      this.where = new Where(condition);
    }

    private BuildableScanAllWithWhere(BuildableScanAllWithOngoingWhere buildable) {
      this.buildableScanAll = buildable.buildableScanAll;
      this.where = buildable.where;
    }

    @Override
    public BuildableScanAllWithWhere projection(String projection) {
      buildableScanAll.projection(projection);
      return this;
    }

    @Override
    public BuildableScanAllWithWhere projections(Collection<String> projections) {
      buildableScanAll.projections(projections);
      return this;
    }

    @Override
    public BuildableScanAllWithWhere projections(String... projections) {
      return projections(Arrays.asList(projections));
    }

    @Override
    public BuildableScanAllWithWhere ordering(Scan.Ordering ordering) {
      buildableScanAll.ordering(ordering);
      return this;
    }

    @Override
    public BuildableScanAllWithWhere orderings(Collection<Scan.Ordering> orderings) {
      buildableScanAll.orderings(orderings);
      return this;
    }

    @Override
    public BuildableScanAllWithWhere orderings(Scan.Ordering... orderings) {
      return orderings(Arrays.asList(orderings));
    }

    @Override
    public BuildableScanAllWithWhere limit(int limit) {
      buildableScanAll.limit(limit);
      return this;
    }

    @Override
    public BuildableScanAllWithWhere consistency(com.scalar.db.api.Consistency consistency) {
      buildableScanAll.consistency(consistency);
      return this;
    }

    @Override
    public BuildableScanAllWithWhere attribute(String name, String value) {
      buildableScanAll.attribute(name, value);
      return this;
    }

    @Override
    public BuildableScanAllWithWhere attributes(Map<String, String> attributes) {
      buildableScanAll.attributes(attributes);
      return this;
    }

    @Override
    public BuildableScanAllWithWhere readTag(String policyName, String readTag) {
      buildableScanAll.readTag(policyName, readTag);
      return this;
    }

    public Scan build() {
      return buildableScanAll.build(getConjunctions(where));
    }
  }

  public static class BuildableScanOrScanAllFromExisting extends BuildableScan
      implements OperationBuilder.Namespace<BuildableScanOrScanAllFromExisting>,
          OperationBuilder.Table<BuildableScanOrScanAllFromExisting>,
          PartitionKey<BuildableScanOrScanAllFromExisting>,
          IndexKey<BuildableScanOrScanAllFromExisting>,
          OperationBuilder.Where<BuildableScanFromExistingWithOngoingWhere>,
          WhereAnd<BuildableScanFromExistingWithOngoingWhereAnd>,
          WhereOr<BuildableScanFromExistingWithOngoingWhereOr>,
          ClearConditions<BuildableScanOrScanAllFromExisting>,
          ClearProjections<BuildableScanOrScanAllFromExisting>,
          ClearOrderings<BuildableScanOrScanAllFromExisting>,
          ClearBoundaries<BuildableScanOrScanAllFromExisting>,
          ClearNamespace<BuildableScanOrScanAllFromExisting>,
          ClearAttribute<BuildableScanOrScanAllFromExisting>,
          ClearAbacReadTagAttribute<BuildableScanOrScanAllFromExisting> {

    private final boolean isScanWithIndex;
    private final boolean isScanAll;
    private Key indexKey;
    final Set<Set<ConditionalExpression>> conjunctions = new HashSet<>();

    BuildableScanOrScanAllFromExisting(Scan scan) {
      super(scan.forNamespace().orElse(null), scan.forTable().orElse(null), scan.getPartitionKey());
      isScanWithIndex = scan instanceof ScanWithIndex;
      if (isScanWithIndex) {
        indexKey = scan.getPartitionKey();
      }
      isScanAll = scan instanceof ScanAll;
      scan.getStartClusteringKey()
          .ifPresent(
              key -> {
                startClusteringKey = key;
                startInclusive = scan.getStartInclusive();
              });
      scan.getEndClusteringKey()
          .ifPresent(
              key -> {
                endClusteringKey = key;
                endInclusive = scan.getEndInclusive();
              });
      limit = scan.getLimit();
      orderings.addAll(scan.getOrderings());
      projections.addAll(scan.getProjections());
      consistency = scan.getConsistency();
      attributes.putAll(scan.getAttributes());
      conjunctions.addAll(
          scan.getConjunctions().stream()
              .map(Conjunction::getConditions)
              .collect(Collectors.toSet()));
    }

    @Override
    public BuildableScanOrScanAllFromExisting namespace(String namespaceName) {
      checkNotNull(namespaceName);
      this.namespaceName = namespaceName;
      return this;
    }

    @Override
    public BuildableScanOrScanAllFromExisting table(String tableName) {
      checkNotNull(tableName);
      this.tableName = tableName;
      return this;
    }

    @Override
    public BuildableScanOrScanAllFromExisting partitionKey(Key partitionKey) {
      checkNotScanWithIndexOrScanAll();
      checkNotNull(partitionKey);
      this.partitionKey = partitionKey;
      return this;
    }

    @Override
    public BuildableScanOrScanAllFromExisting indexKey(Key indexKey) {
      checkScanWithIndex();
      checkNotNull(indexKey);
      this.indexKey = indexKey;
      return this;
    }

    @Override
    public BuildableScanOrScanAllFromExisting consistency(
        com.scalar.db.api.Consistency consistency) {
      super.consistency(consistency);
      return this;
    }

    @Override
    public BuildableScanOrScanAllFromExisting attribute(String name, String value) {
      super.attribute(name, value);
      return this;
    }

    @Override
    public BuildableScanOrScanAllFromExisting attributes(Map<String, String> attributes) {
      super.attributes(attributes);
      return this;
    }

    @Override
    public BuildableScan readTag(String policyName, String readTag) {
      super.readTag(policyName, readTag);
      return this;
    }

    @Override
    public BuildableScanOrScanAllFromExisting projection(String projection) {
      super.projection(projection);
      return this;
    }

    @Override
    public BuildableScanOrScanAllFromExisting projections(Collection<String> projections) {
      super.projections(projections);
      return this;
    }

    @Override
    public BuildableScanOrScanAllFromExisting projections(String... projections) {
      super.projections(projections);
      return this;
    }

    @Override
    public BuildableScanOrScanAllFromExisting clearProjections() {
      this.projections.clear();
      return this;
    }

    @Override
    public BuildableScanOrScanAllFromExisting limit(int limit) {
      super.limit(limit);
      return this;
    }

    @Override
    public BuildableScanOrScanAllFromExisting ordering(Scan.Ordering ordering) {
      checkNotScanWithIndex();
      super.ordering(ordering);
      return this;
    }

    @Override
    public BuildableScanOrScanAllFromExisting orderings(Collection<Scan.Ordering> orderings) {
      checkNotScanWithIndex();
      super.orderings(orderings);
      return this;
    }

    @Override
    public BuildableScanOrScanAllFromExisting orderings(Scan.Ordering... orderings) {
      checkNotScanWithIndex();
      super.orderings(orderings);
      return this;
    }

    @Override
    public BuildableScanOrScanAllFromExisting start(Key clusteringKey, boolean inclusive) {
      checkNotScanWithIndexOrScanAll();
      super.start(clusteringKey, inclusive);
      return this;
    }

    @Override
    public BuildableScanOrScanAllFromExisting end(Key clusteringKey, boolean inclusive) {
      checkNotScanWithIndexOrScanAll();
      super.end(clusteringKey, inclusive);
      return this;
    }

    @Override
    public BuildableScanOrScanAllFromExisting start(Key clusteringKey) {
      checkNotScanWithIndexOrScanAll();
      super.start(clusteringKey);
      return this;
    }

    @Override
    public BuildableScanOrScanAllFromExisting end(Key clusteringKey) {
      checkNotScanWithIndexOrScanAll();
      super.end(clusteringKey);
      return this;
    }

    @Override
    public BuildableScanFromExistingWithOngoingWhere where(ConditionalExpression condition) {
      checkConditionsEmpty();
      checkNotNull(condition);
      return new BuildableScanFromExistingWithOngoingWhere(this, condition);
    }

    @Override
    public BuildableScanFromExistingWithOngoingWhereAnd where(OrConditionSet orConditionSet) {
      checkConditionsEmpty();
      checkNotNull(orConditionSet);
      return new BuildableScanFromExistingWithOngoingWhereAnd(this, orConditionSet);
    }

    @Override
    public BuildableScanFromExistingWithOngoingWhereOr where(AndConditionSet andConditionSet) {
      checkConditionsEmpty();
      checkNotNull(andConditionSet);
      return new BuildableScanFromExistingWithOngoingWhereOr(this, andConditionSet);
    }

    @Override
    public BuildableScanFromExistingWithOngoingWhereAnd whereAnd(
        Set<OrConditionSet> orConditionSets) {
      checkConditionsEmpty();
      checkNotNull(orConditionSets);
      return new BuildableScanFromExistingWithOngoingWhereAnd(this, orConditionSets);
    }

    @Override
    public BuildableScanFromExistingWithOngoingWhereOr whereOr(
        Set<AndConditionSet> andConditionSets) {
      checkConditionsEmpty();
      checkNotNull(andConditionSets);
      return new BuildableScanFromExistingWithOngoingWhereOr(this, andConditionSets);
    }

    @Override
    public BuildableScanOrScanAllFromExisting clearStart() {
      checkNotScanWithIndexOrScanAll();
      this.startClusteringKey = null;
      this.startInclusive = false;
      return this;
    }

    @Override
    public BuildableScanOrScanAllFromExisting clearEnd() {
      checkNotScanWithIndexOrScanAll();
      this.endClusteringKey = null;
      this.endInclusive = false;
      return this;
    }

    @Override
    public BuildableScanOrScanAllFromExisting clearOrderings() {
      checkNotScanWithIndex();
      this.orderings.clear();
      return this;
    }

    @Override
    public BuildableScanOrScanAllFromExisting clearConditions() {
      this.conjunctions.clear();
      return this;
    }

    @Override
    public BuildableScanOrScanAllFromExisting clearNamespace() {
      this.namespaceName = null;
      return this;
    }

    @Override
    public BuildableScanOrScanAllFromExisting clearAttributes() {
      this.attributes.clear();
      return this;
    }

    @Override
    public BuildableScanOrScanAllFromExisting clearAttribute(String name) {
      checkNotNull(name);
      attributes.remove(name);
      return this;
    }

    @Override
    public BuildableScanOrScanAllFromExisting clearReadTag(String policyName) {
      AbacOperationAttributes.clearReadTag(attributes, policyName);
      return this;
    }

    @Override
    public BuildableScanOrScanAllFromExisting clearReadTags() {
      AbacOperationAttributes.clearReadTags(attributes);
      return this;
    }

    private void checkNotScanWithIndexOrScanAll() {
      if (isScanWithIndex || isScanAll) {
        throw new UnsupportedOperationException(
            CoreError
                .SCAN_BUILD_ERROR_OPERATION_NOT_SUPPORTED_WHEN_SCANNING_ALL_RECORDS_OF_DATABASE_OR_SCANNING_RECORDS_OF_DATABASE_USING_INDEX
                .buildMessage());
      }
    }

    private void checkScanWithIndex() {
      if (!isScanWithIndex) {
        throw new UnsupportedOperationException(
            CoreError
                .SCAN_BUILD_ERROR_OPERATION_SUPPORTED_ONLY_WHEN_SCANNING_RECORDS_OF_DATABASE_USING_INDEX
                .buildMessage());
      }
    }

    private void checkNotScanWithIndex() {
      if (isScanWithIndex) {
        throw new UnsupportedOperationException(
            CoreError
                .SCAN_BUILD_ERROR_OPERATION_NOT_SUPPORTED_WHEN_SCANNING_RECORDS_OF_DATABASE_USING_INDEX
                .buildMessage());
      }
    }

    private void checkConditionsEmpty() {
      if (!conjunctions.isEmpty()) {
        throw new IllegalStateException(
            CoreError.SCAN_BUILD_ERROR_OPERATION_SUPPORTED_ONLY_WHEN_NO_CONDITIONS_ARE_SPECIFIED
                .buildMessage());
      }
    }

    @Override
    public Scan build() {
      return build(
          conjunctions.stream().map(Conjunction::of).collect(ImmutableSet.toImmutableSet()));
    }

    private Scan build(ImmutableSet<Conjunction> conjunctions) {
      if (isScanWithIndex) {
        return new ScanWithIndex(
            namespaceName,
            tableName,
            indexKey,
            consistency,
            ImmutableMap.copyOf(attributes),
            projections,
            conjunctions,
            limit);
      } else if (isScanAll) {
        return new ScanAll(
            namespaceName,
            tableName,
            consistency,
            ImmutableMap.copyOf(attributes),
            projections,
            conjunctions,
            orderings,
            limit);
      } else {
        return new Scan(
            namespaceName,
            tableName,
            partitionKey,
            consistency,
            ImmutableMap.copyOf(attributes),
            projections,
            conjunctions,
            startClusteringKey,
            startInclusive,
            endClusteringKey,
            endInclusive,
            orderings,
            limit);
      }
    }
  }

  public static class BuildableScanFromExistingWithWhere
      implements OperationBuilder.Namespace<BuildableScanFromExistingWithWhere>,
          OperationBuilder.Table<BuildableScanFromExistingWithWhere>,
          PartitionKey<BuildableScanFromExistingWithWhere>,
          ClusteringKeyFiltering<BuildableScanFromExistingWithWhere>,
          IndexKey<BuildableScanFromExistingWithWhere>,
          Consistency<BuildableScanFromExistingWithWhere>,
          Projection<BuildableScanFromExistingWithWhere>,
          Ordering<BuildableScanFromExistingWithWhere>,
          Limit<BuildableScanFromExistingWithWhere>,
          Attribute<BuildableScanFromExistingWithWhere>,
          AbacReadTagAttribute<BuildableScanFromExistingWithWhere>,
          ClearProjections<BuildableScanFromExistingWithWhere>,
          ClearOrderings<BuildableScanFromExistingWithWhere>,
          ClearNamespace<BuildableScanFromExistingWithWhere>,
          ClearAttribute<BuildableScanFromExistingWithWhere>,
          ClearAbacReadTagAttribute<BuildableScanFromExistingWithWhere> {

    private final BuildableScanOrScanAllFromExisting buildableScanFromExisting;
    final Where where;

    private BuildableScanFromExistingWithWhere(BuildableScanFromExistingWithWhere buildable) {
      this.buildableScanFromExisting = buildable.buildableScanFromExisting;
      this.where = buildable.where;
    }

    private BuildableScanFromExistingWithWhere(
        BuildableScanOrScanAllFromExisting buildable, @Nullable ConditionalExpression condition) {
      this.buildableScanFromExisting = buildable;
      this.where = new Where(condition);
    }

    private BuildableScanFromExistingWithWhere(BuildableScanOrScanAllFromExisting buildable) {
      this(buildable, null);
    }

    @Override
    public BuildableScanFromExistingWithWhere namespace(String namespaceName) {
      buildableScanFromExisting.namespace(namespaceName);
      return this;
    }

    @Override
    public BuildableScanFromExistingWithWhere table(String tableName) {
      buildableScanFromExisting.table(tableName);
      return this;
    }

    @Override
    public BuildableScanFromExistingWithWhere partitionKey(Key partitionKey) {
      buildableScanFromExisting.partitionKey(partitionKey);
      return this;
    }

    @Override
    public BuildableScanFromExistingWithWhere start(Key clusteringKey, boolean inclusive) {
      buildableScanFromExisting.start(clusteringKey, inclusive);
      return this;
    }

    @Override
    public BuildableScanFromExistingWithWhere end(Key clusteringKey, boolean inclusive) {
      buildableScanFromExisting.end(clusteringKey, inclusive);
      return this;
    }

    @Override
    public BuildableScanFromExistingWithWhere indexKey(Key indexKey) {
      buildableScanFromExisting.indexKey(indexKey);
      return this;
    }

    @Override
    public BuildableScanFromExistingWithWhere projection(String projection) {
      buildableScanFromExisting.projections(projection);
      return this;
    }

    @Override
    public BuildableScanFromExistingWithWhere projections(Collection<String> projections) {
      buildableScanFromExisting.projections(projections);
      return this;
    }

    @Override
    public BuildableScanFromExistingWithWhere projections(String... projections) {
      return projections(Arrays.asList(projections));
    }

    @Override
    public BuildableScanFromExistingWithWhere ordering(Scan.Ordering ordering) {
      buildableScanFromExisting.ordering(ordering);
      return this;
    }

    @Override
    public BuildableScanFromExistingWithWhere orderings(Collection<Scan.Ordering> orderings) {
      buildableScanFromExisting.orderings(orderings);
      return this;
    }

    @Override
    public BuildableScanFromExistingWithWhere orderings(Scan.Ordering... orderings) {
      return orderings(Arrays.asList(orderings));
    }

    @Override
    public BuildableScanFromExistingWithWhere limit(int limit) {
      buildableScanFromExisting.limit(limit);
      return this;
    }

    @Override
    public BuildableScanFromExistingWithWhere consistency(
        com.scalar.db.api.Consistency consistency) {
      buildableScanFromExisting.consistency(consistency);
      return this;
    }

    @Override
    public BuildableScanFromExistingWithWhere attribute(String name, String value) {
      buildableScanFromExisting.attribute(name, value);
      return this;
    }

    @Override
    public BuildableScanFromExistingWithWhere attributes(Map<String, String> attributes) {
      buildableScanFromExisting.attributes(attributes);
      return this;
    }

    @Override
    public BuildableScanFromExistingWithWhere readTag(String policyName, String readTag) {
      buildableScanFromExisting.readTag(policyName, readTag);
      return this;
    }

    @Override
    public BuildableScanFromExistingWithWhere clearProjections() {
      buildableScanFromExisting.clearProjections();
      return this;
    }

    @Override
    public BuildableScanFromExistingWithWhere clearOrderings() {
      buildableScanFromExisting.clearOrderings();
      return this;
    }

    @Override
    public BuildableScanFromExistingWithWhere clearNamespace() {
      buildableScanFromExisting.clearNamespace();
      return this;
    }

    @Override
    public BuildableScanFromExistingWithWhere clearAttributes() {
      buildableScanFromExisting.clearAttributes();
      return this;
    }

    @Override
    public BuildableScanFromExistingWithWhere clearAttribute(String name) {
      buildableScanFromExisting.clearAttribute(name);
      return this;
    }

    @Override
    public BuildableScanFromExistingWithWhere clearReadTag(String policyName) {
      buildableScanFromExisting.clearReadTag(policyName);
      return this;
    }

    @Override
    public BuildableScanFromExistingWithWhere clearReadTags() {
      buildableScanFromExisting.clearReadTags();
      return this;
    }

    public Scan build() {
      return buildableScanFromExisting.build(getConjunctions(where));
    }
  }

  public static class BuildableScanFromExistingWithOngoingWhere
      extends BuildableScanFromExistingWithWhere
      implements And<BuildableScanFromExistingWithOngoingWhereAnd>,
          Or<BuildableScanFromExistingWithOngoingWhereOr> {

    private BuildableScanFromExistingWithOngoingWhere(
        BuildableScanOrScanAllFromExisting buildable, ConditionalExpression condition) {
      super(buildable, condition);
    }

    @Override
    public BuildableScanFromExistingWithOngoingWhereAnd and(ConditionalExpression condition) {
      checkNotNull(condition);
      where.and(condition);
      return new BuildableScanFromExistingWithOngoingWhereAnd(this);
    }

    @Override
    public BuildableScanFromExistingWithOngoingWhereAnd and(OrConditionSet orConditionSet) {
      checkNotNull(orConditionSet);
      where.and(orConditionSet);
      return new BuildableScanFromExistingWithOngoingWhereAnd(this);
    }

    @Override
    public BuildableScanFromExistingWithOngoingWhereOr or(ConditionalExpression condition) {
      checkNotNull(condition);
      where.or(condition);
      return new BuildableScanFromExistingWithOngoingWhereOr(this);
    }

    @Override
    public BuildableScanFromExistingWithOngoingWhereOr or(AndConditionSet andConditionSet) {
      checkNotNull(andConditionSet);
      where.or(andConditionSet);
      return new BuildableScanFromExistingWithOngoingWhereOr(this);
    }
  }

  public static class BuildableScanFromExistingWithOngoingWhereOr
      extends BuildableScanFromExistingWithWhere
      implements Or<BuildableScanFromExistingWithOngoingWhereOr> {

    private BuildableScanFromExistingWithOngoingWhereOr(
        BuildableScanFromExistingWithOngoingWhere buildable) {
      super(buildable);
    }

    private BuildableScanFromExistingWithOngoingWhereOr(
        BuildableScanOrScanAllFromExisting buildable, AndConditionSet andConditionSet) {
      super(buildable);
      where.or(andConditionSet);
    }

    private BuildableScanFromExistingWithOngoingWhereOr(
        BuildableScanOrScanAllFromExisting buildable, Set<AndConditionSet> andConditionSets) {
      super(buildable);
      where.or(andConditionSets);
    }

    @Override
    public BuildableScanFromExistingWithOngoingWhereOr or(ConditionalExpression condition) {
      checkNotNull(condition);
      where.or(condition);
      return this;
    }

    @Override
    public BuildableScanFromExistingWithOngoingWhereOr or(AndConditionSet andConditionSet) {
      checkNotNull(andConditionSet);
      where.or(andConditionSet);
      return this;
    }
  }

  public static class BuildableScanFromExistingWithOngoingWhereAnd
      extends BuildableScanFromExistingWithWhere
      implements And<BuildableScanFromExistingWithOngoingWhereAnd> {

    private BuildableScanFromExistingWithOngoingWhereAnd(
        BuildableScanFromExistingWithOngoingWhere buildable) {
      super(buildable);
    }

    private BuildableScanFromExistingWithOngoingWhereAnd(
        BuildableScanOrScanAllFromExisting buildable, OrConditionSet orConditionSet) {
      super(buildable);
      where.and(orConditionSet);
    }

    private BuildableScanFromExistingWithOngoingWhereAnd(
        BuildableScanOrScanAllFromExisting buildable, Set<OrConditionSet> orConditionSets) {
      super(buildable);
      where.and(orConditionSets);
    }

    @Override
    public BuildableScanFromExistingWithOngoingWhereAnd and(ConditionalExpression condition) {
      checkNotNull(condition);
      where.and(condition);
      return this;
    }

    @Override
    public BuildableScanFromExistingWithOngoingWhereAnd and(OrConditionSet orConditionSet) {
      checkNotNull(orConditionSet);
      where.and(orConditionSet);
      return this;
    }
  }
}
