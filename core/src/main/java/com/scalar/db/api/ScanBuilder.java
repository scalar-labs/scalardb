package com.scalar.db.api;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.scalar.db.api.OperationBuilder.All;
import com.scalar.db.api.OperationBuilder.And;
import com.scalar.db.api.OperationBuilder.Buildable;
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
import com.scalar.db.api.OperationBuilder.Where;
import com.scalar.db.api.OperationBuilder.WhereAnd;
import com.scalar.db.api.OperationBuilder.WhereOr;
import com.scalar.db.api.Scan.Conjunction;
import com.scalar.db.io.Key;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

public class ScanBuilder {

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

  public static class PartitionKeyOrIndexKeyOrAll extends PartitionKeyBuilder<BuildableScan>
      implements IndexKey<BuildableScanWithIndex>, All<BuildableScanAll> {

    private PartitionKeyOrIndexKeyOrAll(@Nullable String namespace, String table) {
      super(namespace, table);
    }

    @Override
    public BuildableScan partitionKey(Key partitionKey) {
      checkNotNull(partitionKey);
      return new BuildableScan(namespaceName, tableName, partitionKey);
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
          Limit<BuildableScan> {
    final List<Scan.Ordering> orderings = new ArrayList<>();
    final List<String> projections = new ArrayList<>();
    @Nullable Key startClusteringKey;
    boolean startInclusive;
    @Nullable Key endClusteringKey;
    boolean endInclusive;
    int limit = 0;
    @Nullable com.scalar.db.api.Consistency consistency;

    private BuildableScan(@Nullable String namespace, String table, Key partitionKey) {
      super(namespace, table, partitionKey);
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
    public Scan build() {
      Scan scan = new Scan(partitionKey);
      scan.forNamespace(namespaceName).forTable(tableName).withLimit(limit);
      orderings.forEach(scan::withOrdering);
      if (startClusteringKey != null) {
        scan.withStart(startClusteringKey, startInclusive);
      }
      if (endClusteringKey != null) {
        scan.withEnd(endClusteringKey, endInclusive);
      }

      if (!projections.isEmpty()) {
        scan.withProjections(projections);
      }

      if (consistency != null) {
        scan.withConsistency(consistency);
      }

      return scan;
    }
  }

  public static class BuildableScanWithIndex
      implements Consistency<BuildableScanWithIndex>,
          Projection<BuildableScanWithIndex>,
          Limit<BuildableScanWithIndex> {
    @Nullable private final String namespaceName;
    private final String tableName;
    private final Key indexKey;
    private final List<String> projections = new ArrayList<>();
    private int limit = 0;
    @Nullable private com.scalar.db.api.Consistency consistency;

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

    public Scan build() {
      Scan scan = new ScanWithIndex(indexKey);
      scan.forNamespace(namespaceName).forTable(tableName).withLimit(limit);

      if (!projections.isEmpty()) {
        scan.withProjections(projections);
      }

      if (consistency != null) {
        scan.withConsistency(consistency);
      }

      return scan;
    }
  }

  public static class BuildableScanAll
      implements Ordering<BuildableScanAll>,
          Consistency<BuildableScanAll>,
          Projection<BuildableScanAll>,
          Where<BuildableScanAllWithOngoingWhere>,
          WhereAnd<BuildableScanAllWithOngoingWhereAnd>,
          WhereOr<BuildableScanAllWithOngoingWhereOr>,
          Limit<BuildableScanAll> {
    private final String namespaceName;
    private final String tableName;
    private final List<Scan.Ordering> orderings = new ArrayList<>();
    private final List<String> projections = new ArrayList<>();
    private int limit = 0;
    @Nullable private com.scalar.db.api.Consistency consistency;

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

    public Scan build() {
      Scan scan = new ScanAll();
      scan.forNamespace(namespaceName).forTable(tableName).withLimit(limit);
      orderings.forEach(scan::withOrdering);

      if (!projections.isEmpty()) {
        scan.withProjections(projections);
      }

      if (consistency != null) {
        scan.withConsistency(consistency);
      }

      return scan;
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
      checkNotNull(super.condition);
      checkNotNull(condition);
      disjunctions.add(ImmutableSet.of(super.condition));
      disjunctions.add(ImmutableSet.of(condition));
      return new BuildableScanAllWithOngoingWhereAnd(this);
    }

    @Override
    public BuildableScanAllWithOngoingWhereAnd and(OrConditionSet orConditionSet) {
      checkNotNull(super.condition);
      checkNotNull(orConditionSet);
      disjunctions.add(ImmutableSet.of(super.condition));
      disjunctions.add(orConditionSet.getConditions());
      return new BuildableScanAllWithOngoingWhereAnd(this);
    }

    @Override
    public BuildableScanAllWithOngoingWhereOr or(ConditionalExpression condition) {
      checkNotNull(super.condition);
      checkNotNull(condition);
      conjunctions.add(ImmutableSet.of(super.condition));
      conjunctions.add(ImmutableSet.of(condition));
      return new BuildableScanAllWithOngoingWhereOr(this);
    }

    @Override
    public BuildableScanAllWithOngoingWhereOr or(AndConditionSet andConditionSet) {
      checkNotNull(super.condition);
      checkNotNull(andConditionSet);
      conjunctions.add(ImmutableSet.of(super.condition));
      conjunctions.add(andConditionSet.getConditions());
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
      disjunctions.add(orConditionSet.getConditions());
    }

    @Override
    public BuildableScanAllWithOngoingWhereAnd and(ConditionalExpression condition) {
      checkNotNull(condition);
      disjunctions.add(ImmutableSet.of(condition));
      return this;
    }

    @Override
    public BuildableScanAllWithOngoingWhereAnd and(OrConditionSet orConditionSet) {
      checkNotNull(orConditionSet);
      disjunctions.add(orConditionSet.getConditions());
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
      conjunctions.add(andConditionSet.getConditions());
    }

    @Override
    public BuildableScanAllWithOngoingWhereOr or(ConditionalExpression condition) {
      checkNotNull(condition);
      conjunctions.add(ImmutableSet.of(condition));
      return this;
    }

    @Override
    public BuildableScanAllWithOngoingWhereOr or(AndConditionSet andConditionSet) {
      checkNotNull(andConditionSet);
      conjunctions.add(andConditionSet.getConditions());
      return this;
    }
  }

  public static class BuildableScanAllWithWhere
      implements Consistency<BuildableScanAllWithWhere>,
          Projection<BuildableScanAllWithWhere>,
          Ordering<BuildableScanAllWithWhere>,
          Limit<BuildableScanAllWithWhere> {

    protected final String namespaceName;
    protected final String tableName;
    @Nullable protected final ConditionalExpression condition;
    protected final Set<Set<ConditionalExpression>> conjunctions = new HashSet<>();
    protected final Set<Set<ConditionalExpression>> disjunctions = new HashSet<>();
    protected final List<String> projections = new ArrayList<>();
    protected final List<Scan.Ordering> orderings = new ArrayList<>();
    protected int limit = 0;
    @Nullable protected com.scalar.db.api.Consistency consistency;

    private BuildableScanAllWithWhere(BuildableScanAll buildable) {
      this(buildable, null);
    }

    private BuildableScanAllWithWhere(
        BuildableScanAll buildable, @Nullable ConditionalExpression condition) {
      this.namespaceName = buildable.namespaceName;
      this.tableName = buildable.tableName;
      this.condition = condition;
      this.limit = buildable.limit;
      this.orderings.addAll(buildable.orderings);
      this.projections.addAll(buildable.projections);
      this.consistency = buildable.consistency;
    }

    private BuildableScanAllWithWhere(BuildableScanAllWithOngoingWhere buildable) {
      this.namespaceName = buildable.namespaceName;
      this.tableName = buildable.tableName;
      this.condition = null;
      this.conjunctions.addAll(buildable.conjunctions);
      this.disjunctions.addAll(buildable.disjunctions);
    }

    @Override
    public BuildableScanAllWithWhere projection(String projection) {
      checkNotNull(projection);
      projections.add(projection);
      return this;
    }

    @Override
    public BuildableScanAllWithWhere projections(Collection<String> projections) {
      checkNotNull(projections);
      this.projections.addAll(projections);
      return this;
    }

    @Override
    public BuildableScanAllWithWhere projections(String... projections) {
      return projections(Arrays.asList(projections));
    }

    @Override
    public BuildableScanAllWithWhere ordering(Scan.Ordering ordering) {
      checkNotNull(ordering);
      orderings.add(ordering);
      return this;
    }

    @Override
    public BuildableScanAllWithWhere orderings(Collection<Scan.Ordering> orderings) {
      checkNotNull(orderings);
      this.orderings.addAll(orderings);
      return this;
    }

    @Override
    public BuildableScanAllWithWhere orderings(Scan.Ordering... orderings) {
      return orderings(Arrays.asList(orderings));
    }

    @Override
    public BuildableScanAllWithWhere limit(int limit) {
      this.limit = limit;
      return this;
    }

    @Override
    public BuildableScanAllWithWhere consistency(com.scalar.db.api.Consistency consistency) {
      checkNotNull(consistency);
      this.consistency = consistency;
      return this;
    }

    public Scan build() {
      return buildScanAllWithConjunctions(
          namespaceName,
          tableName,
          condition,
          conjunctions,
          disjunctions,
          projections,
          orderings,
          limit,
          consistency);
    }
  }

  public static class BuildableScanOrScanAllFromExisting extends BuildableScan
      implements OperationBuilder.Namespace<BuildableScanOrScanAllFromExisting>,
          OperationBuilder.Table<BuildableScanOrScanAllFromExisting>,
          PartitionKey<BuildableScanOrScanAllFromExisting>,
          IndexKey<BuildableScanOrScanAllFromExisting>,
          Where<BuildableScanAllFromExistingWithOngoingWhere>,
          WhereAnd<BuildableScanAllFromExistingWithOngoingWhereAnd>,
          WhereOr<BuildableScanAllFromExistingWithOngoingWhereOr>,
          ClearConditions<BuildableScanOrScanAllFromExisting>,
          ClearProjections<BuildableScanOrScanAllFromExisting>,
          ClearOrderings<BuildableScanOrScanAllFromExisting>,
          ClearBoundaries<BuildableScanOrScanAllFromExisting>,
          ClearNamespace<BuildableScanOrScanAllFromExisting> {

    private final boolean isScanWithIndex;
    private final boolean isScanAll;
    private Key indexKey;
    protected final Set<Set<ConditionalExpression>> conjunctions = new HashSet<>();

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
    public BuildableScanAllFromExistingWithOngoingWhere where(ConditionalExpression condition) {
      checkScanAll();
      checkConditionsEmpty();
      checkNotNull(condition);
      return new BuildableScanAllFromExistingWithOngoingWhere(this, condition);
    }

    @Override
    public BuildableScanAllFromExistingWithOngoingWhereAnd where(OrConditionSet orConditionSet) {
      checkScanAll();
      checkConditionsEmpty();
      checkNotNull(orConditionSet);
      return new BuildableScanAllFromExistingWithOngoingWhereAnd(this, orConditionSet);
    }

    @Override
    public BuildableScanAllFromExistingWithOngoingWhereOr where(AndConditionSet andConditionSet) {
      checkScanAll();
      checkConditionsEmpty();
      checkNotNull(andConditionSet);
      return new BuildableScanAllFromExistingWithOngoingWhereOr(this, andConditionSet);
    }

    @Override
    public BuildableScanOrScanAllFromExisting clearStart() {
      checkNotScanWithIndexOrScanAll();
      this.startClusteringKey = null;
      return this;
    }

    @Override
    public BuildableScanOrScanAllFromExisting clearEnd() {
      checkNotScanWithIndexOrScanAll();
      this.endClusteringKey = null;
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
      checkScanAll();
      this.conjunctions.clear();
      return this;
    }

    @Override
    public BuildableScanOrScanAllFromExisting clearNamespace() {
      this.namespaceName = null;
      return this;
    }

    private void checkNotScanWithIndexOrScanAll() {
      if (isScanWithIndex || isScanAll) {
        throw new UnsupportedOperationException(
            "This operation is not supported when scanning all the records of a database "
                + "or scanning records of a database using a secondary index.");
      }
    }

    private void checkScanWithIndex() {
      if (!isScanWithIndex) {
        throw new UnsupportedOperationException(
            "This operation is supported only when scanning records of a database using a secondary index.");
      }
    }

    private void checkNotScanWithIndex() {
      if (isScanWithIndex) {
        throw new UnsupportedOperationException(
            "This operation is not supported when scanning records of a database using a secondary index.");
      }
    }

    private void checkScanAll() {
      if (!isScanAll) {
        throw new UnsupportedOperationException(
            "This operation is supported only when scanning all the records of a database.");
      }
    }

    private void checkConditionsEmpty() {
      if (!conjunctions.isEmpty()) {
        throw new IllegalStateException(
            "This operation is supported only when no conditions are specified at all. "
                + "If you want to modify the condition, please use clearConditions() to remove all existing conditions first.");
      }
    }

    @Override
    public Scan build() {
      Scan scan;

      if (isScanWithIndex) {
        scan = new ScanWithIndex(indexKey);
      } else if (isScanAll) {
        scan = new ScanAll();
        orderings.forEach(scan::withOrdering);
        if (!conjunctions.isEmpty()) {
          scan.withConjunctions(
              conjunctions.stream().map(Conjunction::of).collect(Collectors.toSet()));
        }
      } else {
        scan = new Scan(partitionKey);
        orderings.forEach(scan::withOrdering);
        if (startClusteringKey != null) {
          scan.withStart(startClusteringKey, startInclusive);
        }
        if (endClusteringKey != null) {
          scan.withEnd(endClusteringKey, endInclusive);
        }
      }

      scan.forNamespace(namespaceName)
          .forTable(tableName)
          .withLimit(limit)
          .withConsistency(consistency);
      if (!projections.isEmpty()) {
        scan.withProjections(projections);
      }

      return scan;
    }
  }

  public static class BuildableScanAllFromExistingWithWhere
      implements OperationBuilder.Namespace<BuildableScanAllFromExistingWithWhere>,
          OperationBuilder.Table<BuildableScanAllFromExistingWithWhere>,
          Consistency<BuildableScanAllFromExistingWithWhere>,
          Projection<BuildableScanAllFromExistingWithWhere>,
          Ordering<BuildableScanAllFromExistingWithWhere>,
          Limit<BuildableScanAllFromExistingWithWhere>,
          ClearProjections<BuildableScanAllFromExistingWithWhere>,
          ClearOrderings<BuildableScanAllFromExistingWithWhere>,
          ClearNamespace<BuildableScanAllFromExistingWithWhere> {

    @Nullable protected String namespaceName;
    protected String tableName;
    @Nullable protected final ConditionalExpression condition;
    protected final Set<Set<ConditionalExpression>> conjunctions = new HashSet<>();
    protected final Set<Set<ConditionalExpression>> disjunctions = new HashSet<>();
    protected final List<String> projections = new ArrayList<>();
    protected final List<Scan.Ordering> orderings = new ArrayList<>();
    protected int limit;
    @Nullable protected com.scalar.db.api.Consistency consistency;

    private BuildableScanAllFromExistingWithWhere(BuildableScanAllFromExistingWithWhere buildable) {
      this.namespaceName = buildable.namespaceName;
      this.tableName = buildable.tableName;
      this.condition = null;
      this.conjunctions.addAll(buildable.conjunctions);
      this.disjunctions.addAll(buildable.disjunctions);
      this.limit = buildable.limit;
      this.orderings.addAll(buildable.orderings);
      this.projections.addAll(buildable.projections);
      this.consistency = buildable.consistency;
    }

    private BuildableScanAllFromExistingWithWhere(
        BuildableScanOrScanAllFromExisting buildable, @Nullable ConditionalExpression condition) {
      this.namespaceName = buildable.namespaceName;
      this.tableName = buildable.tableName;
      this.condition = condition;
      this.limit = buildable.limit;
      this.orderings.addAll(buildable.orderings);
      this.projections.addAll(buildable.projections);
      this.consistency = buildable.consistency;
    }

    @Override
    public BuildableScanAllFromExistingWithWhere namespace(String namespaceName) {
      checkNotNull(namespaceName);
      this.namespaceName = namespaceName;
      return this;
    }

    @Override
    public BuildableScanAllFromExistingWithWhere table(String tableName) {
      checkNotNull(tableName);
      this.tableName = tableName;
      return this;
    }

    @Override
    public BuildableScanAllFromExistingWithWhere projection(String projection) {
      checkNotNull(projection);
      projections.add(projection);
      return this;
    }

    @Override
    public BuildableScanAllFromExistingWithWhere projections(Collection<String> projections) {
      checkNotNull(projections);
      this.projections.addAll(projections);
      return this;
    }

    @Override
    public BuildableScanAllFromExistingWithWhere projections(String... projections) {
      return projections(Arrays.asList(projections));
    }

    @Override
    public BuildableScanAllFromExistingWithWhere ordering(Scan.Ordering ordering) {
      checkNotNull(ordering);
      orderings.add(ordering);
      return this;
    }

    @Override
    public BuildableScanAllFromExistingWithWhere orderings(Collection<Scan.Ordering> orderings) {
      checkNotNull(orderings);
      this.orderings.addAll(orderings);
      return this;
    }

    @Override
    public BuildableScanAllFromExistingWithWhere orderings(Scan.Ordering... orderings) {
      return orderings(Arrays.asList(orderings));
    }

    @Override
    public BuildableScanAllFromExistingWithWhere limit(int limit) {
      this.limit = limit;
      return this;
    }

    @Override
    public BuildableScanAllFromExistingWithWhere consistency(
        com.scalar.db.api.Consistency consistency) {
      checkNotNull(consistency);
      this.consistency = consistency;
      return this;
    }

    @Override
    public BuildableScanAllFromExistingWithWhere clearProjections() {
      this.projections.clear();
      return this;
    }

    @Override
    public BuildableScanAllFromExistingWithWhere clearOrderings() {
      this.orderings.clear();
      return this;
    }

    @Override
    public BuildableScanAllFromExistingWithWhere clearNamespace() {
      this.namespaceName = null;
      return this;
    }

    public Scan build() {
      return buildScanAllWithConjunctions(
          namespaceName,
          tableName,
          condition,
          conjunctions,
          disjunctions,
          projections,
          orderings,
          limit,
          consistency);
    }
  }

  public static class BuildableScanAllFromExistingWithOngoingWhere
      extends BuildableScanAllFromExistingWithWhere
      implements And<BuildableScanAllFromExistingWithOngoingWhereAnd>,
          Or<BuildableScanAllFromExistingWithOngoingWhereOr> {

    private BuildableScanAllFromExistingWithOngoingWhere(
        BuildableScanOrScanAllFromExisting buildable) {
      super(buildable, null);
    }

    private BuildableScanAllFromExistingWithOngoingWhere(
        BuildableScanOrScanAllFromExisting buildable, ConditionalExpression condition) {
      super(buildable, condition);
    }

    private BuildableScanAllFromExistingWithOngoingWhere(
        BuildableScanAllFromExistingWithOngoingWhere buildable) {
      super(buildable);
    }

    @Override
    public BuildableScanAllFromExistingWithOngoingWhereAnd and(ConditionalExpression condition) {
      checkNotNull(this.condition);
      checkNotNull(condition);
      disjunctions.add(ImmutableSet.of(this.condition));
      disjunctions.add(ImmutableSet.of(condition));
      return new BuildableScanAllFromExistingWithOngoingWhereAnd(this);
    }

    @Override
    public BuildableScanAllFromExistingWithOngoingWhereAnd and(OrConditionSet orConditionSet) {
      checkNotNull(this.condition);
      checkNotNull(orConditionSet);
      disjunctions.add(ImmutableSet.of(this.condition));
      disjunctions.add(orConditionSet.getConditions());
      return new BuildableScanAllFromExistingWithOngoingWhereAnd(this);
    }

    @Override
    public BuildableScanAllFromExistingWithOngoingWhereOr or(ConditionalExpression condition) {
      checkNotNull(this.condition);
      checkNotNull(condition);
      conjunctions.add(ImmutableSet.of(this.condition));
      conjunctions.add(ImmutableSet.of(condition));
      return new BuildableScanAllFromExistingWithOngoingWhereOr(this);
    }

    @Override
    public BuildableScanAllFromExistingWithOngoingWhereOr or(AndConditionSet andConditionSet) {
      checkNotNull(this.condition);
      checkNotNull(andConditionSet);
      conjunctions.add(ImmutableSet.of(this.condition));
      conjunctions.add(andConditionSet.getConditions());
      return new BuildableScanAllFromExistingWithOngoingWhereOr(this);
    }
  }

  public static class BuildableScanAllFromExistingWithOngoingWhereOr
      extends BuildableScanAllFromExistingWithOngoingWhere
      implements Or<BuildableScanAllFromExistingWithOngoingWhereOr> {

    private BuildableScanAllFromExistingWithOngoingWhereOr(
        BuildableScanAllFromExistingWithOngoingWhere buildable) {
      super(buildable);
    }

    private BuildableScanAllFromExistingWithOngoingWhereOr(
        BuildableScanOrScanAllFromExisting buildable, AndConditionSet andConditionSet) {
      super(buildable);
      conjunctions.add(andConditionSet.getConditions());
    }

    @Override
    public BuildableScanAllFromExistingWithOngoingWhereOr or(ConditionalExpression condition) {
      checkNotNull(condition);
      conjunctions.add(ImmutableSet.of(condition));
      return this;
    }

    @Override
    public BuildableScanAllFromExistingWithOngoingWhereOr or(AndConditionSet andConditionSet) {
      checkNotNull(andConditionSet);
      conjunctions.add(andConditionSet.getConditions());
      return this;
    }
  }

  public static class BuildableScanAllFromExistingWithOngoingWhereAnd
      extends BuildableScanAllFromExistingWithOngoingWhere
      implements And<BuildableScanAllFromExistingWithOngoingWhereAnd> {

    private BuildableScanAllFromExistingWithOngoingWhereAnd(
        BuildableScanAllFromExistingWithOngoingWhere buildable) {
      super(buildable);
    }

    private BuildableScanAllFromExistingWithOngoingWhereAnd(
        BuildableScanOrScanAllFromExisting buildable, OrConditionSet orConditionSet) {
      super(buildable);
      disjunctions.add(orConditionSet.getConditions());
    }

    @Override
    public BuildableScanAllFromExistingWithOngoingWhereAnd and(ConditionalExpression condition) {
      checkNotNull(condition);
      disjunctions.add(ImmutableSet.of(condition));
      return this;
    }

    @Override
    public BuildableScanAllFromExistingWithOngoingWhereAnd and(OrConditionSet orConditionSet) {
      checkNotNull(orConditionSet);
      disjunctions.add(orConditionSet.getConditions());
      return this;
    }
  }

  /**
   * An and-wise set of {@link ConditionalExpression} used for specifying arbitrary conditions in a
   * {@link Scan} command.
   */
  @Immutable
  public static class AndConditionSet {
    private final ImmutableSet<ConditionalExpression> conditions;

    private AndConditionSet(ImmutableSet<ConditionalExpression> conditions) {
      this.conditions = conditions;
    }

    /**
     * Returns the set of {@code ConditionalExpression}.
     *
     * @return set of {@code ConditionalExpression}
     */
    public Set<ConditionalExpression> getConditions() {
      return conditions;
    }

    /**
     * Indicates whether some other object is "equal to" this object. The other object is considered
     * equal if:
     *
     * <ul>
     *   <li>it is also an {@code AndConditionSet}
     *   <li>both instances have the same set of {@code ConditionalExpression}
     * </ul>
     *
     * @param o an object to be tested for equality
     * @return {@code true} if the other object is "equal to" this object otherwise {@code false}
     */
    @Override
    public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (!(o instanceof AndConditionSet)) {
        return false;
      }
      AndConditionSet other = (AndConditionSet) o;
      return conditions.equals(other.conditions);
    }

    @Override
    public int hashCode() {
      return Objects.hash(conditions);
    }

    @Override
    public String toString() {
      return conditions.toString();
    }
  }

  /**
   * An or-wise set of {@link ConditionalExpression} used for specifying arbitrary conditions in a
   * {@link Scan} command.
   */
  @Immutable
  public static class OrConditionSet {
    private final ImmutableSet<ConditionalExpression> conditions;

    private OrConditionSet(ImmutableSet<ConditionalExpression> conditions) {
      this.conditions = conditions;
    }

    /**
     * Returns the set of {@code ConditionalExpression}.
     *
     * @return set of {@code ConditionalExpression}
     */
    public Set<ConditionalExpression> getConditions() {
      return conditions;
    }

    /**
     * Indicates whether some other object is "equal to" this object. The other object is considered
     * equal if:
     *
     * <ul>
     *   <li>it is also an {@code OrConditionSet}
     *   <li>both instances have the same set of {@code ConditionalExpression}
     * </ul>
     *
     * @param o an object to be tested for equality
     * @return {@code true} if the other object is "equal to" this object otherwise {@code false}
     */
    @Override
    public boolean equals(Object o) {
      if (o == this) {
        return true;
      }
      if (!(o instanceof OrConditionSet)) {
        return false;
      }
      OrConditionSet other = (OrConditionSet) o;
      return conditions.equals(other.conditions);
    }

    @Override
    public int hashCode() {
      return Objects.hash(conditions);
    }

    @Override
    public String toString() {
      return conditions.toString();
    }
  }

  public static class ConditionSetBuilder {

    /**
     * Returns a builder object to build a {@code AndConditionSet} or {@code OrConditionSet}.
     *
     * @param condition a conditional expression to build a {@code AndConditionSet} or {@code
     *     OrConditionSet}.
     * @return a builder object
     */
    public static AndOrConditionSetBuilder condition(ConditionalExpression condition) {
      return new AndOrConditionSetBuilder(ImmutableSet.of(condition));
    }

    /**
     * Returns a builder object to build a {@code AndConditionSet}.
     *
     * @param conditions a set of conditional expressions to build a {@code AndConditionSet}.
     * @return a builder object
     */
    public static BuildableAndConditionSet andConditionSet(Set<ConditionalExpression> conditions) {
      return new BuildableAndConditionSet(conditions);
    }

    /**
     * Returns a builder object to build a {@code OrConditionSet}.
     *
     * @param conditions a set of conditional expressions to build a {@code OrConditionSet}.
     * @return a builder object
     */
    public static BuildableOrConditionSet orConditionSet(Set<ConditionalExpression> conditions) {
      return new BuildableOrConditionSet(conditions);
    }
  }

  public static class AndOrConditionSetBuilder {

    private final Set<ConditionalExpression> conditions;

    private AndOrConditionSetBuilder(Set<ConditionalExpression> conditions) {
      this.conditions = new HashSet<>();
      this.conditions.addAll(conditions);
    }

    /**
     * Adds a conditional expression for a {@code AndConditionSet}.
     *
     * @param condition a conditional expression for a {@code AndConditionSet}.
     * @return a builder object
     */
    public BuildableAndConditionSet and(ConditionalExpression condition) {
      conditions.add(condition);
      return new BuildableAndConditionSet(conditions);
    }

    /**
     * Adds a conditional expression for a {@code OrConditionSet}.
     *
     * @param condition a conditional expression for a {@code OrConditionSet}.
     * @return a builder object
     */
    public BuildableOrConditionSet or(ConditionalExpression condition) {
      conditions.add(condition);
      return new BuildableOrConditionSet(conditions);
    }
  }

  public static class BuildableAndConditionSet {

    private final Set<ConditionalExpression> conditions;

    private BuildableAndConditionSet(Set<ConditionalExpression> conditions) {
      this.conditions = new HashSet<>();
      this.conditions.addAll(conditions);
    }

    /**
     * Adds a conditional expression for a {@code AndConditionSet}.
     *
     * @param condition a conditional expression for a {@code AndConditionSet}.
     * @return a builder object
     */
    public BuildableAndConditionSet and(ConditionalExpression condition) {
      conditions.add(condition);
      return this;
    }

    /**
     * Builds an and-wise condition set with the specified conditional expressions.
     *
     * @return an and-wise condition set
     */
    public AndConditionSet build() {
      return new AndConditionSet(ImmutableSet.copyOf(conditions));
    }
  }

  public static class BuildableOrConditionSet {

    private final Set<ConditionalExpression> conditions;

    private BuildableOrConditionSet(Set<ConditionalExpression> conditions) {
      this.conditions = new HashSet<>();
      this.conditions.addAll(conditions);
    }

    /**
     * Adds a conditional expression for a {@code OrConditionSet}.
     *
     * @param condition a conditional expression for a {@code OrConditionSet}.
     * @return a builder object
     */
    public BuildableOrConditionSet or(ConditionalExpression condition) {
      conditions.add(condition);
      return this;
    }

    /**
     * Builds an or-wise condition set with the specified conditional expressions.
     *
     * @return an or-wise condition set
     */
    public OrConditionSet build() {
      return new OrConditionSet(ImmutableSet.copyOf(conditions));
    }
  }

  private static Scan buildScanAllWithConjunctions(
      String namespaceName,
      String tableName,
      @Nullable ConditionalExpression condition,
      Set<Set<ConditionalExpression>> conjunctions,
      Set<Set<ConditionalExpression>> disjunctions,
      List<String> projections,
      List<Scan.Ordering> orderings,
      int limit,
      @Nullable com.scalar.db.api.Consistency consistency) {
    ScanAll scan = new ScanAll();
    scan.forNamespace(namespaceName).forTable(tableName).withLimit(limit);
    orderings.forEach(scan::withOrdering);

    if (condition != null) {
      assert conjunctions.isEmpty() && disjunctions.isEmpty();
      scan.withConjunctions(ImmutableSet.of(Scan.Conjunction.of(condition)));
    } else if (conjunctions.isEmpty()) {
      scan.withConjunctions(
          Sets.cartesianProduct(new ArrayList<>(disjunctions)).stream()
              .filter(conditions -> conditions.size() > 0)
              .map(Scan.Conjunction::of)
              .collect(Collectors.toSet()));
    } else {
      scan.withConjunctions(
          conjunctions.stream()
              .filter(conditions -> conditions.size() > 0)
              .map(Conjunction::of)
              .collect(Collectors.toSet()));
    }

    if (!projections.isEmpty()) {
      scan.withProjections(projections);
    }

    if (consistency != null) {
      scan.withConsistency(consistency);
    }

    return scan;
  }
}
