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
      where.and(condition);
      return this;
    }

    @Override
    public BuildableScanAllWithOngoingWhereAnd and(OrConditionSet orConditionSet) {
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
      where.or(condition);
      return this;
    }

    @Override
    public BuildableScanAllWithOngoingWhereOr or(AndConditionSet andConditionSet) {
      where.or(andConditionSet);
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
    protected final OngoingWhere where;
    protected final List<String> projections = new ArrayList<>();
    protected final List<Scan.Ordering> orderings = new ArrayList<>();
    protected int limit;
    @Nullable protected com.scalar.db.api.Consistency consistency;

    private BuildableScanAllWithWhere(BuildableScanAll buildable) {
      this(buildable, null);
    }

    private BuildableScanAllWithWhere(
        BuildableScanAll buildable, @Nullable ConditionalExpression condition) {
      this.namespaceName = buildable.namespaceName;
      this.tableName = buildable.tableName;
      this.where = new OngoingWhere(condition);
      this.limit = buildable.limit;
      this.orderings.addAll(buildable.orderings);
      this.projections.addAll(buildable.projections);
      this.consistency = buildable.consistency;
    }

    private BuildableScanAllWithWhere(BuildableScanAllWithOngoingWhere buildable) {
      this.namespaceName = buildable.namespaceName;
      this.tableName = buildable.tableName;
      this.where = buildable.where;
      this.limit = buildable.limit;
      this.projections.addAll(buildable.projections);
      this.orderings.addAll(buildable.orderings);
      this.consistency = buildable.consistency;
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
          namespaceName, tableName, where, projections, orderings, limit, consistency);
    }
  }

  public static class BuildableScanOrScanAllFromExisting extends BuildableScan
      implements OperationBuilder.Namespace<BuildableScanOrScanAllFromExisting>,
          OperationBuilder.Table<BuildableScanOrScanAllFromExisting>,
          PartitionKey<BuildableScanOrScanAllFromExisting>,
          IndexKey<BuildableScanOrScanAllFromExisting>,
          Where<BuildableScanFromExistingWithOngoingWhere>,
          WhereAnd<BuildableScanFromExistingWithOngoingWhereAnd>,
          WhereOr<BuildableScanFromExistingWithOngoingWhereOr>,
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
    public BuildableScanFromExistingWithOngoingWhere where(ConditionalExpression condition) {
      checkScanAll();
      checkConditionsEmpty();
      checkNotNull(condition);
      return new BuildableScanFromExistingWithOngoingWhere(this, condition);
    }

    @Override
    public BuildableScanFromExistingWithOngoingWhereAnd where(OrConditionSet orConditionSet) {
      checkScanAll();
      checkConditionsEmpty();
      checkNotNull(orConditionSet);
      return new BuildableScanFromExistingWithOngoingWhereAnd(this, orConditionSet);
    }

    @Override
    public BuildableScanFromExistingWithOngoingWhereOr where(AndConditionSet andConditionSet) {
      checkScanAll();
      checkConditionsEmpty();
      checkNotNull(andConditionSet);
      return new BuildableScanFromExistingWithOngoingWhereOr(this, andConditionSet);
    }

    @Override
    public BuildableScanFromExistingWithOngoingWhereAnd whereAnd(
        Set<OrConditionSet> orConditionSets) {
      checkScanAll();
      checkConditionsEmpty();
      checkNotNull(orConditionSets);
      return new BuildableScanFromExistingWithOngoingWhereAnd(this, orConditionSets);
    }

    @Override
    public BuildableScanFromExistingWithOngoingWhereOr whereOr(
        Set<AndConditionSet> andConditionSets) {
      checkScanAll();
      checkConditionsEmpty();
      return new BuildableScanFromExistingWithOngoingWhereOr(this, andConditionSets);
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

    private void checkScanAll() {
      if (!isScanAll) {
        throw new UnsupportedOperationException(
            CoreError
                .SCAN_BUILD_ERROR_OPERATION_SUPPORTED_ONLY_WHEN_SCANNING_ALL_RECORDS_OF_DATABASE
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

  public static class BuildableScanFromExistingWithWhere
      implements OperationBuilder.Namespace<BuildableScanFromExistingWithWhere>,
          OperationBuilder.Table<BuildableScanFromExistingWithWhere>,
          Consistency<BuildableScanFromExistingWithWhere>,
          Projection<BuildableScanFromExistingWithWhere>,
          Ordering<BuildableScanFromExistingWithWhere>,
          Limit<BuildableScanFromExistingWithWhere>,
          ClearProjections<BuildableScanFromExistingWithWhere>,
          ClearOrderings<BuildableScanFromExistingWithWhere>,
          ClearNamespace<BuildableScanFromExistingWithWhere> {

    BuildableScanOrScanAllFromExisting buildableScanFromExisting;
    protected final OngoingWhere where;

    private BuildableScanFromExistingWithWhere(BuildableScanFromExistingWithWhere buildable) {
      this.buildableScanFromExisting = buildable.buildableScanFromExisting;
      this.where = buildable.where;
    }

    private BuildableScanFromExistingWithWhere(
        BuildableScanOrScanAllFromExisting buildable, @Nullable ConditionalExpression condition) {
      this.buildableScanFromExisting = buildable;
      this.where = new OngoingWhere(condition);
    }

    private BuildableScanFromExistingWithWhere(BuildableScanOrScanAllFromExisting buildable) {
      this(buildable, null);
    }

    @Override
    public BuildableScanFromExistingWithWhere namespace(String namespaceName) {
      buildableScanFromExisting = buildableScanFromExisting.namespace(namespaceName);
      return this;
    }

    @Override
    public BuildableScanFromExistingWithWhere table(String tableName) {
      buildableScanFromExisting = buildableScanFromExisting.table(tableName);
      return this;
    }

    @Override
    public BuildableScanFromExistingWithWhere projection(String projection) {
      buildableScanFromExisting = buildableScanFromExisting.projections(projection);
      return this;
    }

    @Override
    public BuildableScanFromExistingWithWhere projections(Collection<String> projections) {
      buildableScanFromExisting = buildableScanFromExisting.projections(projections);
      return this;
    }

    @Override
    public BuildableScanFromExistingWithWhere projections(String... projections) {
      return projections(Arrays.asList(projections));
    }

    @Override
    public BuildableScanFromExistingWithWhere ordering(Scan.Ordering ordering) {
      buildableScanFromExisting = buildableScanFromExisting.ordering(ordering);
      return this;
    }

    @Override
    public BuildableScanFromExistingWithWhere orderings(Collection<Scan.Ordering> orderings) {
      buildableScanFromExisting = buildableScanFromExisting.orderings(orderings);
      return this;
    }

    @Override
    public BuildableScanFromExistingWithWhere orderings(Scan.Ordering... orderings) {
      return orderings(Arrays.asList(orderings));
    }

    @Override
    public BuildableScanFromExistingWithWhere limit(int limit) {
      buildableScanFromExisting = buildableScanFromExisting.limit(limit);
      return this;
    }

    @Override
    public BuildableScanFromExistingWithWhere consistency(
        com.scalar.db.api.Consistency consistency) {
      buildableScanFromExisting = buildableScanFromExisting.consistency(consistency);
      return this;
    }

    @Override
    public BuildableScanFromExistingWithWhere clearProjections() {
      buildableScanFromExisting = buildableScanFromExisting.clearProjections();
      return this;
    }

    @Override
    public BuildableScanFromExistingWithWhere clearOrderings() {
      buildableScanFromExisting = buildableScanFromExisting.clearOrderings();
      return this;
    }

    @Override
    public BuildableScanFromExistingWithWhere clearNamespace() {
      buildableScanFromExisting = buildableScanFromExisting.clearNamespace();
      return this;
    }

    public Scan build() {
      return addConjunctionsTo(buildableScanFromExisting.build(), where);
    }
  }

  public static class BuildableScanFromExistingWithOngoingWhere
      extends BuildableScanFromExistingWithWhere
      implements And<BuildableScanFromExistingWithOngoingWhereAnd>,
          Or<BuildableScanFromExistingWithOngoingWhereOr> {

    private BuildableScanFromExistingWithOngoingWhere(
        BuildableScanOrScanAllFromExisting buildable) {
      super(buildable);
    }

    private BuildableScanFromExistingWithOngoingWhere(
        BuildableScanOrScanAllFromExisting buildable, ConditionalExpression condition) {
      super(buildable, condition);
    }

    private BuildableScanFromExistingWithOngoingWhere(
        BuildableScanFromExistingWithOngoingWhere buildable) {
      super(buildable);
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
      extends BuildableScanFromExistingWithOngoingWhere
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
      extends BuildableScanFromExistingWithOngoingWhere
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

  private static class OngoingWhere {

    @Nullable private ConditionalExpression condition;
    private final Set<Set<ConditionalExpression>> conjunctions = new HashSet<>();
    private final Set<Set<ConditionalExpression>> disjunctions = new HashSet<>();

    private OngoingWhere(ConditionalExpression condition) {
      this.condition = condition;
    }

    private void and(ConditionalExpression condition) {
      checkNotNull(condition);
      if (this.condition != null) {
        disjunctions.add(ImmutableSet.of(this.condition));
        this.condition = null;
      }
      disjunctions.add(ImmutableSet.of(condition));
    }

    private void and(OrConditionSet orConditionSet) {
      checkNotNull(orConditionSet);
      if (this.condition != null) {
        disjunctions.add(ImmutableSet.of(this.condition));
        this.condition = null;
      }
      disjunctions.add(orConditionSet.getConditions());
    }

    private void and(Set<OrConditionSet> orConditionSets) {
      disjunctions.addAll(
          orConditionSets.stream().map(OrConditionSet::getConditions).collect(Collectors.toSet()));
    }

    private void or(ConditionalExpression condition) {
      if (this.condition != null) {
        conjunctions.add(ImmutableSet.of(this.condition));
        this.condition = null;
      }
      conjunctions.add(ImmutableSet.of(condition));
    }

    private void or(AndConditionSet andConditionSet) {
      if (this.condition != null) {
        conjunctions.add(ImmutableSet.of(this.condition));
        this.condition = null;
      }
      conjunctions.add(andConditionSet.getConditions());
    }

    private void or(Set<AndConditionSet> andConditionSets) {
      conjunctions.addAll(
          andConditionSets.stream()
              .map(AndConditionSet::getConditions)
              .collect(Collectors.toSet()));
    }
  }

  private static Scan buildScanAllWithConjunctions(
      String namespaceName,
      String tableName,
      OngoingWhere where,
      List<String> projections,
      List<Scan.Ordering> orderings,
      int limit,
      @Nullable com.scalar.db.api.Consistency consistency) {
    ScanAll scan = new ScanAll();
    scan.forNamespace(namespaceName).forTable(tableName).withLimit(limit);
    orderings.forEach(scan::withOrdering);

    if (!projections.isEmpty()) {
      scan.withProjections(projections);
    }

    if (consistency != null) {
      scan.withConsistency(consistency);
    }

    return addConjunctionsTo(scan, where);
  }

  private static Scan addConjunctionsTo(Scan scan, OngoingWhere where) {

    if (where.condition != null) {
      assert where.conjunctions.isEmpty() && where.disjunctions.isEmpty();
      scan.withConjunctions(ImmutableSet.of(Conjunction.of(where.condition)));
    } else if (where.conjunctions.isEmpty()) {
      scan.withConjunctions(
          Sets.cartesianProduct(new ArrayList<>(where.disjunctions)).stream()
              .filter(conditions -> conditions.size() > 0)
              .map(Conjunction::of)
              .collect(Collectors.toSet()));
    } else {
      scan.withConjunctions(
          where.conjunctions.stream()
              .filter(conditions -> conditions.size() > 0)
              .map(Conjunction::of)
              .collect(Collectors.toSet()));
    }

    return scan;
  }
}
