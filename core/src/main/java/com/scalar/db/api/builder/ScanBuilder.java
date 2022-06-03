package com.scalar.db.api.builder;

import static com.google.common.base.Preconditions.checkNotNull;

import com.scalar.db.api.Scan;
import com.scalar.db.api.ScanAll;
import com.scalar.db.api.builder.OperationBuilder.All;
import com.scalar.db.api.builder.OperationBuilder.ClearOrderings;
import com.scalar.db.api.builder.OperationBuilder.ClearProjections;
import com.scalar.db.api.builder.OperationBuilder.ClusteringKeyFiltering;
import com.scalar.db.api.builder.OperationBuilder.Consistency;
import com.scalar.db.api.builder.OperationBuilder.Limit;
import com.scalar.db.api.builder.OperationBuilder.Ordering;
import com.scalar.db.api.builder.OperationBuilder.PartitionKeyBuilder;
import com.scalar.db.api.builder.OperationBuilder.Projection;
import com.scalar.db.api.builder.OperationBuilder.TableBuilder;
import com.scalar.db.io.Key;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;

public class ScanBuilder {

  public static class Namespace implements OperationBuilder.Namespace<Table> {
    @Override
    public Table namespace(String namespaceName) {
      checkNotNull(namespaceName);
      return new Table(namespaceName);
    }
  }

  public static class Table extends TableBuilder<PartitionKeyOrAll> {

    public Table(String namespaceName) {
      super(namespaceName);
    }

    @Override
    public PartitionKeyOrAll table(String tableName) {
      checkNotNull(tableName);
      return new PartitionKeyOrAll(namespace, tableName);
    }
  }

  public static class PartitionKeyOrAll extends PartitionKeyBuilder<BuildableScan>
      implements All<BuildableScanAll> {
    public PartitionKeyOrAll(String namespace, String table) {
      super(namespace, table);
    }

    @Override
    public BuildableScan partitionKey(Key partitionKey) {
      checkNotNull(partitionKey);
      return new BuildableScan(namespaceName, tableName, partitionKey);
    }

    @Override
    public BuildableScanAll all() {
      return new BuildableScanAll(namespaceName, tableName);
    }
  }

  public static class BuildableScan extends OperationBuilder.Buildable<Scan>
      implements ClusteringKeyFiltering<BuildableScan>,
          Ordering<BuildableScan>,
          Consistency<BuildableScan>,
          Projection<BuildableScan>,
          Limit<BuildableScan> {
    private final List<Scan.Ordering> orderings = new ArrayList<>();
    private final List<String> projections = new ArrayList<>();
    @Nullable private Key startClusteringKey;
    private boolean startInclusive;
    @Nullable private Key endClusteringKey;
    private boolean endInclusive;
    private int limit = 0;
    @Nullable private com.scalar.db.api.Consistency consistency;

    public BuildableScan(String namespace, String table, Key partitionKey) {
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

  public static class BuildableScanFromExisting
      implements OperationBuilder.Namespace<BuildableScanFromExisting>,
          OperationBuilder.Table<BuildableScanFromExisting>,
          OperationBuilder.PartitionKey<BuildableScanFromExisting>,
          ClusteringKeyFiltering<BuildableScanFromExisting>,
          Ordering<BuildableScanFromExisting>,
          ClearOrderings<BuildableScanFromExisting>,
          Limit<BuildableScanFromExisting>,
          Projection<BuildableScanFromExisting>,
          ClearProjections<BuildableScanFromExisting>,
          Consistency<BuildableScanFromExisting> {
    private final List<String> projections = new ArrayList<>();
    private final List<Scan.Ordering> orderings = new ArrayList<>();
    @Nullable private String namespaceName;
    @Nullable private String tableName;
    private Key partitionKey;
    private com.scalar.db.api.Consistency consistency;
    @Nullable private Key startClusteringKey;
    private boolean startInclusive;
    @Nullable private Key endClusteringKey;
    private boolean endInclusive;
    private int limit;

    public BuildableScanFromExisting(Scan scan) {
      this.namespaceName = scan.forNamespace().orElse(null);
      this.tableName = scan.forTable().orElse(null);
      this.partitionKey = scan.getPartitionKey();
      scan.getStartClusteringKey()
          .ifPresent(
              key -> {
                this.startClusteringKey = key;
                this.startInclusive = scan.getStartInclusive();
              });
      scan.getEndClusteringKey()
          .ifPresent(
              key -> {
                this.endClusteringKey = key;
                this.endInclusive = scan.getEndInclusive();
              });
      this.limit = scan.getLimit();
      this.orderings.addAll(scan.getOrderings());
      this.projections.addAll(scan.getProjections());
      this.consistency = scan.getConsistency();
    }

    @Override
    public BuildableScanFromExisting namespace(String namespaceName) {
      checkNotNull(namespaceName);
      this.namespaceName = namespaceName;
      return this;
    }

    @Override
    public BuildableScanFromExisting table(String tableName) {
      checkNotNull(tableName);
      this.tableName = tableName;
      return this;
    }

    @Override
    public BuildableScanFromExisting partitionKey(Key partitionKey) {
      checkNotNull(partitionKey);
      this.partitionKey = partitionKey;
      return this;
    }

    @Override
    public BuildableScanFromExisting consistency(com.scalar.db.api.Consistency consistency) {
      checkNotNull(consistency);
      this.consistency = consistency;
      return this;
    }

    @Override
    public BuildableScanFromExisting projection(String projection) {
      checkNotNull(projection);
      this.projections.add(projection);
      return this;
    }

    @Override
    public BuildableScanFromExisting projections(Collection<String> projections) {
      checkNotNull(projections);
      this.projections.addAll(projections);
      return this;
    }

    @Override
    public BuildableScanFromExisting clearProjections() {
      this.projections.clear();
      return this;
    }

    @Override
    public BuildableScanFromExisting limit(int limit) {
      this.limit = limit;
      return this;
    }

    @Override
    public BuildableScanFromExisting ordering(Scan.Ordering ordering) {
      checkNotNull(ordering);
      orderings.add(ordering);
      return this;
    }
    /**
     * Sets the specified clustering key with the specified boundary as a starting point for scan.
     *
     * @param clusteringKey a starting clustering key. This can be set to {@code null} to clear the
     *     existing starting clustering key.
     * @param inclusive indicates whether the boundary is inclusive or not
     * @return the scan operation builder
     */
    @Override
    public BuildableScanFromExisting start(@Nullable Key clusteringKey, boolean inclusive) {
      startClusteringKey = clusteringKey;
      startInclusive = inclusive;
      return this;
    }
    /**
     * Sets the specified clustering key with the specified boundary as an ending point for scan.
     *
     * @param clusteringKey an ending clustering key. This can be set to {@code null} to clear the
     *     existing ending clustering key.
     * @param inclusive indicates whether the boundary is inclusive or not
     * @return the scan operation builder
     */
    @Override
    public BuildableScanFromExisting end(@Nullable Key clusteringKey, boolean inclusive) {
      endClusteringKey = clusteringKey;
      endInclusive = inclusive;
      return this;
    }

    /**
     * Sets the specified clustering key with the specified boundary as a starting point for scan.
     *
     * @param clusteringKey a starting clustering key. This can be set to {@code null} to clear the
     *     existing starting clustering key.
     * @return the scan operation builder
     */
    @Override
    public BuildableScanFromExisting start(@Nullable Key clusteringKey) {
      return ClusteringKeyFiltering.super.start(clusteringKey);
    }

    /**
     * Sets the specified clustering key with the specified boundary as an ending point for scan.
     * The boundary is inclusive.
     *
     * @param clusteringKey an ending clustering key. This can be set to {@code null} to clear the
     *     existing ending clustering key.
     * @return the scan operation builder
     */
    @Override
    public BuildableScanFromExisting end(@Nullable Key clusteringKey) {
      return ClusteringKeyFiltering.super.end(clusteringKey);
    }

    @Override
    public BuildableScanFromExisting clearOrderings() {
      this.orderings.clear();
      return this;
    }

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

      scan.withConsistency(consistency);

      return scan;
    }
  }

  public static class BuildableScanAll
      implements Consistency<BuildableScanAll>,
          Projection<BuildableScanAll>,
          Limit<BuildableScanAll> {
    private final String namespaceName;
    private final String tableName;
    private final List<String> projections = new ArrayList<>();
    private int limit = 0;
    @Nullable private com.scalar.db.api.Consistency consistency;

    public BuildableScanAll(String namespaceName, String tableName) {
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
    public BuildableScanAll limit(int limit) {
      this.limit = limit;
      return this;
    }

    @Override
    public BuildableScanAll consistency(com.scalar.db.api.Consistency consistency) {
      checkNotNull(consistency);
      this.consistency = consistency;
      return this;
    }

    public Scan build() {
      Scan scan = new ScanAll();
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

  public static class BuildableScanOrScanAllFromExisting extends BuildableScanFromExisting
      implements All<BuildableScanAllFromExisting> {
    private final Scan existingScan;

    public BuildableScanOrScanAllFromExisting(Scan scan) {
      super(scan);
      this.existingScan = scan;
    }

    @Override
    public BuildableScanAllFromExisting all() {
      if (!(existingScan instanceof ScanAll)) {
        throw new IllegalStateException(
            "This method is to be called only when rebuilding a Scan object used to "
                + "retrieve all the database entries. Rebuilding a Scan object targeting a database"
                + " partition is not possible here.");
      }
      return new BuildableScanAllFromExisting((ScanAll) existingScan);
    }

    private void checkIsNotScanAll() {
      if (existingScan instanceof ScanAll) {
        throw new IllegalStateException(
            "This method is to be called only when rebuilding a Scan object targeting a database partition."
                + ". Rebuilding a Scan object used to retrieve all the database entries is not possible here");
      }
    }

    @Override
    public BuildableScanFromExisting namespace(String namespaceName) {
      checkIsNotScanAll();
      return super.namespace(namespaceName);
    }

    @Override
    public BuildableScanFromExisting table(String tableName) {
      checkIsNotScanAll();
      return super.table(tableName);
    }

    @Override
    public BuildableScanFromExisting partitionKey(Key partitionKey) {
      checkIsNotScanAll();
      return super.partitionKey(partitionKey);
    }

    @Override
    public BuildableScanFromExisting consistency(com.scalar.db.api.Consistency consistency) {
      checkIsNotScanAll();
      return super.consistency(consistency);
    }

    @Override
    public BuildableScanFromExisting projection(String projection) {
      checkIsNotScanAll();
      return super.projection(projection);
    }

    @Override
    public BuildableScanFromExisting projections(Collection<String> projections) {
      checkIsNotScanAll();
      return super.projections(projections);
    }

    @Override
    public BuildableScanFromExisting clearProjections() {
      checkIsNotScanAll();
      return super.clearProjections();
    }

    @Override
    public BuildableScanFromExisting limit(int limit) {
      checkIsNotScanAll();
      return super.limit(limit);
    }

    @Override
    public BuildableScanFromExisting ordering(Scan.Ordering ordering) {
      checkIsNotScanAll();
      return super.ordering(ordering);
    }

    @Override
    public BuildableScanFromExisting start(@Nullable Key clusteringKey, boolean inclusive) {
      checkIsNotScanAll();
      return super.start(clusteringKey, inclusive);
    }

    @Override
    public BuildableScanFromExisting end(@Nullable Key clusteringKey, boolean inclusive) {
      checkIsNotScanAll();
      return super.end(clusteringKey, inclusive);
    }

    @Override
    public BuildableScanFromExisting start(@Nullable Key clusteringKey) {
      checkIsNotScanAll();
      return super.start(clusteringKey);
    }

    @Override
    public BuildableScanFromExisting end(@Nullable Key clusteringKey) {
      checkIsNotScanAll();
      return super.end(clusteringKey);
    }

    @Override
    public BuildableScanFromExisting clearOrderings() {
      checkIsNotScanAll();
      return super.clearOrderings();
    }

    @Override
    public Scan build() {
      checkIsNotScanAll();
      return super.build();
    }
  }

  public static class BuildableScanAllFromExisting
      implements OperationBuilder.Namespace<BuildableScanAllFromExisting>,
          OperationBuilder.Table<BuildableScanAllFromExisting>,
          Limit<BuildableScanAllFromExisting>,
          Projection<BuildableScanAllFromExisting>,
          ClearProjections<BuildableScanAllFromExisting>,
          Consistency<BuildableScanAllFromExisting> {
    private final List<String> projections = new ArrayList<>();
    @Nullable private String namespaceName;
    @Nullable private String tableName;
    private com.scalar.db.api.Consistency consistency;
    private int limit;

    public BuildableScanAllFromExisting(ScanAll scanAll) {
      this.namespaceName = scanAll.forNamespace().orElse(null);
      this.tableName = scanAll.forTable().orElse(null);
      this.limit = scanAll.getLimit();
      this.projections.addAll(scanAll.getProjections());
      this.consistency = scanAll.getConsistency();
    }

    @Override
    public BuildableScanAllFromExisting namespace(String namespaceName) {
      checkNotNull(namespaceName);
      this.namespaceName = namespaceName;
      return this;
    }

    @Override
    public BuildableScanAllFromExisting table(String tableName) {
      checkNotNull(tableName);
      this.tableName = tableName;
      return this;
    }

    @Override
    public BuildableScanAllFromExisting consistency(com.scalar.db.api.Consistency consistency) {
      checkNotNull(consistency);
      this.consistency = consistency;
      return this;
    }

    @Override
    public BuildableScanAllFromExisting projection(String projection) {
      Objects.requireNonNull(projection);
      this.projections.add(projection);
      return this;
    }

    @Override
    public BuildableScanAllFromExisting projections(Collection<String> projections) {
      checkNotNull(projections);
      this.projections.addAll(projections);
      return this;
    }

    @Override
    public BuildableScanAllFromExisting clearProjections() {
      this.projections.clear();
      return this;
    }

    @Override
    public BuildableScanAllFromExisting limit(int limit) {
      this.limit = limit;
      return this;
    }

    public Scan build() {
      Scan scan = new ScanAll();
      scan.forNamespace(namespaceName).forTable(tableName).withLimit(limit);

      if (!projections.isEmpty()) {
        scan.withProjections(projections);
      }

      scan.withConsistency(consistency);

      return scan;
    }
  }
}
