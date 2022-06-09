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

  public static class BuildableScanOrScanAllFromExisting
      implements OperationBuilder.Namespace<BuildableScanOrScanAllFromExisting>,
          OperationBuilder.Table<BuildableScanOrScanAllFromExisting>,
          OperationBuilder.PartitionKey<BuildableScanOrScanAllFromExisting>,
          ClusteringKeyFiltering<BuildableScanOrScanAllFromExisting>,
          Ordering<BuildableScanOrScanAllFromExisting>,
          ClearOrderings<BuildableScanOrScanAllFromExisting>,
          Limit<BuildableScanOrScanAllFromExisting>,
          Projection<BuildableScanOrScanAllFromExisting>,
          ClearProjections<BuildableScanOrScanAllFromExisting>,
          Consistency<BuildableScanOrScanAllFromExisting> {
    private final List<String> projections = new ArrayList<>();
    private final List<Scan.Ordering> orderings = new ArrayList<>();
    private final boolean isScanAll;
    @Nullable private String namespaceName;
    @Nullable private String tableName;
    private Key partitionKey;
    private com.scalar.db.api.Consistency consistency;
    @Nullable private Key startClusteringKey;
    private boolean startInclusive;
    @Nullable private Key endClusteringKey;
    private boolean endInclusive;
    private int limit;

    public BuildableScanOrScanAllFromExisting(Scan scan) {
      isScanAll = scan instanceof ScanAll;
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
      checkNotScanAll();
      checkNotNull(partitionKey);
      this.partitionKey = partitionKey;
      return this;
    }

    @Override
    public BuildableScanOrScanAllFromExisting consistency(
        com.scalar.db.api.Consistency consistency) {
      checkNotNull(consistency);
      this.consistency = consistency;
      return this;
    }

    @Override
    public BuildableScanOrScanAllFromExisting projection(String projection) {
      checkNotNull(projection);
      this.projections.add(projection);
      return this;
    }

    @Override
    public BuildableScanOrScanAllFromExisting projections(Collection<String> projections) {
      checkNotNull(projections);
      this.projections.addAll(projections);
      return this;
    }

    @Override
    public BuildableScanOrScanAllFromExisting clearProjections() {
      this.projections.clear();
      return this;
    }

    @Override
    public BuildableScanOrScanAllFromExisting limit(int limit) {
      this.limit = limit;
      return this;
    }

    @Override
    public BuildableScanOrScanAllFromExisting ordering(Scan.Ordering ordering) {
      checkNotScanAll();
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
    public BuildableScanOrScanAllFromExisting start(
        @Nullable Key clusteringKey, boolean inclusive) {
      checkNotScanAll();
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
    public BuildableScanOrScanAllFromExisting end(@Nullable Key clusteringKey, boolean inclusive) {
      checkNotScanAll();
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
    public BuildableScanOrScanAllFromExisting start(@Nullable Key clusteringKey) {
      checkNotScanAll();
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
    public BuildableScanOrScanAllFromExisting end(@Nullable Key clusteringKey) {
      checkNotScanAll();
      return ClusteringKeyFiltering.super.end(clusteringKey);
    }

    @Override
    public BuildableScanOrScanAllFromExisting clearOrderings() {
      checkNotScanAll();
      this.orderings.clear();
      return this;
    }

    private void checkNotScanAll() {
      if (isScanAll) {
        throw new UnsupportedOperationException(
            "This operation is not supported when scanning all the records of a database.");
      }
    }

    public Scan build() {
      Scan scan;

      if (isScanAll) {
        scan = new ScanAll();
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
}
