package com.scalar.db.api;

import static com.google.common.base.Preconditions.checkNotNull;

import com.scalar.db.api.OperationBuilder.All;
import com.scalar.db.api.OperationBuilder.Buildable;
import com.scalar.db.api.OperationBuilder.ClearBoundaries;
import com.scalar.db.api.OperationBuilder.ClearOrderings;
import com.scalar.db.api.OperationBuilder.ClearProjections;
import com.scalar.db.api.OperationBuilder.ClusteringKeyFiltering;
import com.scalar.db.api.OperationBuilder.Consistency;
import com.scalar.db.api.OperationBuilder.IndexKey;
import com.scalar.db.api.OperationBuilder.Limit;
import com.scalar.db.api.OperationBuilder.Ordering;
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

public class ScanBuilder {

  public static class Namespace implements OperationBuilder.Namespace<Table> {

    Namespace() {}

    @Override
    public Table namespace(String namespaceName) {
      checkNotNull(namespaceName);
      return new Table(namespaceName);
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
      implements IndexKey<BuildableIndexScan>, All<BuildableScanAll> {

    private PartitionKeyOrIndexKeyOrAll(String namespace, String table) {
      super(namespace, table);
    }

    @Override
    public BuildableScan partitionKey(Key partitionKey) {
      checkNotNull(partitionKey);
      return new BuildableScan(namespaceName, tableName, partitionKey);
    }

    @Override
    public BuildableIndexScan indexKey(Key indexKey) {
      checkNotNull(indexKey);
      return new BuildableIndexScan(namespaceName, tableName, indexKey);
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

    private BuildableScan(String namespace, String table, Key partitionKey) {
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

  public static class BuildableIndexScan
      implements Consistency<BuildableIndexScan>,
          Projection<BuildableIndexScan>,
          Limit<BuildableIndexScan> {
    private final String namespaceName;
    private final String tableName;
    private final Key indexKey;
    private final List<String> projections = new ArrayList<>();
    private int limit = 0;
    @Nullable private com.scalar.db.api.Consistency consistency;

    private BuildableIndexScan(String namespaceName, String tableName, Key indexKey) {
      this.namespaceName = namespaceName;
      this.tableName = tableName;
      this.indexKey = indexKey;
    }

    @Override
    public BuildableIndexScan projection(String projection) {
      checkNotNull(projection);
      projections.add(projection);
      return this;
    }

    @Override
    public BuildableIndexScan projections(Collection<String> projections) {
      checkNotNull(projections);
      this.projections.addAll(projections);
      return this;
    }

    @Override
    public BuildableIndexScan projections(String... projections) {
      return projections(Arrays.asList(projections));
    }

    @Override
    public BuildableIndexScan limit(int limit) {
      this.limit = limit;
      return this;
    }

    @Override
    public BuildableIndexScan consistency(com.scalar.db.api.Consistency consistency) {
      checkNotNull(consistency);
      this.consistency = consistency;
      return this;
    }

    public Scan build() {
      Scan scan = new IndexScan(indexKey);
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
      implements Consistency<BuildableScanAll>,
          Projection<BuildableScanAll>,
          Limit<BuildableScanAll> {
    private final String namespaceName;
    private final String tableName;
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

  public static class BuildableScanOrScanAllFromExisting extends BuildableScan
      implements OperationBuilder.Namespace<BuildableScanOrScanAllFromExisting>,
          OperationBuilder.Table<BuildableScanOrScanAllFromExisting>,
          PartitionKey<BuildableScanOrScanAllFromExisting>,
          IndexKey<BuildableScanOrScanAllFromExisting>,
          ClearProjections<BuildableScanOrScanAllFromExisting>,
          ClearOrderings<BuildableScanOrScanAllFromExisting>,
          ClearBoundaries<BuildableScanOrScanAllFromExisting> {

    private final boolean isIndexScan;
    private final boolean isScanAll;
    private Key indexKey;

    BuildableScanOrScanAllFromExisting(Scan scan) {
      super(scan.forNamespace().orElse(null), scan.forTable().orElse(null), scan.getPartitionKey());
      isIndexScan = scan instanceof IndexScan;
      if (isIndexScan) {
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
      checkNotIndexScanOrScanAll();
      checkNotNull(partitionKey);
      this.partitionKey = partitionKey;
      return this;
    }

    @Override
    public BuildableScanOrScanAllFromExisting indexKey(Key indexKey) {
      checkNotScanOrScanAll();
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
      checkNotIndexScanOrScanAll();
      super.ordering(ordering);
      return this;
    }

    @Override
    public BuildableScanOrScanAllFromExisting orderings(Collection<Scan.Ordering> orderings) {
      checkNotIndexScanOrScanAll();
      super.orderings(orderings);
      return this;
    }

    @Override
    public BuildableScanOrScanAllFromExisting orderings(Scan.Ordering... orderings) {
      checkNotIndexScanOrScanAll();
      super.orderings(orderings);
      return this;
    }

    @Override
    public BuildableScanOrScanAllFromExisting start(Key clusteringKey, boolean inclusive) {
      checkNotIndexScanOrScanAll();
      super.start(clusteringKey, inclusive);
      return this;
    }

    @Override
    public BuildableScanOrScanAllFromExisting end(Key clusteringKey, boolean inclusive) {
      checkNotIndexScanOrScanAll();
      super.end(clusteringKey, inclusive);
      return this;
    }

    @Override
    public BuildableScanOrScanAllFromExisting start(Key clusteringKey) {
      checkNotIndexScanOrScanAll();
      super.start(clusteringKey);
      return this;
    }

    @Override
    public BuildableScanOrScanAllFromExisting end(Key clusteringKey) {
      checkNotIndexScanOrScanAll();
      super.end(clusteringKey);
      return this;
    }

    @Override
    public BuildableScanOrScanAllFromExisting clearStart() {
      checkNotIndexScanOrScanAll();
      this.startClusteringKey = null;
      return this;
    }

    @Override
    public BuildableScanOrScanAllFromExisting clearEnd() {
      checkNotIndexScanOrScanAll();
      this.endClusteringKey = null;
      return this;
    }

    @Override
    public BuildableScanOrScanAllFromExisting clearOrderings() {
      checkNotIndexScanOrScanAll();
      this.orderings.clear();
      return this;
    }

    private void checkNotIndexScanOrScanAll() {
      if (isIndexScan || isScanAll) {
        throw new UnsupportedOperationException(
            "This operation is not supported when scanning all the records of a database "
                + "or scanning records of a database with using secondary index.");
      }
    }

    private void checkNotScanOrScanAll() {
      if (!isIndexScan) {
        throw new UnsupportedOperationException(
            "This operation is supported only when scanning records of a database with using secondary index.");
      }
    }

    @Override
    public Scan build() {
      Scan scan;

      if (isIndexScan) {
        scan = new IndexScan(indexKey);
      } else if (isScanAll) {
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
}
