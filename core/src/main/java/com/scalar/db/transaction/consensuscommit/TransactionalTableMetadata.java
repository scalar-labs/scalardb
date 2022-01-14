package com.scalar.db.transaction.consensuscommit;

import com.scalar.db.api.Scan;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import com.scalar.db.util.ImmutableLinkedHashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.concurrent.Immutable;

@Immutable
public class TransactionalTableMetadata {

  private final TableMetadata tableMetadata;
  private final LinkedHashSet<String> transactionalMetaColumnNames;
  private final LinkedHashSet<String> beforeImageColumnNames;
  private final LinkedHashSet<String> afterImageColumnNames;

  public TransactionalTableMetadata(TableMetadata tableMetadata) {
    this.tableMetadata = tableMetadata;
    transactionalMetaColumnNames =
        new ImmutableLinkedHashSet<>(
            tableMetadata.getColumnNames().stream()
                .filter(c -> ConsensusCommitUtils.isTransactionalMetaColumn(c, tableMetadata))
                .collect(Collectors.toList()));
    beforeImageColumnNames =
        new ImmutableLinkedHashSet<>(
            tableMetadata.getColumnNames().stream()
                .filter(c -> ConsensusCommitUtils.isBeforeImageColumn(c, tableMetadata))
                .collect(Collectors.toList()));
    afterImageColumnNames =
        new ImmutableLinkedHashSet<>(
            tableMetadata.getColumnNames().stream()
                .filter(c -> ConsensusCommitUtils.isAfterImageColumn(c, tableMetadata))
                .collect(Collectors.toList()));
  }

  public TableMetadata getTableMetadata() {
    return tableMetadata;
  }

  public LinkedHashSet<String> getColumnNames() {
    return tableMetadata.getColumnNames();
  }

  public DataType getColumnDataType(String columnName) {
    return tableMetadata.getColumnDataType(columnName);
  }

  public LinkedHashSet<String> getPartitionKeyNames() {
    return tableMetadata.getPartitionKeyNames();
  }

  public LinkedHashSet<String> getClusteringKeyNames() {
    return tableMetadata.getClusteringKeyNames();
  }

  public Scan.Ordering.Order getClusteringOrder(String clusteringKeyName) {
    return tableMetadata.getClusteringOrder(clusteringKeyName);
  }

  public Map<String, Order> getClusteringOrders() {
    return tableMetadata.getClusteringOrders();
  }

  public Set<String> getSecondaryIndexNames() {
    return tableMetadata.getSecondaryIndexNames();
  }

  public LinkedHashSet<String> getTransactionalMetaColumnNames() {
    return transactionalMetaColumnNames;
  }

  public LinkedHashSet<String> getBeforeImageColumnNames() {
    return beforeImageColumnNames;
  }

  public LinkedHashSet<String> getAfterImageColumnNames() {
    return afterImageColumnNames;
  }
}
