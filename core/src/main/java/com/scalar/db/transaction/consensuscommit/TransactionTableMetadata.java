package com.scalar.db.transaction.consensuscommit;

import com.google.common.collect.Streams;
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
public class TransactionTableMetadata {

  private final TableMetadata tableMetadata;
  private final ImmutableLinkedHashSet<String> primaryKeyColumnNames;
  private final ImmutableLinkedHashSet<String> transactionMetaColumnNames;
  private final ImmutableLinkedHashSet<String> beforeImageColumnNames;
  private final ImmutableLinkedHashSet<String> afterImageColumnNames;

  public TransactionTableMetadata(TableMetadata tableMetadata) {
    this.tableMetadata = tableMetadata;
    primaryKeyColumnNames =
        new ImmutableLinkedHashSet<>(
            Streams.concat(
                    tableMetadata.getPartitionKeyNames().stream(),
                    tableMetadata.getClusteringKeyNames().stream())
                .collect(Collectors.toCollection(LinkedHashSet::new)));
    transactionMetaColumnNames =
        new ImmutableLinkedHashSet<>(
            tableMetadata.getColumnNames().stream()
                .filter(c -> ConsensusCommitUtils.isTransactionMetaColumn(c, tableMetadata))
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

  public LinkedHashSet<String> getPrimaryKeyColumnNames() {
    return primaryKeyColumnNames;
  }

  public LinkedHashSet<String> getTransactionMetaColumnNames() {
    return transactionMetaColumnNames;
  }

  public LinkedHashSet<String> getBeforeImageColumnNames() {
    return beforeImageColumnNames;
  }

  public LinkedHashSet<String> getAfterImageColumnNames() {
    return afterImageColumnNames;
  }
}
