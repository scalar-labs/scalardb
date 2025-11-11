package com.scalar.db.storage.objectstorage;

import com.google.common.collect.Ordering;
import com.scalar.db.api.Get;
import com.scalar.db.api.Scan;
import com.scalar.db.api.ScanAll;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.Selection;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.CoreError;
import com.scalar.db.common.EmptyScanner;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Column;
import com.scalar.db.io.Key;
import com.scalar.db.util.ScalarDbUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class SelectStatementHandler extends StatementHandler {
  public SelectStatementHandler(
      ObjectStorageWrapper wrapper, TableMetadataManager metadataManager) {
    super(wrapper, metadataManager);
  }

  @Nonnull
  public Scanner handle(Selection selection) throws ExecutionException {
    TableMetadata tableMetadata = metadataManager.getTableMetadata(selection);
    if (selection instanceof Get) {
      if (ScalarDbUtils.isSecondaryIndexSpecified(selection, tableMetadata)) {
        throw new UnsupportedOperationException(
            CoreError.OBJECT_STORAGE_INDEX_NOT_SUPPORTED.buildMessage());
      } else {
        return executeGet((Get) selection, tableMetadata);
      }
    } else {
      if (selection instanceof ScanAll) {
        return executeScanAll((ScanAll) selection, tableMetadata);
      } else if (ScalarDbUtils.isSecondaryIndexSpecified(selection, tableMetadata)) {
        throw new UnsupportedOperationException(
            CoreError.OBJECT_STORAGE_INDEX_NOT_SUPPORTED.buildMessage());
      } else {
        return executeScan((Scan) selection, tableMetadata);
      }
    }
  }

  private Scanner executeGet(Get get, TableMetadata metadata) throws ExecutionException {
    ObjectStorageOperation operation = new ObjectStorageOperation(get, metadata);
    operation.checkArgument(Get.class);
    ObjectStoragePartition partition =
        getPartition(getNamespace(get), getTable(get), operation.getConcatenatedPartitionKey());
    if (!partition.getRecord(operation.getRecordId()).isPresent()) {
      return new EmptyScanner();
    }
    ObjectStorageRecord record = partition.getRecord(operation.getRecordId()).get();
    return new ScannerImpl(
        Collections.singletonList(record).iterator(),
        new ResultInterpreter(get.getProjections(), metadata),
        1);
  }

  private Scanner executeScan(Scan scan, TableMetadata metadata) throws ExecutionException {
    ObjectStorageOperation operation = new ObjectStorageOperation(scan, metadata);
    operation.checkArgument(Scan.class);
    ObjectStoragePartition partition =
        getPartition(getNamespace(scan), getTable(scan), operation.getConcatenatedPartitionKey());
    List<ObjectStorageRecord> records = new ArrayList<>(partition.getRecords().values());

    records.sort(
        (o1, o2) ->
            new ClusteringKeyComparator(metadata)
                .compare(o1.getClusteringKey(), o2.getClusteringKey()));
    if (isReverseOrder(scan, metadata)) {
      Collections.reverse(records);
    }

    // If the scan is for DESC clustering order, use the end clustering key as a start key and the
    // start clustering key as an end key
    boolean scanForDescClusteringOrder = isScanForDescClusteringOrder(scan, metadata);
    Optional<Key> startKey =
        scanForDescClusteringOrder ? scan.getEndClusteringKey() : scan.getStartClusteringKey();
    boolean startInclusive =
        scanForDescClusteringOrder ? scan.getEndInclusive() : scan.getStartInclusive();
    Optional<Key> endKey =
        scanForDescClusteringOrder ? scan.getStartClusteringKey() : scan.getEndClusteringKey();
    boolean endInclusive =
        scanForDescClusteringOrder ? scan.getStartInclusive() : scan.getEndInclusive();

    if (startKey.isPresent()) {
      records =
          filterRecordsByClusteringKeyBoundary(
              records, startKey.get(), true, startInclusive, metadata);
    }
    if (endKey.isPresent()) {
      records =
          filterRecordsByClusteringKeyBoundary(
              records, endKey.get(), false, endInclusive, metadata);
    }

    if (scan.getLimit() > 0) {
      records = records.subList(0, Math.min(scan.getLimit(), records.size()));
    }

    return new ScannerImpl(
        records.iterator(),
        new ResultInterpreter(scan.getProjections(), metadata),
        scan.getLimit());
  }

  private Scanner executeScanAll(ScanAll scan, TableMetadata metadata) throws ExecutionException {
    ObjectStorageOperation operation = new ObjectStorageOperation(scan, metadata);
    operation.checkArgument(ScanAll.class);
    Set<ObjectStorageRecord> records = getRecordsInTable(getNamespace(scan), getTable(scan));
    if (scan.getLimit() > 0) {
      records = records.stream().limit(scan.getLimit()).collect(Collectors.toSet());
    }
    return new ScannerImpl(
        records.iterator(),
        new ResultInterpreter(scan.getProjections(), metadata),
        scan.getLimit());
  }

  private ObjectStoragePartition getPartition(
      String namespaceName, String tableName, String partitionKey) throws ExecutionException {
    try {
      Optional<ObjectStorageWrapperResponse> response =
          wrapper.get(ObjectStorageUtils.getObjectKey(namespaceName, tableName, partitionKey));
      if (!response.isPresent()) {
        return new ObjectStoragePartition(Collections.emptyMap());
      }
      return ObjectStoragePartition.deserialize(response.get().getPayload());
    } catch (ObjectStorageWrapperException e) {
      throw new ExecutionException(
          CoreError.OBJECT_STORAGE_ERROR_OCCURRED_IN_SELECTION.buildMessage(e.getMessage()), e);
    }
  }

  private Set<ObjectStorageRecord> getRecordsInTable(String namespaceName, String tableName)
      throws ExecutionException {
    try {
      Set<String> partitionKeys =
          wrapper.getKeys(ObjectStorageUtils.getObjectKey(namespaceName, tableName, "")).stream()
              .map(
                  key ->
                      key.substring(key.lastIndexOf(ObjectStorageUtils.OBJECT_KEY_DELIMITER) + 1))
              .filter(partition -> !partition.isEmpty())
              .collect(Collectors.toSet());
      Set<ObjectStorageRecord> records = new HashSet<>();
      for (String partitionKey : partitionKeys) {
        ObjectStoragePartition partition = getPartition(namespaceName, tableName, partitionKey);
        records.addAll(partition.getRecords().values());
      }
      return records;
    } catch (Exception e) {
      throw new ExecutionException(
          CoreError.OBJECT_STORAGE_ERROR_OCCURRED_IN_SELECTION.buildMessage(e.getMessage()), e);
    }
  }

  private boolean isReverseOrder(Scan scan, TableMetadata metadata) {
    Boolean reverse = null;
    Iterator<String> iterator = metadata.getClusteringKeyNames().iterator();
    for (Scan.Ordering ordering : scan.getOrderings()) {
      String clusteringKeyName = iterator.next();
      if (!ordering.getColumnName().equals(clusteringKeyName)) {
        throw new IllegalArgumentException(
            CoreError.OPERATION_CHECK_ERROR_ORDERING_NOT_PROPERLY_SPECIFIED.buildMessage(scan));
      }
      boolean isValidOrder =
          ordering.getOrder() != metadata.getClusteringOrder(ordering.getColumnName());
      if (reverse == null) {
        reverse = isValidOrder;
      } else {
        if (reverse != isValidOrder) {
          throw new IllegalArgumentException(
              CoreError.OPERATION_CHECK_ERROR_ORDERING_NOT_PROPERLY_SPECIFIED.buildMessage(scan));
        }
      }
    }
    return reverse != null && reverse;
  }

  private boolean isScanForDescClusteringOrder(Scan scan, TableMetadata tableMetadata) {
    if (scan.getStartClusteringKey().isPresent()) {
      Key startClusteringKey = scan.getStartClusteringKey().get();
      String lastValueName =
          startClusteringKey.getColumns().get(startClusteringKey.size() - 1).getName();
      return tableMetadata.getClusteringOrder(lastValueName) == Scan.Ordering.Order.DESC;
    }
    if (scan.getEndClusteringKey().isPresent()) {
      Key endClusteringKey = scan.getEndClusteringKey().get();
      String lastValueName =
          endClusteringKey.getColumns().get(endClusteringKey.size() - 1).getName();
      return tableMetadata.getClusteringOrder(lastValueName) == Scan.Ordering.Order.DESC;
    }
    return false;
  }

  private List<ObjectStorageRecord> filterRecordsByClusteringKeyBoundary(
      List<ObjectStorageRecord> records,
      Key clusteringKey,
      boolean isStart,
      boolean isInclusive,
      TableMetadata metadata) {
    for (Column<?> column : clusteringKey.getColumns()) {
      Scan.Ordering.Order order = metadata.getClusteringOrder(column.getName());
      if (clusteringKey.getColumns().indexOf(column) == clusteringKey.size() - 1) {
        return records.stream()
            .filter(
                record -> {
                  Column<?> recordColumn =
                      ColumnValueMapper.convert(
                          record.getClusteringKey().get(column.getName()),
                          column.getName(),
                          column.getDataType());
                  int cmp =
                      order == Scan.Ordering.Order.ASC
                          ? Ordering.natural().compare(recordColumn, column)
                          : Ordering.natural().compare(column, recordColumn);
                  if (isStart) {
                    if (isInclusive) {
                      return cmp >= 0;
                    } else {
                      return cmp > 0;
                    }
                  } else {
                    if (isInclusive) {
                      return cmp <= 0;
                    } else {
                      return cmp < 0;
                    }
                  }
                })
            .collect(Collectors.toList());
      } else {
        List<ObjectStorageRecord> tmpRecords = new ArrayList<>();
        records.forEach(
            record -> {
              Column<?> recordColumn =
                  ColumnValueMapper.convert(
                      record.getClusteringKey().get(column.getName()),
                      column.getName(),
                      column.getDataType());
              int cmp = Ordering.natural().compare(recordColumn, column);
              if (cmp == 0) {
                tmpRecords.add(record);
              }
            });
        if (tmpRecords.isEmpty()) {
          return Collections.emptyList();
        }
        records = tmpRecords;
      }
    }
    return records;
  }
}
