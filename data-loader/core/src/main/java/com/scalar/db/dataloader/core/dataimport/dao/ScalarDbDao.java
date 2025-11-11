package com.scalar.db.dataloader.core.dataimport.dao;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Get;
import com.scalar.db.api.GetBuilder;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutBuilder.Buildable;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.ScanBuilder;
import com.scalar.db.api.Scanner;
import com.scalar.db.api.TransactionManagerCrudOperable;
import com.scalar.db.dataloader.core.DataLoaderError;
import com.scalar.db.dataloader.core.ScanRange;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.UnknownTransactionStatusException;
import com.scalar.db.io.Column;
import com.scalar.db.io.Key;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import javax.annotation.Nullable;

/** The generic DAO that is used to scan ScalarDB data */
public class ScalarDbDao {

  /**
   * Retrieves a record from a ScalarDB table using the specified partition and optional clustering
   * keys while operating in storage mode through a {@link DistributedTransactionManager}.
   *
   * <p>This method creates a {@link Get} operation for the given namespace and table, executes it
   * using the provided transaction manager, and returns the result if the record exists.
   *
   * @param namespace the name of the ScalarDB namespace containing the target table
   * @param table the name of the table to retrieve the record from
   * @param partitionKey the partition key identifying the record's partition
   * @param clusteringKey the optional clustering key identifying a specific record within the
   *     partition; may be {@code null} if the table does not use clustering keys
   * @param manager the {@link DistributedTransactionManager} instance used to perform the get
   *     operation
   * @return an {@link Optional} containing the {@link Result} if the record exists, or an empty
   *     {@link Optional} if not found
   * @throws ScalarDbDaoException if an error occurs while performing the get operation or
   *     interacting with ScalarDB
   */
  public Optional<Result> get(
      String namespace,
      String table,
      Key partitionKey,
      Key clusteringKey,
      DistributedTransactionManager manager)
      throws ScalarDbDaoException {

    // Retrieving the key data for logging
    String loggingKey = keysToString(partitionKey, clusteringKey);

    try {
      Get get = createGetWith(namespace, table, partitionKey, clusteringKey);
      return manager.get(get);
    } catch (CrudException | UnknownTransactionStatusException e) {
      throw new ScalarDbDaoException("error GET " + loggingKey, e);
    }
  }

  /**
   * Retrieve record from ScalarDB instance in transaction mode
   *
   * @param namespace Namespace name
   * @param table Table name
   * @param partitionKey Partition key
   * @param clusteringKey Optional clustering key for get
   * @param transaction ScalarDB transaction instance
   * @return Optional get result
   * @throws ScalarDbDaoException if something goes wrong while reading the data
   */
  public Optional<Result> get(
      String namespace,
      String table,
      Key partitionKey,
      Key clusteringKey,
      DistributedTransaction transaction)
      throws ScalarDbDaoException {

    Get get = createGetWith(namespace, table, partitionKey, clusteringKey);
    // Retrieving the key data for logging
    String loggingKey = keysToString(partitionKey, clusteringKey);
    try {
      return transaction.get(get);
    } catch (CrudException e) {
      throw new ScalarDbDaoException("error GET " + loggingKey, e.getCause());
    }
  }

  /**
   * Save record in ScalarDB instance
   *
   * @param namespace Namespace name
   * @param table Table name
   * @param partitionKey Partition key
   * @param clusteringKey Optional clustering key
   * @param columns List of column values to be inserted or updated
   * @param transaction ScalarDB transaction instance
   * @throws ScalarDbDaoException if something goes wrong while executing the transaction
   */
  public void put(
      String namespace,
      String table,
      Key partitionKey,
      Key clusteringKey,
      List<Column<?>> columns,
      DistributedTransaction transaction)
      throws ScalarDbDaoException {

    Put put = createPutWith(namespace, table, partitionKey, clusteringKey, columns);
    try {
      transaction.put(put);
    } catch (CrudException e) {
      throw new ScalarDbDaoException(
          DataLoaderError.ERROR_CRUD_EXCEPTION.buildMessage(e.getMessage()), e);
    }
  }

  /**
   * Saves a record into a ScalarDB table using the specified partition and optional clustering keys
   * through a {@link DistributedTransactionManager}.
   *
   * <p>This method constructs a {@link Put} operation with the provided key and column information,
   * then executes it using the given transaction manager. The operation inserts a new record or
   * updates an existing one if a record with the same primary key already exists.
   *
   * @param namespace the name of the ScalarDB namespace containing the target table
   * @param table the name of the table where the record will be inserted or updated
   * @param partitionKey the partition key identifying the record's partition
   * @param clusteringKey the optional clustering key identifying a specific record within the
   *     partition; may be {@code null} if the table does not use clustering keys
   * @param columns the list of {@link Column} objects representing the column values to insert or
   *     update
   * @param manager the {@link DistributedTransactionManager} instance used to perform the put
   *     operation
   * @throws ScalarDbDaoException if an error occurs while executing the put operation or
   *     interacting with ScalarDB
   */
  public void put(
      String namespace,
      String table,
      Key partitionKey,
      Key clusteringKey,
      List<Column<?>> columns,
      DistributedTransactionManager manager)
      throws ScalarDbDaoException {
    Put put = createPutWith(namespace, table, partitionKey, clusteringKey, columns);
    try {
      manager.put(put);
    } catch (CrudException | UnknownTransactionStatusException e) {
      throw new ScalarDbDaoException(
          DataLoaderError.ERROR_CRUD_EXCEPTION.buildMessage(e.getMessage()), e);
    }
  }

  /**
   * Scan a ScalarDB table
   *
   * @param namespace ScalarDB namespace
   * @param table ScalarDB table name
   * @param partitionKey Partition key used in ScalarDB scan
   * @param range Optional range to set ScalarDB scan start and end values
   * @param sorts Optional scan clustering key sorting values
   * @param projections List of column projection to use during scan
   * @param limit Scan limit value
   * @param storage Distributed storage for ScalarDB connection that is running in storage mode
   * @return List of ScalarDB scan results
   * @throws ScalarDbDaoException if scan fails
   */
  public List<Result> scan(
      String namespace,
      String table,
      Key partitionKey,
      ScanRange range,
      List<Scan.Ordering> sorts,
      List<String> projections,
      int limit,
      DistributedStorage storage)
      throws ScalarDbDaoException {
    // Create scan
    Scan scan = createScan(namespace, table, partitionKey, range, sorts, projections, limit);

    // scan data
    try {
      try (Scanner scanner = storage.scan(scan)) {
        return scanner.all();
      }
    } catch (ExecutionException | IOException e) {
      throw new ScalarDbDaoException(DataLoaderError.ERROR_SCAN.buildMessage(e.getMessage()), e);
    }
  }

  /**
   * Scan a ScalarDB table
   *
   * @param namespace ScalarDB namespace
   * @param table ScalarDB table name
   * @param partitionKey Partition key used in ScalarDB scan
   * @param range Optional range to set ScalarDB scan start and end values
   * @param sorts Optional scan clustering key sorting values
   * @param projections List of column projection to use during scan
   * @param limit Scan limit value
   * @param transaction Distributed Transaction manager for ScalarDB connection that is * running in
   *     transaction mode
   * @return List of ScalarDB scan results
   * @throws ScalarDbDaoException if scan fails
   */
  public List<Result> scan(
      String namespace,
      String table,
      Key partitionKey,
      ScanRange range,
      List<Scan.Ordering> sorts,
      List<String> projections,
      int limit,
      DistributedTransaction transaction)
      throws ScalarDbDaoException {

    // Create scan
    Scan scan = createScan(namespace, table, partitionKey, range, sorts, projections, limit);

    // scan data
    try {
      return transaction.scan(scan);
    } catch (CrudException | NoSuchElementException e) {
      // No such element Exception is thrown when the scan is done in transaction mode but
      // ScalarDB is running in storage mode
      throw new ScalarDbDaoException(DataLoaderError.ERROR_SCAN.buildMessage(e.getMessage()), e);
    }
  }

  /**
   * Creates a {@link TransactionManagerCrudOperable.Scanner} instance for reading data from
   * ScalarDB.
   *
   * <p>a scanner that iterates over the retrieved records. It performs the scan transactionally
   * using a {@link DistributedTransactionManager}.
   *
   * @param namespace the ScalarDB namespace to scan
   * @param table the ScalarDB table name
   * @param projectionColumns the list of column names to include in the scan results
   * @param limit the maximum number of records to retrieve
   * @param manager the {@link DistributedTransactionManager} used to obtain the ScalarDB scanner in
   *     storage mode
   * @return a {@link TransactionManagerCrudOperable.Scanner} instance for iterating over the scan
   *     results
   * @throws ScalarDbDaoException if an error occurs while creating or executing the scan
   */
  public TransactionManagerCrudOperable.Scanner createScanner(
      String namespace,
      String table,
      List<String> projectionColumns,
      int limit,
      DistributedTransactionManager manager)
      throws ScalarDbDaoException {
    Scan scan =
        createScan(namespace, table, null, null, new ArrayList<>(), projectionColumns, limit);
    try {
      return manager.getScanner(scan);
    } catch (CrudException e) {
      throw new ScalarDbDaoException(DataLoaderError.ERROR_SCAN.buildMessage(e.getMessage()), e);
    }
  }

  /**
   * Creates a {@link TransactionManagerCrudOperable.Scanner} instance for reading data from
   * ScalarDB.
   *
   * <p>returns a scanner that iterates over the matching records. The scan is performed
   * transactionally through a {@link DistributedTransactionManager}.
   *
   * @param namespace the ScalarDB namespace to scan
   * @param table the ScalarDB table name
   * @param partitionKey the optional {@link Key} representing the partition key for the scan
   * @param scanRange the optional {@link ScanRange} defining the start and end boundaries for the
   *     scan
   * @param sortOrders the optional list of {@link Scan.Ordering} objects defining the clustering
   *     key sort order
   * @param projectionColumns the optional list of column names to include in the scan results
   * @param limit the maximum number of records to retrieve
   * @param manager the {@link DistributedTransactionManager} used to obtain the ScalarDB scanner in
   *     storage mode
   * @return a {@link TransactionManagerCrudOperable.Scanner} instance for iterating over scan
   *     results
   * @throws ScalarDbDaoException if an error occurs while creating or executing the scan
   */
  public TransactionManagerCrudOperable.Scanner createScanner(
      String namespace,
      String table,
      @Nullable Key partitionKey,
      @Nullable ScanRange scanRange,
      @Nullable List<Scan.Ordering> sortOrders,
      @Nullable List<String> projectionColumns,
      int limit,
      DistributedTransactionManager manager)
      throws ScalarDbDaoException {
    Scan scan =
        createScan(namespace, table, partitionKey, scanRange, sortOrders, projectionColumns, limit);
    try {
      return manager.getScanner(scan);
    } catch (CrudException e) {
      throw new ScalarDbDaoException(DataLoaderError.ERROR_SCAN.buildMessage(e.getMessage()), e);
    }
  }

  /**
   * Create ScalarDB scan instance
   *
   * @param namespace ScalarDB namespace
   * @param table ScalarDB table name
   * @param partitionKey Partition key used in ScalarDB scan
   * @param scanRange Optional range to set ScalarDB scan start and end values
   * @param sortOrders Optional scan clustering key sorting values
   * @param projectionColumns List of column projection to use during scan
   * @param limit Scan limit value
   * @return ScalarDB scan instance
   */
  Scan createScan(
      String namespace,
      String table,
      @Nullable Key partitionKey,
      @Nullable ScanRange scanRange,
      @Nullable List<Scan.Ordering> sortOrders,
      @Nullable List<String> projectionColumns,
      int limit) {
    // If no partition key is provided a scan all is created
    if (partitionKey == null) {
      ScanBuilder.BuildableScanAll buildableScanAll =
          Scan.newBuilder().namespace(namespace).table(table).all();

      // projection columns
      if (projectionColumns != null && !projectionColumns.isEmpty()) {
        buildableScanAll.projections(projectionColumns);
      }

      if (sortOrders != null && !sortOrders.isEmpty()) {
        for (Scan.Ordering sort : sortOrders) {
          buildableScanAll.ordering(sort);
        }
      }

      // limit
      if (limit > 0) {
        buildableScanAll.limit(limit);
      }
      return buildableScanAll.build();
    }

    // Create a scan with partition key (not a scan all)
    ScanBuilder.BuildableScan buildableScan =
        Scan.newBuilder().namespace(namespace).table(table).partitionKey(partitionKey);

    // Set the scan boundary
    if (scanRange != null) {
      // Set boundary start
      if (scanRange.getScanStartKey() != null) {
        buildableScan.start(scanRange.getScanStartKey(), scanRange.isStartInclusive());
      }

      // with end
      if (scanRange.getScanEndKey() != null) {
        buildableScan.end(scanRange.getScanEndKey(), scanRange.isEndInclusive());
      }
    }

    // clustering order
    if (sortOrders != null && !sortOrders.isEmpty()) {
      for (Scan.Ordering sort : sortOrders) {
        buildableScan.ordering(sort);
      }
    }

    // projections
    if (projectionColumns != null && !projectionColumns.isEmpty()) {
      buildableScan.projections(projectionColumns);
    }

    // limit
    if (limit > 0) {
      buildableScan.limit(limit);
    }
    return buildableScan.build();
  }

  /**
   * Return a ScalarDB get based on provided parameters
   *
   * @param namespace Namespace name
   * @param table Table name
   * @param partitionKey Partition key
   * @param clusteringKey Optional clustering key for get
   * @return ScalarDB Get instance
   */
  private Get createGetWith(
      String namespace, String table, Key partitionKey, @Nullable Key clusteringKey) {
    GetBuilder.BuildableGetWithPartitionKey buildable =
        Get.newBuilder().namespace(namespace).table(table).partitionKey(partitionKey);
    if (clusteringKey != null) {
      buildable.clusteringKey(clusteringKey);
    }
    return buildable.build();
  }

  /**
   * Return a ScalarDB put based on provided parameters
   *
   * @param namespace Namespace name
   * @param table Table name
   * @param partitionKey Partition key
   * @param clusteringKey Optional clustering key
   * @param columns List of column values
   * @return ScalarDB Put Instance
   */
  private Put createPutWith(
      String namespace,
      String table,
      Key partitionKey,
      @Nullable Key clusteringKey,
      List<Column<?>> columns) {
    Buildable buildable =
        Put.newBuilder().namespace(namespace).table(table).partitionKey(partitionKey);
    if (clusteringKey != null) {
      buildable.clusteringKey(clusteringKey);
    }

    for (Column<?> column : columns) {
      buildable.value(column);
    }
    return buildable.build();
  }

  private String keysToString(Key partitionKey, Key clusteringKey) {
    if (clusteringKey != null) {
      return partitionKey.toString() + "," + clusteringKey;
    } else {
      return partitionKey.toString();
    }
  }
}
