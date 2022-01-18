package com.scalar.db.graphql.datafetcher;

import static java.util.stream.Collectors.toList;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scan.Ordering;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.io.Key;
import graphql.execution.DataFetcherResult;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryScanDataFetcher
    implements DataFetcher<DataFetcherResult<Map<String, List<Map<String, Object>>>>> {
  private static final Logger LOGGER = LoggerFactory.getLogger(QueryScanDataFetcher.class);
  private final DistributedStorage storage;
  private final DataFetcherHelper helper;

  public QueryScanDataFetcher(DistributedStorage storage, DataFetcherHelper helper) {
    this.storage = storage;
    this.helper = helper;
  }

  @Override
  public DataFetcherResult<Map<String, List<Map<String, Object>>>> get(
      DataFetchingEnvironment environment) {
    Map<String, Object> scanInput = environment.getArgument("scan");
    LOGGER.debug("got scan argument: {}", scanInput);
    Scan scan = createScan(scanInput);

    // TODO: scan.withProjections()

    DataFetcherResult.Builder<Map<String, List<Map<String, Object>>>> result =
        DataFetcherResult.newResult();
    LinkedHashSet<String> fieldNames = helper.getFieldNames();
    ImmutableList.Builder<Map<String, Object>> list = ImmutableList.builder();
    try {
      for (Result dbResult : performScan(environment, scan)) {
        ImmutableMap.Builder<String, Object> map = ImmutableMap.builder();
        for (String fieldName : fieldNames) {
          dbResult.getValue(fieldName).ifPresent(value -> map.put(fieldName, value.get()));
        }
        list.add(map.build());
      }
      result.data(ImmutableMap.of(helper.getObjectTypeName(), list.build()));
    } catch (CrudException | ExecutionException e) {
      LOGGER.warn("Scalar DB scan operation failed", e);
      result.error(DataFetcherHelper.getGraphQLError(e, environment));
    }

    return result.build();
  }

  @VisibleForTesting
  @SuppressWarnings("unchecked")
  Scan createScan(Map<String, Object> scanInput) {
    Scan scan =
        new Scan(
                helper.createPartitionKeyFromKeyArgument(
                    (Map<String, Object>) scanInput.get("partitionKey")))
            .forNamespace(helper.getNamespaceName())
            .forTable(helper.getTableName());

    List<Map<String, Object>> startInput = (List<Map<String, Object>>) scanInput.get("start");
    Boolean startInclusiveInput = (Boolean) scanInput.get("startInclusive");
    if (startInput != null) {
      Key key =
          new Key(
              startInput.stream()
                  .map(
                      start ->
                          DataFetcherHelper.createValueFromMap((String) start.get("name"), start))
                  .collect(toList()));
      if (startInclusiveInput != null) {
        scan.withStart(key, startInclusiveInput);
      } else {
        scan.withStart(key);
      }
    }

    List<Map<String, Object>> endInput = (List<Map<String, Object>>) scanInput.get("end");
    Boolean endInclusiveInput = (Boolean) scanInput.get("endInclusive");
    if (endInput != null) {
      Key key =
          new Key(
              endInput.stream()
                  .map(end -> DataFetcherHelper.createValueFromMap((String) end.get("name"), end))
                  .collect(toList()));
      if (endInclusiveInput != null) {
        scan.withEnd(key, endInclusiveInput);
      } else {
        scan.withEnd(key);
      }
    }

    Integer limitInput = (Integer) scanInput.get("limit");
    if (limitInput != null) {
      scan.withLimit(limitInput);
    }

    List<Map<String, Object>> orderingsInput =
        (List<Map<String, Object>>) scanInput.get("orderings");
    if (orderingsInput != null) {
      for (Map<String, Object> o : orderingsInput) {
        scan.withOrdering(
            new Ordering((String) o.get("name"), Order.valueOf((String) o.get("order"))));
      }
    }

    String consistencyInput = (String) scanInput.get("consistency");
    if (consistencyInput != null) {
      scan.withConsistency(Consistency.valueOf(consistencyInput));
    }

    return scan;
  }

  @VisibleForTesting
  List<Result> performScan(DataFetchingEnvironment environment, Scan scan)
      throws CrudException, ExecutionException {
    DistributedTransaction transaction = DataFetcherHelper.getCurrentTransaction(environment);
    if (transaction != null) {
      LOGGER.debug("running Scan operation with transaction: {}", scan);
      return transaction.scan(scan);
    } else {
      LOGGER.debug("running Scan operation with storage: {}", scan);
      return ImmutableList.copyOf(storage.scan(scan));
    }
  }
}
