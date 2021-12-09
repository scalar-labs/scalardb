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
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.graphql.schema.TableGraphQlModel;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import graphql.schema.DataFetchingEnvironment;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

public class QueryScanDataFetcher extends DataFetcherBase<Map<String, List<Map<String, Object>>>> {

  public QueryScanDataFetcher(DistributedStorage storage, TableGraphQlModel tableModel) {
    super(storage, tableModel);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Map<String, List<Map<String, Object>>> get(DataFetchingEnvironment environment)
      throws Exception {
    Map<String, Object> scanInput = environment.getArgument("scan");
    Scan scan =
        new Scan(
                createPartitionKeyFromKeyArgument(
                    (Map<String, Object>) scanInput.get("partitionKey")))
            .forNamespace(tableModel.getNamespaceName())
            .forTable(tableModel.getTableName());

    List<Map<String, Object>> startInput = (List<Map<String, Object>>) scanInput.get("start");
    Boolean startInclusiveInput = (Boolean) scanInput.get("startInclusive");
    if (startInput != null) {
      Key key =
          new Key(
              startInput.stream()
                  .map(start -> createValueFromMap((String) start.get("name"), start))
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
                  .map(end -> createValueFromMap((String) end.get("name"), end))
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

    // TODO: scan.withProjections()
    LinkedHashSet<String> fieldNames = tableModel.getFieldNames();
    ImmutableList.Builder<Map<String, Object>> list = ImmutableList.builder();
    for (Result result : performScan(environment, scan)) {
      ImmutableMap.Builder<String, Object> map = ImmutableMap.builder();
      for (String fieldName : fieldNames) {
        result
            .getValue(fieldName)
            .ifPresent(
                value -> {
                  if (value instanceof TextValue) {
                    map.put(fieldName, value.getAsString().get());
                  } else {
                    map.put(fieldName, value.get());
                  }
                });
      }
      list.add(map.build());
    }

    return ImmutableMap.of(tableModel.getObjectType().getName(), list.build());
  }

  @VisibleForTesting
  List<Result> performScan(DataFetchingEnvironment environment, Scan scan)
      throws TransactionException, ExecutionException {
    DistributedTransaction transaction = getTransactionIfEnabled(environment);
    if (transaction != null) {
      return transaction.scan(scan);
    } else {
      return ImmutableList.copyOf(storage.scan(scan));
    }
  }
}
