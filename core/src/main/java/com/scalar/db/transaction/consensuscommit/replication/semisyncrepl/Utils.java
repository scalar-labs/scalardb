package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Record.Value;
import java.util.Collection;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public final class Utils {

  private Utils() {}

  public static String convPutOperationMetadataToString(Put put) {
    return MoreObjects.toStringHelper(put)
        .add("namespace", put.forNamespace())
        .add("table", put.forTable())
        .add("partitionKey", put.getPartitionKey())
        .add("clusteringKey", put.getClusteringKey())
        .add(
            "columns",
            ImmutableMap.builder()
                .put("tx_id", put.getColumns().get("tx_id"))
                .put("tx_version", put.getColumns().get("tx_version"))
                .put("tx_state", put.getColumns().get("tx_state"))
                .put("tx_prepared_at", put.getColumns().get("tx_prepared_at"))
                .put("tx_committed_at", put.getColumns().get("tx_committed_at"))
                .build())
        .add("consistency", put.getConsistency())
        .add("condition", put.getCondition())
        .toString();
  }

  public static String convOptBackupDbTableResultMetadataToString(Optional<Result> optResult) {
    return optResult.map(Utils::convBackupDbTableResultMetadataToString).orElse("None");
  }

  private static String nullableStr(@Nullable Object v) {
    return v != null ? v.toString() : "null";
  }

  public static String convBackupDbTableResultMetadataToString(Result result) {
    return MoreObjects.toStringHelper(result)
        .add(
            "columns",
            ImmutableMap.builder()
                .put("tx_id", nullableStr(result.getColumns().get("tx_id")))
                .put("tx_version", nullableStr(result.getColumns().get("tx_version")))
                .put("tx_state", nullableStr(result.getColumns().get("tx_state")))
                .put("tx_prepared_at", nullableStr(result.getColumns().get("tx_prepared_at")))
                .put("tx_committed_at", nullableStr(result.getColumns().get("tx_committed_at")))
                .build())
        .toString();
  }

  public static String convOptReplDbRecordsTableResultMetadataToString(Optional<Result> optResult) {
    return optResult.map(Utils::convReplDbRecordsTableResultMetadataToString).orElse("None");
  }

  public static String convReplDbRecordsTableResultMetadataToString(Result result) {
    return MoreObjects.toStringHelper(result)
        .add(
            "columns",
            ImmutableMap.builder()
                .put("namespace", result.getColumns().get("namespace"))
                .put("table", result.getColumns().get("table"))
                .put("pk", result.getColumns().get("pk"))
                .put("ck", result.getColumns().get("ck"))
                .put("version", result.getColumns().get("version"))
                .put("current_tx_id", result.getColumns().get("current_tx_id"))
                .put("prep_tx_id", result.getColumns().get("prep_tx_id"))
                .put("deleted", result.getColumns().get("deleted"))
                .put("insert_tx_ids", result.getColumns().get("insert_tx_ids"))
                .build())
        .toString();
  }

  public static String convValuesToString(Collection<Value> values) {
    return values.stream().map(Value::toStringOnlyWithMetadata).collect(Collectors.joining(","));
  }
}
