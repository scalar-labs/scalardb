package com.scalar.db.common;

import com.scalar.db.api.Delete;
import com.scalar.db.api.DeleteBuilder;
import com.scalar.db.api.Get;
import com.scalar.db.api.GetBuilder;
import com.scalar.db.api.Insert;
import com.scalar.db.api.InsertBuilder;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutBuilder;
import com.scalar.db.api.Scan;
import com.scalar.db.api.ScanBuilder;
import com.scalar.db.api.Update;
import com.scalar.db.api.UpdateBuilder;
import com.scalar.db.api.Upsert;
import com.scalar.db.api.UpsertBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Merges transaction-scoped attributes into an {@link Operation}.
 *
 * <p>For each transaction attribute, the value is added to the operation only when the operation
 * does not already carry an attribute with that name — i.e., an attribute set directly on the
 * operation wins over the transaction-scoped one. The same operation subtype is returned.
 *
 * <p>This is the single implementation shared by the attribute-propagating decorators (the
 * manager-shaped {@link AttributePropagatingDistributedTransactionManager} and the id-based {@code
 * TwoPhaseCommitParticipant} decorator).
 */
final class OperationAttributeMerger {

  private OperationAttributeMerger() {}

  @SuppressWarnings("unchecked")
  static <T extends Operation> T merge(T operation, Map<String, String> transactionAttributes) {
    if (transactionAttributes.isEmpty() || containsAllKeys(operation, transactionAttributes)) {
      return operation;
    }
    if (operation instanceof Put) {
      return (T) mergeIntoPut((Put) operation, transactionAttributes);
    }
    if (operation instanceof Insert) {
      return (T) mergeIntoInsert((Insert) operation, transactionAttributes);
    }
    if (operation instanceof Upsert) {
      return (T) mergeIntoUpsert((Upsert) operation, transactionAttributes);
    }
    if (operation instanceof Update) {
      return (T) mergeIntoUpdate((Update) operation, transactionAttributes);
    }
    if (operation instanceof Delete) {
      return (T) mergeIntoDelete((Delete) operation, transactionAttributes);
    }
    if (operation instanceof Get) {
      return (T) mergeIntoGet((Get) operation, transactionAttributes);
    }
    if (operation instanceof Scan) {
      return (T) mergeIntoScan((Scan) operation, transactionAttributes);
    }
    return operation;
  }

  /**
   * Merges the transaction-scoped attributes into each operation in the list, with the same
   * per-operation semantics as {@link #merge}. Allocates a new list only when at least one
   * operation actually needs merging; otherwise the original list (same reference) is returned, so
   * the common case of "no transaction attribute needs to be added on any operation" avoids a list
   * copy.
   */
  @SuppressWarnings({"unchecked", "ReferenceEquality"})
  static <T extends Operation> List<T> mergeEach(
      List<? extends T> operations, Map<String, String> transactionAttributes) {
    if (transactionAttributes.isEmpty()) {
      return (List<T>) operations;
    }
    for (int i = 0; i < operations.size(); i++) {
      T operation = operations.get(i);
      T mergedOperation = merge(operation, transactionAttributes);
      if (mergedOperation == operation) {
        continue;
      }
      // Found an operation that needed merging: allocate the new list, backfill the prior unchanged
      // elements, and merge the remaining elements.
      List<T> merged = new ArrayList<>(operations.size());
      merged.addAll(operations.subList(0, i));
      merged.add(mergedOperation);
      for (int j = i + 1; j < operations.size(); j++) {
        merged.add(merge(operations.get(j), transactionAttributes));
      }
      return merged;
    }
    return (List<T>) operations;
  }

  private static boolean containsAllKeys(
      Operation operation, Map<String, String> transactionAttributes) {
    Map<String, String> operationAttributes = operation.getAttributes();
    for (String key : transactionAttributes.keySet()) {
      if (!operationAttributes.containsKey(key)) {
        return false;
      }
    }
    return true;
  }

  private static Put mergeIntoPut(Put put, Map<String, String> transactionAttributes) {
    Map<String, String> existing = put.getAttributes();
    PutBuilder.BuildableFromExisting builder = Put.newBuilder(put);
    for (Map.Entry<String, String> entry : transactionAttributes.entrySet()) {
      if (!existing.containsKey(entry.getKey())) {
        builder.attribute(entry.getKey(), entry.getValue());
      }
    }
    return builder.build();
  }

  private static Insert mergeIntoInsert(Insert insert, Map<String, String> transactionAttributes) {
    Map<String, String> existing = insert.getAttributes();
    InsertBuilder.BuildableFromExisting builder = Insert.newBuilder(insert);
    for (Map.Entry<String, String> entry : transactionAttributes.entrySet()) {
      if (!existing.containsKey(entry.getKey())) {
        builder.attribute(entry.getKey(), entry.getValue());
      }
    }
    return builder.build();
  }

  private static Upsert mergeIntoUpsert(Upsert upsert, Map<String, String> transactionAttributes) {
    Map<String, String> existing = upsert.getAttributes();
    UpsertBuilder.BuildableFromExisting builder = Upsert.newBuilder(upsert);
    for (Map.Entry<String, String> entry : transactionAttributes.entrySet()) {
      if (!existing.containsKey(entry.getKey())) {
        builder.attribute(entry.getKey(), entry.getValue());
      }
    }
    return builder.build();
  }

  private static Update mergeIntoUpdate(Update update, Map<String, String> transactionAttributes) {
    Map<String, String> existing = update.getAttributes();
    UpdateBuilder.BuildableFromExisting builder = Update.newBuilder(update);
    for (Map.Entry<String, String> entry : transactionAttributes.entrySet()) {
      if (!existing.containsKey(entry.getKey())) {
        builder.attribute(entry.getKey(), entry.getValue());
      }
    }
    return builder.build();
  }

  private static Delete mergeIntoDelete(Delete delete, Map<String, String> transactionAttributes) {
    Map<String, String> existing = delete.getAttributes();
    DeleteBuilder.BuildableFromExisting builder = Delete.newBuilder(delete);
    for (Map.Entry<String, String> entry : transactionAttributes.entrySet()) {
      if (!existing.containsKey(entry.getKey())) {
        builder.attribute(entry.getKey(), entry.getValue());
      }
    }
    return builder.build();
  }

  private static Get mergeIntoGet(Get get, Map<String, String> transactionAttributes) {
    Map<String, String> existing = get.getAttributes();
    GetBuilder.BuildableGetOrGetWithIndexFromExisting builder = Get.newBuilder(get);
    for (Map.Entry<String, String> entry : transactionAttributes.entrySet()) {
      if (!existing.containsKey(entry.getKey())) {
        builder.attribute(entry.getKey(), entry.getValue());
      }
    }
    return builder.build();
  }

  private static Scan mergeIntoScan(Scan scan, Map<String, String> transactionAttributes) {
    Map<String, String> existing = scan.getAttributes();
    ScanBuilder.BuildableScanOrScanAllFromExisting builder = Scan.newBuilder(scan);
    for (Map.Entry<String, String> entry : transactionAttributes.entrySet()) {
      if (!existing.containsKey(entry.getKey())) {
        builder.attribute(entry.getKey(), entry.getValue());
      }
    }
    return builder.build();
  }
}
