package com.scalar.db.common;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DeleteBuilder;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.Get;
import com.scalar.db.api.GetBuilder;
import com.scalar.db.api.Insert;
import com.scalar.db.api.InsertBuilder;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutBuilder;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.ScanBuilder;
import com.scalar.db.api.Update;
import com.scalar.db.api.UpdateBuilder;
import com.scalar.db.api.Upsert;
import com.scalar.db.api.UpsertBuilder;
import com.scalar.db.exception.transaction.CrudException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A {@link DistributedTransaction} decorator that propagates the transaction-scoped attributes
 * given to {@code begin(..., Map<String, String>)} into every operation issued on the transaction.
 * If an operation already carries an attribute with the same name, the operation's value wins.
 */
@NotThreadSafe
public class AttributePropagatingDistributedTransaction extends DecoratedDistributedTransaction {

  private final ImmutableMap<String, String> transactionAttributes;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public AttributePropagatingDistributedTransaction(
      DistributedTransaction transaction, Map<String, String> transactionAttributes) {
    super(transaction);
    this.transactionAttributes = ImmutableMap.copyOf(transactionAttributes);
  }

  @Override
  public Optional<Result> get(Get get) throws CrudException {
    return super.get(mergeAttributes(get));
  }

  @Override
  public List<Result> scan(Scan scan) throws CrudException {
    return super.scan(mergeAttributes(scan));
  }

  @Override
  public Scanner getScanner(Scan scan) throws CrudException {
    return super.getScanner(mergeAttributes(scan));
  }

  /** @deprecated As of release 3.13.0. Will be removed in release 5.0.0. */
  @Deprecated
  @Override
  public void put(Put put) throws CrudException {
    super.put(mergeAttributes(put));
  }

  /** @deprecated As of release 3.13.0. Will be removed in release 5.0.0. */
  @Deprecated
  @Override
  public void put(List<Put> puts) throws CrudException {
    super.put(mergeAttributesForList(puts));
  }

  @Override
  public void insert(Insert insert) throws CrudException {
    super.insert(mergeAttributes(insert));
  }

  @Override
  public void upsert(Upsert upsert) throws CrudException {
    super.upsert(mergeAttributes(upsert));
  }

  @Override
  public void update(Update update) throws CrudException {
    super.update(mergeAttributes(update));
  }

  @Override
  public void delete(Delete delete) throws CrudException {
    super.delete(mergeAttributes(delete));
  }

  /** @deprecated As of release 3.13.0. Will be removed in release 5.0.0. */
  @Deprecated
  @Override
  public void delete(List<Delete> deletes) throws CrudException {
    super.delete(mergeAttributesForList(deletes));
  }

  @Override
  public void mutate(List<? extends Mutation> mutations) throws CrudException {
    List<Mutation> merged = new ArrayList<>(mutations.size());
    for (Mutation mutation : mutations) {
      merged.add(mergeAttributes(mutation));
    }
    super.mutate(merged);
  }

  @Override
  public List<BatchResult> batch(List<? extends Operation> operations) throws CrudException {
    List<Operation> merged = new ArrayList<>(operations.size());
    for (Operation operation : operations) {
      merged.add(mergeAttributes(operation));
    }
    return super.batch(merged);
  }

  private <T extends Operation> List<T> mergeAttributesForList(List<T> operations) {
    List<T> merged = new ArrayList<>(operations.size());
    for (T operation : operations) {
      merged.add(mergeAttributes(operation));
    }
    return merged;
  }

  @SuppressWarnings("unchecked")
  private <T extends Operation> T mergeAttributes(T operation) {
    if (containsAllTransactionAttributeKeys(operation)) {
      return operation;
    }
    if (operation instanceof Put) {
      return (T) mergeAttributesIntoPut((Put) operation);
    }
    if (operation instanceof Insert) {
      return (T) mergeAttributesIntoInsert((Insert) operation);
    }
    if (operation instanceof Upsert) {
      return (T) mergeAttributesIntoUpsert((Upsert) operation);
    }
    if (operation instanceof Update) {
      return (T) mergeAttributesIntoUpdate((Update) operation);
    }
    if (operation instanceof Delete) {
      return (T) mergeAttributesIntoDelete((Delete) operation);
    }
    if (operation instanceof Get) {
      return (T) mergeAttributesIntoGet((Get) operation);
    }
    if (operation instanceof Scan) {
      return (T) mergeAttributesIntoScan((Scan) operation);
    }
    return operation;
  }

  private boolean containsAllTransactionAttributeKeys(Operation operation) {
    Map<String, String> operationAttributes = operation.getAttributes();
    for (String key : transactionAttributes.keySet()) {
      if (!operationAttributes.containsKey(key)) {
        return false;
      }
    }
    return true;
  }

  private Put mergeAttributesIntoPut(Put put) {
    Map<String, String> existing = put.getAttributes();
    PutBuilder.BuildableFromExisting builder = Put.newBuilder(put);
    for (Map.Entry<String, String> entry : transactionAttributes.entrySet()) {
      if (!existing.containsKey(entry.getKey())) {
        builder.attribute(entry.getKey(), entry.getValue());
      }
    }
    return builder.build();
  }

  private Insert mergeAttributesIntoInsert(Insert insert) {
    Map<String, String> existing = insert.getAttributes();
    InsertBuilder.BuildableFromExisting builder = Insert.newBuilder(insert);
    for (Map.Entry<String, String> entry : transactionAttributes.entrySet()) {
      if (!existing.containsKey(entry.getKey())) {
        builder.attribute(entry.getKey(), entry.getValue());
      }
    }
    return builder.build();
  }

  private Upsert mergeAttributesIntoUpsert(Upsert upsert) {
    Map<String, String> existing = upsert.getAttributes();
    UpsertBuilder.BuildableFromExisting builder = Upsert.newBuilder(upsert);
    for (Map.Entry<String, String> entry : transactionAttributes.entrySet()) {
      if (!existing.containsKey(entry.getKey())) {
        builder.attribute(entry.getKey(), entry.getValue());
      }
    }
    return builder.build();
  }

  private Update mergeAttributesIntoUpdate(Update update) {
    Map<String, String> existing = update.getAttributes();
    UpdateBuilder.BuildableFromExisting builder = Update.newBuilder(update);
    for (Map.Entry<String, String> entry : transactionAttributes.entrySet()) {
      if (!existing.containsKey(entry.getKey())) {
        builder.attribute(entry.getKey(), entry.getValue());
      }
    }
    return builder.build();
  }

  private Delete mergeAttributesIntoDelete(Delete delete) {
    Map<String, String> existing = delete.getAttributes();
    DeleteBuilder.BuildableFromExisting builder = Delete.newBuilder(delete);
    for (Map.Entry<String, String> entry : transactionAttributes.entrySet()) {
      if (!existing.containsKey(entry.getKey())) {
        builder.attribute(entry.getKey(), entry.getValue());
      }
    }
    return builder.build();
  }

  private Get mergeAttributesIntoGet(Get get) {
    Map<String, String> existing = get.getAttributes();
    GetBuilder.BuildableGetOrGetWithIndexFromExisting builder = Get.newBuilder(get);
    for (Map.Entry<String, String> entry : transactionAttributes.entrySet()) {
      if (!existing.containsKey(entry.getKey())) {
        builder.attribute(entry.getKey(), entry.getValue());
      }
    }
    return builder.build();
  }

  private Scan mergeAttributesIntoScan(Scan scan) {
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
