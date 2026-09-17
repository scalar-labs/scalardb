package com.scalar.db.common;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Insert;
import com.scalar.db.api.Put;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Update;
import com.scalar.db.api.Upsert;
import com.scalar.db.io.Key;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class OperationAttributeMergerTest {

  private static final String NS = "ns";
  private static final String TBL = "tbl";

  private static Map<String, String> attrs(String k, String v) {
    Map<String, String> m = new HashMap<>();
    m.put(k, v);
    return m;
  }

  private static Get getWithoutAttr() {
    return Get.newBuilder().namespace(NS).table(TBL).partitionKey(Key.ofInt("pk", 1)).build();
  }

  private static Get getWithAttr(String k, String v) {
    return Get.newBuilder()
        .namespace(NS)
        .table(TBL)
        .partitionKey(Key.ofInt("pk", 1))
        .attribute(k, v)
        .build();
  }

  @Test
  void merge_WhenOperationMissingKey_ShouldAddTransactionAttribute() {
    Get get = Get.newBuilder().namespace(NS).table(TBL).partitionKey(Key.ofInt("pk", 1)).build();

    Get merged = OperationAttributeMerger.merge(get, attrs("k", "v"));

    assertThat(merged.getAttributes()).containsEntry("k", "v");
  }

  @Test
  void merge_WhenOperationHasKey_ShouldKeepOperationValue() {
    Get get =
        Get.newBuilder()
            .namespace(NS)
            .table(TBL)
            .partitionKey(Key.ofInt("pk", 1))
            .attribute("k", "op-value")
            .build();

    Get merged = OperationAttributeMerger.merge(get, attrs("k", "tx-value"));

    assertThat(merged.getAttributes()).containsEntry("k", "op-value");
  }

  @Test
  void merge_WhenOperationHasSomeButNotAllKeys_ShouldFillMissingAndKeepExisting() {
    Get get =
        Get.newBuilder()
            .namespace(NS)
            .table(TBL)
            .partitionKey(Key.ofInt("pk", 1))
            .attribute("k1", "op-value")
            .build();
    Map<String, String> transactionAttributes = new HashMap<>();
    transactionAttributes.put("k1", "tx-value-1");
    transactionAttributes.put("k2", "tx-value-2");

    Get merged = OperationAttributeMerger.merge(get, transactionAttributes);

    // Partial overlap: the operation already carries k1 (kept, operation wins) while the missing k2
    // is filled from the transaction. Exercises the per-key guard, not just the all-or-nothing
    // path.
    assertThat(merged.getAttributes()).containsEntry("k1", "op-value");
    assertThat(merged.getAttributes()).containsEntry("k2", "tx-value-2");
  }

  @Test
  void merge_WhenNoTransactionAttributes_ShouldReturnSameOperation() {
    Get get = Get.newBuilder().namespace(NS).table(TBL).partitionKey(Key.ofInt("pk", 1)).build();

    Get merged = OperationAttributeMerger.merge(get, Collections.emptyMap());

    // Empty attributes: the operation is returned unchanged (same reference).
    assertThat(merged).isSameAs(get);
  }

  @Test
  void merge_WhenOperationAlreadyHasAllKeys_ShouldReturnSameOperation() {
    Get get =
        Get.newBuilder()
            .namespace(NS)
            .table(TBL)
            .partitionKey(Key.ofInt("pk", 1))
            .attribute("k", "op-value")
            .build();

    Get merged = OperationAttributeMerger.merge(get, attrs("k", "tx-value"));

    // The operation already carries every transaction-attribute key → returned unchanged (same
    // reference), nothing to merge.
    assertThat(merged).isSameAs(get);
  }

  @Test
  void merge_ShouldPreserveOperationSubtypeForEachType() {
    Key pk = Key.ofInt("pk", 1);
    Map<String, String> a = attrs("k", "v");

    Put put = Put.newBuilder().namespace(NS).table(TBL).partitionKey(pk).intValue("v", 1).build();
    Insert insert =
        Insert.newBuilder().namespace(NS).table(TBL).partitionKey(pk).intValue("v", 1).build();
    Upsert upsert =
        Upsert.newBuilder().namespace(NS).table(TBL).partitionKey(pk).intValue("v", 1).build();
    Update update =
        Update.newBuilder().namespace(NS).table(TBL).partitionKey(pk).intValue("v", 1).build();
    Delete delete = Delete.newBuilder().namespace(NS).table(TBL).partitionKey(pk).build();
    Scan scan = Scan.newBuilder().namespace(NS).table(TBL).all().build();

    assertThat(OperationAttributeMerger.merge(put, a)).isInstanceOf(Put.class);
    assertThat(OperationAttributeMerger.merge(put, a).getAttributes()).containsEntry("k", "v");
    assertThat(OperationAttributeMerger.merge(insert, a)).isInstanceOf(Insert.class);
    assertThat(OperationAttributeMerger.merge(insert, a).getAttributes()).containsEntry("k", "v");
    assertThat(OperationAttributeMerger.merge(upsert, a)).isInstanceOf(Upsert.class);
    assertThat(OperationAttributeMerger.merge(upsert, a).getAttributes()).containsEntry("k", "v");
    assertThat(OperationAttributeMerger.merge(update, a)).isInstanceOf(Update.class);
    assertThat(OperationAttributeMerger.merge(update, a).getAttributes()).containsEntry("k", "v");
    assertThat(OperationAttributeMerger.merge(delete, a)).isInstanceOf(Delete.class);
    assertThat(OperationAttributeMerger.merge(delete, a).getAttributes()).containsEntry("k", "v");
    assertThat(OperationAttributeMerger.merge(scan, a)).isInstanceOf(Scan.class);
    assertThat(OperationAttributeMerger.merge(scan, a).getAttributes()).containsEntry("k", "v");
  }

  @Test
  void mergeEach_WhenNoTransactionAttributes_ShouldReturnSameListReference() {
    List<Get> operations = Arrays.asList(getWithoutAttr(), getWithoutAttr());

    List<Get> merged = OperationAttributeMerger.mergeEach(operations, Collections.emptyMap());

    // No transaction attributes: nothing to merge, so the original list is returned (no copy).
    assertThat(merged).isSameAs(operations);
  }

  @Test
  void mergeEach_WhenNoOperationNeedsMerging_ShouldReturnSameListReference() {
    List<Get> operations = Arrays.asList(getWithAttr("k", "a"), getWithAttr("k", "b"));

    List<Get> merged = OperationAttributeMerger.mergeEach(operations, attrs("k", "tx"));

    // Every operation already carries the transaction-attribute key → no element changes, so the
    // original list is returned (no copy).
    assertThat(merged).isSameAs(operations);
  }

  @Test
  void mergeEach_WhenSomeOperationsNeedMerging_ShouldCopyPreservingOrderAndUnchangedElements() {
    Get has = getWithAttr("k", "op"); // already carries the key → kept as-is
    Get missing = getWithoutAttr(); // missing the key → merged
    List<Get> operations = Arrays.asList(has, missing);

    List<Get> merged = OperationAttributeMerger.mergeEach(operations, attrs("k", "tx"));

    // At least one element changed: a new list is allocated, order preserved, the unchanged element
    // kept by reference, and the missing-key element gets the transaction attribute.
    assertThat(merged).isNotSameAs(operations);
    assertThat(merged).hasSize(2);
    assertThat(merged.get(0)).isSameAs(has);
    assertThat(merged.get(0).getAttributes()).containsEntry("k", "op");
    assertThat(merged.get(1).getAttributes()).containsEntry("k", "tx");
  }
}
