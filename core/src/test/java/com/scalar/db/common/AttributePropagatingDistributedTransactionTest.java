package com.scalar.db.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
import com.scalar.db.api.TransactionCrudOperable.Scanner;
import com.scalar.db.api.Update;
import com.scalar.db.api.UpdateBuilder;
import com.scalar.db.api.Upsert;
import com.scalar.db.api.UpsertBuilder;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.io.Key;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class AttributePropagatingDistributedTransactionTest {

  private static final String TRANSACTION_ID = "test-tx-id";
  private static final String NAMESPACE = "ns";
  private static final String TABLE = "tbl";
  private static final String TX_ATTR_KEY_1 = "cc-implicit-pre-read-enabled";
  private static final String TX_ATTR_VALUE_1 = "true";
  private static final String TX_ATTR_KEY_2 = "tenant-id";
  private static final String TX_ATTR_VALUE_2 = "tenant-1";

  @Mock private DistributedTransaction delegate;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
    when(delegate.getId()).thenReturn(TRANSACTION_ID);
  }

  private AttributePropagatingDistributedTransaction newDecorator(
      Map<String, String> transactionAttributes) {
    return new AttributePropagatingDistributedTransaction(delegate, transactionAttributes);
  }

  private Map<String, String> twoTransactionAttributes() {
    Map<String, String> attributes = new HashMap<>();
    attributes.put(TX_ATTR_KEY_1, TX_ATTR_VALUE_1);
    attributes.put(TX_ATTR_KEY_2, TX_ATTR_VALUE_2);
    return attributes;
  }

  private Put buildPut(Map<String, String> attributes) {
    PutBuilder.BuildableFromExisting builder =
        Put.newBuilder(
            Put.newBuilder()
                .namespace(NAMESPACE)
                .table(TABLE)
                .partitionKey(Key.ofText("pk", "v"))
                .build());
    attributes.forEach(builder::attribute);
    return builder.build();
  }

  private Get buildGet(Map<String, String> attributes) {
    GetBuilder.BuildableGetOrGetWithIndexFromExisting builder =
        Get.newBuilder(
            Get.newBuilder()
                .namespace(NAMESPACE)
                .table(TABLE)
                .partitionKey(Key.ofText("pk", "v"))
                .build());
    attributes.forEach(builder::attribute);
    return builder.build();
  }

  private Scan buildScan(Map<String, String> attributes) {
    ScanBuilder.BuildableScanOrScanAllFromExisting builder =
        Scan.newBuilder(
            Scan.newBuilder()
                .namespace(NAMESPACE)
                .table(TABLE)
                .partitionKey(Key.ofText("pk", "v"))
                .build());
    attributes.forEach(builder::attribute);
    return builder.build();
  }

  private Insert buildInsert(Map<String, String> attributes) {
    InsertBuilder.BuildableFromExisting builder =
        Insert.newBuilder(
            Insert.newBuilder()
                .namespace(NAMESPACE)
                .table(TABLE)
                .partitionKey(Key.ofText("pk", "v"))
                .build());
    attributes.forEach(builder::attribute);
    return builder.build();
  }

  private Upsert buildUpsert(Map<String, String> attributes) {
    UpsertBuilder.BuildableFromExisting builder =
        Upsert.newBuilder(
            Upsert.newBuilder()
                .namespace(NAMESPACE)
                .table(TABLE)
                .partitionKey(Key.ofText("pk", "v"))
                .build());
    attributes.forEach(builder::attribute);
    return builder.build();
  }

  private Update buildUpdate(Map<String, String> attributes) {
    UpdateBuilder.BuildableFromExisting builder =
        Update.newBuilder(
            Update.newBuilder()
                .namespace(NAMESPACE)
                .table(TABLE)
                .partitionKey(Key.ofText("pk", "v"))
                .build());
    attributes.forEach(builder::attribute);
    return builder.build();
  }

  private Delete buildDelete(Map<String, String> attributes) {
    DeleteBuilder.BuildableFromExisting builder =
        Delete.newBuilder(
            Delete.newBuilder()
                .namespace(NAMESPACE)
                .table(TABLE)
                .partitionKey(Key.ofText("pk", "v"))
                .build());
    attributes.forEach(builder::attribute);
    return builder.build();
  }

  // -------------------- happy path: each operation type --------------------

  @Test
  public void get_WithTransactionAttributes_ShouldMergeIntoOperation() throws CrudException {
    AttributePropagatingDistributedTransaction decorator = newDecorator(twoTransactionAttributes());
    Get get = buildGet(Collections.emptyMap());
    Get expected = buildGet(twoTransactionAttributes());
    when(delegate.get(any(Get.class))).thenReturn(Optional.empty());

    decorator.get(get);

    verify(delegate).get(eq(expected));
  }

  @Test
  public void scan_WithTransactionAttributes_ShouldMergeIntoOperation() throws CrudException {
    AttributePropagatingDistributedTransaction decorator = newDecorator(twoTransactionAttributes());
    Scan scan = buildScan(Collections.emptyMap());
    Scan expected = buildScan(twoTransactionAttributes());
    when(delegate.scan(any(Scan.class))).thenReturn(Collections.emptyList());

    decorator.scan(scan);

    verify(delegate).scan(eq(expected));
  }

  @Test
  public void getScanner_WithTransactionAttributes_ShouldMergeIntoOperation() throws CrudException {
    AttributePropagatingDistributedTransaction decorator = newDecorator(twoTransactionAttributes());
    Scan scan = buildScan(Collections.emptyMap());
    Scan expected = buildScan(twoTransactionAttributes());
    Scanner expectedScanner = mock(Scanner.class);
    when(delegate.getScanner(any(Scan.class))).thenReturn(expectedScanner);

    Scanner actual = decorator.getScanner(scan);

    assertThat(actual).isSameAs(expectedScanner);
    verify(delegate).getScanner(eq(expected));
  }

  @Test
  public void put_WithTransactionAttributes_ShouldMergeIntoOperation() throws CrudException {
    AttributePropagatingDistributedTransaction decorator = newDecorator(twoTransactionAttributes());
    Put put = buildPut(Collections.emptyMap());
    Put expected = buildPut(twoTransactionAttributes());

    decorator.put(put);

    verify(delegate).put(eq(expected));
  }

  @Test
  public void insert_WithTransactionAttributes_ShouldMergeIntoOperation() throws CrudException {
    AttributePropagatingDistributedTransaction decorator = newDecorator(twoTransactionAttributes());
    Insert insert = buildInsert(Collections.emptyMap());
    Insert expected = buildInsert(twoTransactionAttributes());

    decorator.insert(insert);

    verify(delegate).insert(eq(expected));
  }

  @Test
  public void upsert_WithTransactionAttributes_ShouldMergeIntoOperation() throws CrudException {
    AttributePropagatingDistributedTransaction decorator = newDecorator(twoTransactionAttributes());
    Upsert upsert = buildUpsert(Collections.emptyMap());
    Upsert expected = buildUpsert(twoTransactionAttributes());

    decorator.upsert(upsert);

    verify(delegate).upsert(eq(expected));
  }

  @Test
  public void update_WithTransactionAttributes_ShouldMergeIntoOperation() throws CrudException {
    AttributePropagatingDistributedTransaction decorator = newDecorator(twoTransactionAttributes());
    Update update = buildUpdate(Collections.emptyMap());
    Update expected = buildUpdate(twoTransactionAttributes());

    decorator.update(update);

    verify(delegate).update(eq(expected));
  }

  @Test
  public void delete_WithTransactionAttributes_ShouldMergeIntoOperation() throws CrudException {
    AttributePropagatingDistributedTransaction decorator = newDecorator(twoTransactionAttributes());
    Delete delete = buildDelete(Collections.emptyMap());
    Delete expected = buildDelete(twoTransactionAttributes());

    decorator.delete(delete);

    verify(delegate).delete(eq(expected));
  }

  // -------------------- precedence: operation-side wins --------------------

  @Test
  public void put_WhenOperationHasSameKey_ShouldKeepOperationValue() throws CrudException {
    AttributePropagatingDistributedTransaction decorator =
        newDecorator(Collections.singletonMap(TX_ATTR_KEY_1, "true"));

    Put put = buildPut(Collections.singletonMap(TX_ATTR_KEY_1, "false"));
    Put expected = buildPut(Collections.singletonMap(TX_ATTR_KEY_1, "false"));

    decorator.put(put);

    verify(delegate).put(eq(expected));
  }

  @Test
  public void put_WhenOperationHasPartialOverlap_ShouldFillOnlyMissingKeys() throws CrudException {
    AttributePropagatingDistributedTransaction decorator = newDecorator(twoTransactionAttributes());

    Put put = buildPut(Collections.singletonMap(TX_ATTR_KEY_1, "operation-wins"));
    Map<String, String> expectedAttributes = new HashMap<>();
    expectedAttributes.put(TX_ATTR_KEY_1, "operation-wins");
    expectedAttributes.put(TX_ATTR_KEY_2, TX_ATTR_VALUE_2);
    Put expected = buildPut(expectedAttributes);

    decorator.put(put);

    verify(delegate).put(eq(expected));
  }

  // -------------------- fast path: full overlap returns same instance --------------------

  @Test
  public void put_WhenOperationAlreadyHasAllKeys_ShouldForwardSameInstance() throws CrudException {
    AttributePropagatingDistributedTransaction decorator = newDecorator(twoTransactionAttributes());
    Put put = buildPut(twoTransactionAttributes());

    decorator.put(put);

    verify(delegate).put(same(put));
  }

  // -------------------- empty transaction attributes --------------------

  @Test
  public void put_WhenTransactionAttributesEmpty_ShouldForwardSameInstance() throws CrudException {
    AttributePropagatingDistributedTransaction decorator = newDecorator(Collections.emptyMap());
    Put put = buildPut(Collections.emptyMap());

    decorator.put(put);

    verify(delegate).put(same(put));
  }

  // -------------------- defensive snapshot --------------------

  @Test
  public void put_WhenTransactionAttributesMutatedAfterConstruction_ShouldUseSnapshot()
      throws CrudException {
    Map<String, String> transactionAttributes = new HashMap<>();
    transactionAttributes.put(TX_ATTR_KEY_1, TX_ATTR_VALUE_1);
    AttributePropagatingDistributedTransaction decorator = newDecorator(transactionAttributes);

    transactionAttributes.put(TX_ATTR_KEY_2, "added-after");
    transactionAttributes.remove(TX_ATTR_KEY_1);

    Put put = buildPut(Collections.emptyMap());
    Put expected = buildPut(Collections.singletonMap(TX_ATTR_KEY_1, TX_ATTR_VALUE_1));

    decorator.put(put);

    verify(delegate).put(eq(expected));
  }

  // -------------------- mutate / batch --------------------

  @Test
  public void mutate_WithTransactionAttributes_ShouldMergeIntoEachMutation() throws CrudException {
    AttributePropagatingDistributedTransaction decorator = newDecorator(twoTransactionAttributes());

    Put put = buildPut(Collections.emptyMap());
    Delete delete = buildDelete(Collections.singletonMap(TX_ATTR_KEY_1, "operation-wins"));
    List<Mutation> mutations = Arrays.asList(put, delete);

    Put expectedPut = buildPut(twoTransactionAttributes());
    Map<String, String> expectedDeleteAttrs = new HashMap<>();
    expectedDeleteAttrs.put(TX_ATTR_KEY_1, "operation-wins");
    expectedDeleteAttrs.put(TX_ATTR_KEY_2, TX_ATTR_VALUE_2);
    Delete expectedDelete = buildDelete(expectedDeleteAttrs);
    List<Mutation> expected = Arrays.asList(expectedPut, expectedDelete);

    decorator.mutate(mutations);

    verify(delegate).mutate(eq(expected));
  }

  @Test
  public void batch_WithTransactionAttributes_ShouldMergeIntoEachOperation() throws CrudException {
    AttributePropagatingDistributedTransaction decorator = newDecorator(twoTransactionAttributes());

    Get get = buildGet(Collections.emptyMap());
    Put put = buildPut(Collections.emptyMap());
    List<Operation> operations = Arrays.asList(get, put);

    Get expectedGet = buildGet(twoTransactionAttributes());
    Put expectedPut = buildPut(twoTransactionAttributes());
    List<Operation> expected = Arrays.asList(expectedGet, expectedPut);

    when(delegate.batch(any())).thenReturn(Collections.emptyList());

    decorator.batch(operations);

    verify(delegate).batch(eq(expected));
  }

  // -------------------- pass-through --------------------

  @Test
  public void commit_ShouldDelegate() throws Exception {
    AttributePropagatingDistributedTransaction decorator = newDecorator(twoTransactionAttributes());
    decorator.commit();
    verify(delegate).commit();
  }

  @Test
  public void rollback_ShouldDelegate() throws Exception {
    AttributePropagatingDistributedTransaction decorator = newDecorator(twoTransactionAttributes());
    decorator.rollback();
    verify(delegate).rollback();
  }

  @Test
  public void abort_ShouldDelegate() throws Exception {
    AttributePropagatingDistributedTransaction decorator = newDecorator(twoTransactionAttributes());
    decorator.abort();
    verify(delegate).abort();
  }

  @Test
  public void getId_ShouldDelegate() {
    AttributePropagatingDistributedTransaction decorator = newDecorator(twoTransactionAttributes());
    assertThat(decorator.getId()).isEqualTo(TRANSACTION_ID);
  }

  @Test
  public void get_ShouldReturnDelegateResult() throws CrudException {
    AttributePropagatingDistributedTransaction decorator = newDecorator(twoTransactionAttributes());
    Get get = buildGet(Collections.emptyMap());
    Optional<Result> expected = Optional.of(mock(Result.class));
    when(delegate.get(any(Get.class))).thenReturn(expected);

    Optional<Result> actual = decorator.get(get);

    assertThat(actual).isSameAs(expected);
  }

  // -------------------- snapshot stability across multiple calls --------------------

  @Test
  public void multipleCalls_ShouldUseSameSnapshot() throws CrudException {
    AttributePropagatingDistributedTransaction decorator = newDecorator(twoTransactionAttributes());
    Put put1 = buildPut(Collections.emptyMap());
    Put put2 = buildPut(Collections.emptyMap());
    Put expected = buildPut(twoTransactionAttributes());

    decorator.put(put1);
    decorator.put(put2);

    ArgumentCaptor<Put> captor = ArgumentCaptor.forClass(Put.class);
    verify(delegate, times(2)).put(captor.capture());
    assertThat(captor.getAllValues()).containsExactly(expected, expected);
  }
}
