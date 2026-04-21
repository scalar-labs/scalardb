package com.scalar.db.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.scalar.db.api.Delete;
import com.scalar.db.api.DeleteBuilder;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
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
import com.scalar.db.common.AttributePropagatingDistributedTransactionManager.AttributePropagatingDistributedTransaction;
import com.scalar.db.exception.transaction.CrudException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.io.Key;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class AttributePropagatingDistributedTransactionManagerTest {

  private static final String TX_ID = "test-tx-id";
  private static final String NAMESPACE = "ns";
  private static final String TABLE = "tbl";

  @Mock private DistributedTransactionManager wrappedTransactionManager;
  @Mock private DistributedTransaction innerTransaction;

  private AttributePropagatingDistributedTransactionManager transactionManager;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    transactionManager =
        new AttributePropagatingDistributedTransactionManager(wrappedTransactionManager);
  }

  private Map<String, String> singleAttribute() {
    Map<String, String> attributes = new HashMap<>();
    attributes.put("k", "v");
    return attributes;
  }

  // -------------------- wraps when attributes non-empty --------------------

  @Test
  public void begin_WithNonEmptyAttributes_ShouldWrapWithAttributePropagatingDecorator()
      throws TransactionException {
    // Arrange
    Map<String, String> attributes = singleAttribute();
    when(wrappedTransactionManager.begin(attributes)).thenReturn(innerTransaction);

    // Act
    DistributedTransaction actual = transactionManager.begin(attributes);

    // Assert
    assertThat(actual).isInstanceOf(AttributePropagatingDistributedTransaction.class);
    assertThat(((DecoratedDistributedTransaction) actual).getOriginalTransaction())
        .isSameAs(innerTransaction);
  }

  @Test
  public void beginWithTxId_WithNonEmptyAttributes_ShouldWrapWithAttributePropagatingDecorator()
      throws TransactionException {
    // Arrange
    Map<String, String> attributes = singleAttribute();
    when(wrappedTransactionManager.begin(TX_ID, attributes)).thenReturn(innerTransaction);

    // Act
    DistributedTransaction actual = transactionManager.begin(TX_ID, attributes);

    // Assert
    assertThat(actual).isInstanceOf(AttributePropagatingDistributedTransaction.class);
    assertThat(((DecoratedDistributedTransaction) actual).getOriginalTransaction())
        .isSameAs(innerTransaction);
  }

  @Test
  public void beginReadOnly_WithNonEmptyAttributes_ShouldWrapWithAttributePropagatingDecorator()
      throws TransactionException {
    // Arrange
    Map<String, String> attributes = singleAttribute();
    when(wrappedTransactionManager.beginReadOnly(attributes)).thenReturn(innerTransaction);

    // Act
    DistributedTransaction actual = transactionManager.beginReadOnly(attributes);

    // Assert
    assertThat(actual).isInstanceOf(AttributePropagatingDistributedTransaction.class);
    assertThat(((DecoratedDistributedTransaction) actual).getOriginalTransaction())
        .isSameAs(innerTransaction);
  }

  @Test
  public void
      beginReadOnlyWithTxId_WithNonEmptyAttributes_ShouldWrapWithAttributePropagatingDecorator()
          throws TransactionException {
    // Arrange
    Map<String, String> attributes = singleAttribute();
    when(wrappedTransactionManager.beginReadOnly(TX_ID, attributes)).thenReturn(innerTransaction);

    // Act
    DistributedTransaction actual = transactionManager.beginReadOnly(TX_ID, attributes);

    // Assert
    assertThat(actual).isInstanceOf(AttributePropagatingDistributedTransaction.class);
    assertThat(((DecoratedDistributedTransaction) actual).getOriginalTransaction())
        .isSameAs(innerTransaction);
  }

  @Test
  public void start_WithNonEmptyAttributes_ShouldWrapWithAttributePropagatingDecorator()
      throws TransactionException {
    // Arrange
    Map<String, String> attributes = singleAttribute();
    when(wrappedTransactionManager.start(attributes)).thenReturn(innerTransaction);

    // Act
    DistributedTransaction actual = transactionManager.start(attributes);

    // Assert
    assertThat(actual).isInstanceOf(AttributePropagatingDistributedTransaction.class);
  }

  @Test
  public void startWithTxId_WithNonEmptyAttributes_ShouldWrapWithAttributePropagatingDecorator()
      throws TransactionException {
    // Arrange
    Map<String, String> attributes = singleAttribute();
    when(wrappedTransactionManager.start(TX_ID, attributes)).thenReturn(innerTransaction);

    // Act
    DistributedTransaction actual = transactionManager.start(TX_ID, attributes);

    // Assert
    assertThat(actual).isInstanceOf(AttributePropagatingDistributedTransaction.class);
  }

  @Test
  public void startReadOnly_WithNonEmptyAttributes_ShouldWrapWithAttributePropagatingDecorator()
      throws TransactionException {
    // Arrange
    Map<String, String> attributes = singleAttribute();
    when(wrappedTransactionManager.startReadOnly(attributes)).thenReturn(innerTransaction);

    // Act
    DistributedTransaction actual = transactionManager.startReadOnly(attributes);

    // Assert
    assertThat(actual).isInstanceOf(AttributePropagatingDistributedTransaction.class);
  }

  @Test
  public void
      startReadOnlyWithTxId_WithNonEmptyAttributes_ShouldWrapWithAttributePropagatingDecorator()
          throws TransactionException {
    // Arrange
    Map<String, String> attributes = singleAttribute();
    when(wrappedTransactionManager.startReadOnly(TX_ID, attributes)).thenReturn(innerTransaction);

    // Act
    DistributedTransaction actual = transactionManager.startReadOnly(TX_ID, attributes);

    // Assert
    assertThat(actual).isInstanceOf(AttributePropagatingDistributedTransaction.class);
  }

  // -------------------- does not wrap when attributes empty --------------------

  @Test
  public void begin_WithEmptyAttributes_ShouldForwardDelegateTransaction()
      throws TransactionException {
    // Arrange
    when(wrappedTransactionManager.begin(Collections.emptyMap())).thenReturn(innerTransaction);

    // Act
    DistributedTransaction actual = transactionManager.begin(Collections.emptyMap());

    // Assert
    assertThat(actual).isSameAs(innerTransaction);
  }

  @Test
  public void beginWithTxId_WithEmptyAttributes_ShouldForwardDelegateTransaction()
      throws TransactionException {
    // Arrange
    when(wrappedTransactionManager.begin(TX_ID, Collections.emptyMap()))
        .thenReturn(innerTransaction);

    // Act
    DistributedTransaction actual = transactionManager.begin(TX_ID, Collections.emptyMap());

    // Assert
    assertThat(actual).isSameAs(innerTransaction);
  }

  @Test
  public void beginReadOnly_WithEmptyAttributes_ShouldForwardDelegateTransaction()
      throws TransactionException {
    // Arrange
    when(wrappedTransactionManager.beginReadOnly(Collections.emptyMap()))
        .thenReturn(innerTransaction);

    // Act
    DistributedTransaction actual = transactionManager.beginReadOnly(Collections.emptyMap());

    // Assert
    assertThat(actual).isSameAs(innerTransaction);
  }

  @Test
  public void beginReadOnlyWithTxId_WithEmptyAttributes_ShouldForwardDelegateTransaction()
      throws TransactionException {
    // Arrange
    when(wrappedTransactionManager.beginReadOnly(TX_ID, Collections.emptyMap()))
        .thenReturn(innerTransaction);

    // Act
    DistributedTransaction actual = transactionManager.beginReadOnly(TX_ID, Collections.emptyMap());

    // Assert
    assertThat(actual).isSameAs(innerTransaction);
  }

  // -------------------- overloads without attributes do not wrap --------------------

  @Test
  public void begin_WithoutArguments_ShouldForwardDelegateTransaction()
      throws TransactionException {
    // Arrange
    when(wrappedTransactionManager.begin()).thenReturn(innerTransaction);

    // Act
    DistributedTransaction actual = transactionManager.begin();

    // Assert
    assertThat(actual).isSameAs(innerTransaction);
    verify(wrappedTransactionManager).begin();
  }

  @Test
  public void begin_WithOnlyTxId_ShouldForwardDelegateTransaction() throws TransactionException {
    // Arrange
    when(wrappedTransactionManager.begin(TX_ID)).thenReturn(innerTransaction);

    // Act
    DistributedTransaction actual = transactionManager.begin(TX_ID);

    // Assert
    assertThat(actual).isSameAs(innerTransaction);
    verify(wrappedTransactionManager).begin(TX_ID);
  }

  // -------------------- non-begin methods pass through --------------------

  @Test
  public void close_ShouldDelegate() {
    // Act
    transactionManager.close();

    // Assert
    verify(wrappedTransactionManager).close();
  }

  @Test
  public void constructor_ShouldNotTouchDelegate() {
    // Arrange
    DistributedTransactionManager newDelegate = mock(DistributedTransactionManager.class);

    // Act
    new AttributePropagatingDistributedTransactionManager(newDelegate);

    // Assert
    verifyNoInteractions(newDelegate);
  }

  @SuppressFBWarnings({"SIC_INNER_SHOULD_BE_STATIC", "EI_EXPOSE_REP2"})
  @SuppressWarnings("ClassCanBeStatic")
  @Nested
  public class AttributePropagatingDistributedTransactionTest {

    private static final String TRANSACTION_ID = "test-tx-id";
    private static final String TX_ATTR_KEY_1 = "cc-implicit-pre-read-enabled";
    private static final String TX_ATTR_VALUE_1 = "true";
    private static final String TX_ATTR_KEY_2 = "tenant-id";
    private static final String TX_ATTR_VALUE_2 = "tenant-1";

    @Mock private DistributedTransaction wrappedTransaction;

    @BeforeEach
    public void setUp() throws Exception {
      MockitoAnnotations.openMocks(this).close();
      when(wrappedTransaction.getId()).thenReturn(TRANSACTION_ID);
    }

    private AttributePropagatingDistributedTransaction newDecorator(
        Map<String, String> transactionAttributes) {
      return new AttributePropagatingDistributedTransaction(
          wrappedTransaction, transactionAttributes);
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
      // Arrange
      AttributePropagatingDistributedTransaction decorator =
          newDecorator(twoTransactionAttributes());
      Get get = buildGet(Collections.emptyMap());
      Get expected = buildGet(twoTransactionAttributes());
      when(wrappedTransaction.get(any(Get.class))).thenReturn(Optional.empty());

      // Act
      decorator.get(get);

      // Assert
      verify(wrappedTransaction).get(eq(expected));
    }

    @Test
    public void scan_WithTransactionAttributes_ShouldMergeIntoOperation() throws CrudException {
      // Arrange
      AttributePropagatingDistributedTransaction decorator =
          newDecorator(twoTransactionAttributes());
      Scan scan = buildScan(Collections.emptyMap());
      Scan expected = buildScan(twoTransactionAttributes());
      when(wrappedTransaction.scan(any(Scan.class))).thenReturn(Collections.emptyList());

      // Act
      decorator.scan(scan);

      // Assert
      verify(wrappedTransaction).scan(eq(expected));
    }

    @Test
    public void getScanner_WithTransactionAttributes_ShouldMergeIntoOperation()
        throws CrudException {
      // Arrange
      AttributePropagatingDistributedTransaction decorator =
          newDecorator(twoTransactionAttributes());
      Scan scan = buildScan(Collections.emptyMap());
      Scan expected = buildScan(twoTransactionAttributes());
      Scanner expectedScanner = mock(Scanner.class);
      when(wrappedTransaction.getScanner(any(Scan.class))).thenReturn(expectedScanner);

      // Act
      Scanner actual = decorator.getScanner(scan);

      // Assert
      assertThat(actual).isSameAs(expectedScanner);
      verify(wrappedTransaction).getScanner(eq(expected));
    }

    @Test
    public void put_WithTransactionAttributes_ShouldMergeIntoOperation() throws CrudException {
      // Arrange
      AttributePropagatingDistributedTransaction decorator =
          newDecorator(twoTransactionAttributes());
      Put put = buildPut(Collections.emptyMap());
      Put expected = buildPut(twoTransactionAttributes());

      // Act
      decorator.put(put);

      // Assert
      verify(wrappedTransaction).put(eq(expected));
    }

    @Test
    public void insert_WithTransactionAttributes_ShouldMergeIntoOperation() throws CrudException {
      // Arrange
      AttributePropagatingDistributedTransaction decorator =
          newDecorator(twoTransactionAttributes());
      Insert insert = buildInsert(Collections.emptyMap());
      Insert expected = buildInsert(twoTransactionAttributes());

      // Act
      decorator.insert(insert);

      // Assert
      verify(wrappedTransaction).insert(eq(expected));
    }

    @Test
    public void upsert_WithTransactionAttributes_ShouldMergeIntoOperation() throws CrudException {
      // Arrange
      AttributePropagatingDistributedTransaction decorator =
          newDecorator(twoTransactionAttributes());
      Upsert upsert = buildUpsert(Collections.emptyMap());
      Upsert expected = buildUpsert(twoTransactionAttributes());

      // Act
      decorator.upsert(upsert);

      // Assert
      verify(wrappedTransaction).upsert(eq(expected));
    }

    @Test
    public void update_WithTransactionAttributes_ShouldMergeIntoOperation() throws CrudException {
      // Arrange
      AttributePropagatingDistributedTransaction decorator =
          newDecorator(twoTransactionAttributes());
      Update update = buildUpdate(Collections.emptyMap());
      Update expected = buildUpdate(twoTransactionAttributes());

      // Act
      decorator.update(update);

      // Assert
      verify(wrappedTransaction).update(eq(expected));
    }

    @Test
    public void delete_WithTransactionAttributes_ShouldMergeIntoOperation() throws CrudException {
      // Arrange
      AttributePropagatingDistributedTransaction decorator =
          newDecorator(twoTransactionAttributes());
      Delete delete = buildDelete(Collections.emptyMap());
      Delete expected = buildDelete(twoTransactionAttributes());

      // Act
      decorator.delete(delete);

      // Assert
      verify(wrappedTransaction).delete(eq(expected));
    }

    // -------------------- precedence: operation-side wins --------------------

    @Test
    public void put_WhenOperationHasSameKey_ShouldKeepOperationValue() throws CrudException {
      // Arrange
      AttributePropagatingDistributedTransaction decorator =
          newDecorator(Collections.singletonMap(TX_ATTR_KEY_1, "true"));
      Put put = buildPut(Collections.singletonMap(TX_ATTR_KEY_1, "false"));
      Put expected = buildPut(Collections.singletonMap(TX_ATTR_KEY_1, "false"));

      // Act
      decorator.put(put);

      // Assert
      verify(wrappedTransaction).put(eq(expected));
    }

    @Test
    public void put_WhenOperationHasPartialOverlap_ShouldFillOnlyMissingKeys()
        throws CrudException {
      // Arrange
      AttributePropagatingDistributedTransaction decorator =
          newDecorator(twoTransactionAttributes());
      Put put = buildPut(Collections.singletonMap(TX_ATTR_KEY_1, "operation-wins"));
      Map<String, String> expectedAttributes = new HashMap<>();
      expectedAttributes.put(TX_ATTR_KEY_1, "operation-wins");
      expectedAttributes.put(TX_ATTR_KEY_2, TX_ATTR_VALUE_2);
      Put expected = buildPut(expectedAttributes);

      // Act
      decorator.put(put);

      // Assert
      verify(wrappedTransaction).put(eq(expected));
    }

    // -------------------- fast path: full overlap returns same instance --------------------

    @Test
    public void put_WhenOperationAlreadyHasAllKeys_ShouldForwardSameInstance()
        throws CrudException {
      // Arrange
      AttributePropagatingDistributedTransaction decorator =
          newDecorator(twoTransactionAttributes());
      Put put = buildPut(twoTransactionAttributes());

      // Act
      decorator.put(put);

      // Assert
      verify(wrappedTransaction).put(same(put));
    }

    // -------------------- empty transaction attributes --------------------

    @Test
    public void put_WhenTransactionAttributesEmpty_ShouldForwardSameInstance()
        throws CrudException {
      // Arrange
      AttributePropagatingDistributedTransaction decorator = newDecorator(Collections.emptyMap());
      Put put = buildPut(Collections.emptyMap());

      // Act
      decorator.put(put);

      // Assert
      verify(wrappedTransaction).put(same(put));
    }

    // -------------------- defensive snapshot --------------------

    @Test
    public void put_WhenTransactionAttributesMutatedAfterConstruction_ShouldUseSnapshot()
        throws CrudException {
      // Arrange
      Map<String, String> transactionAttributes = new HashMap<>();
      transactionAttributes.put(TX_ATTR_KEY_1, TX_ATTR_VALUE_1);
      AttributePropagatingDistributedTransaction decorator = newDecorator(transactionAttributes);
      transactionAttributes.put(TX_ATTR_KEY_2, "added-after");
      transactionAttributes.remove(TX_ATTR_KEY_1);

      Put put = buildPut(Collections.emptyMap());
      Put expected = buildPut(Collections.singletonMap(TX_ATTR_KEY_1, TX_ATTR_VALUE_1));

      // Act
      decorator.put(put);

      // Assert
      verify(wrappedTransaction).put(eq(expected));
    }

    // -------------------- mutate / batch --------------------

    @Test
    public void mutate_WithTransactionAttributes_ShouldMergeIntoEachMutation()
        throws CrudException {
      // Arrange
      AttributePropagatingDistributedTransaction decorator =
          newDecorator(twoTransactionAttributes());
      Put put = buildPut(Collections.emptyMap());
      Delete delete = buildDelete(Collections.singletonMap(TX_ATTR_KEY_1, "operation-wins"));
      List<Mutation> mutations = Arrays.asList(put, delete);

      Put expectedPut = buildPut(twoTransactionAttributes());
      Map<String, String> expectedDeleteAttrs = new HashMap<>();
      expectedDeleteAttrs.put(TX_ATTR_KEY_1, "operation-wins");
      expectedDeleteAttrs.put(TX_ATTR_KEY_2, TX_ATTR_VALUE_2);
      Delete expectedDelete = buildDelete(expectedDeleteAttrs);
      List<Mutation> expected = Arrays.asList(expectedPut, expectedDelete);

      // Act
      decorator.mutate(mutations);

      // Assert
      verify(wrappedTransaction).mutate(eq(expected));
    }

    @Test
    public void batch_WithTransactionAttributes_ShouldMergeIntoEachOperation()
        throws CrudException {
      // Arrange
      AttributePropagatingDistributedTransaction decorator =
          newDecorator(twoTransactionAttributes());
      Get get = buildGet(Collections.emptyMap());
      Put put = buildPut(Collections.emptyMap());
      List<Operation> operations = Arrays.asList(get, put);

      Get expectedGet = buildGet(twoTransactionAttributes());
      Put expectedPut = buildPut(twoTransactionAttributes());
      List<Operation> expected = Arrays.asList(expectedGet, expectedPut);

      when(wrappedTransaction.batch(any())).thenReturn(Collections.emptyList());

      // Act
      decorator.batch(operations);

      // Assert
      verify(wrappedTransaction).batch(eq(expected));
    }

    // -------------------- pass-through --------------------

    @Test
    public void commit_ShouldDelegate() throws Exception {
      // Arrange
      AttributePropagatingDistributedTransaction decorator =
          newDecorator(twoTransactionAttributes());

      // Act
      decorator.commit();

      // Assert
      verify(wrappedTransaction).commit();
    }

    @Test
    public void rollback_ShouldDelegate() throws Exception {
      // Arrange
      AttributePropagatingDistributedTransaction decorator =
          newDecorator(twoTransactionAttributes());

      // Act
      decorator.rollback();

      // Assert
      verify(wrappedTransaction).rollback();
    }

    @Test
    public void abort_ShouldDelegate() throws Exception {
      // Arrange
      AttributePropagatingDistributedTransaction decorator =
          newDecorator(twoTransactionAttributes());

      // Act
      decorator.abort();

      // Assert
      verify(wrappedTransaction).abort();
    }

    @Test
    public void getId_ShouldDelegate() {
      // Arrange
      AttributePropagatingDistributedTransaction decorator =
          newDecorator(twoTransactionAttributes());

      // Act
      String actual = decorator.getId();

      // Assert
      assertThat(actual).isEqualTo(TRANSACTION_ID);
    }

    @Test
    public void get_ShouldReturnDelegateResult() throws CrudException {
      // Arrange
      AttributePropagatingDistributedTransaction decorator =
          newDecorator(twoTransactionAttributes());
      Get get = buildGet(Collections.emptyMap());
      Optional<Result> expected = Optional.of(mock(Result.class));
      when(wrappedTransaction.get(any(Get.class))).thenReturn(expected);

      // Act
      Optional<Result> actual = decorator.get(get);

      // Assert
      assertThat(actual).isSameAs(expected);
    }

    // -------------------- snapshot stability across multiple calls --------------------

    @Test
    public void multipleCalls_ShouldUseSameSnapshot() throws CrudException {
      // Arrange
      AttributePropagatingDistributedTransaction decorator =
          newDecorator(twoTransactionAttributes());
      Put put1 = buildPut(Collections.emptyMap());
      Put put2 = buildPut(Collections.emptyMap());
      Put expected = buildPut(twoTransactionAttributes());

      // Act
      decorator.put(put1);
      decorator.put(put2);

      // Assert
      ArgumentCaptor<Put> captor = ArgumentCaptor.forClass(Put.class);
      verify(wrappedTransaction, times(2)).put(captor.capture());
      assertThat(captor.getAllValues()).containsExactly(expected, expected);
    }
  }
}
