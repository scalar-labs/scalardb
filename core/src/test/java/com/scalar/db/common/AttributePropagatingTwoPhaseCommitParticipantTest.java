package com.scalar.db.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Insert;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TwoPhaseCommit;
import com.scalar.db.api.Update;
import com.scalar.db.api.Upsert;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.io.Key;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class AttributePropagatingTwoPhaseCommitParticipantTest {

  private static final String NS = "ns";
  private static final String TBL = "tbl";
  private static final String TX = "tx-1";

  @Mock private TwoPhaseCommit.Participant delegate;
  private AttributePropagatingTwoPhaseCommitParticipant participant;

  @BeforeEach
  void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
    participant = new AttributePropagatingTwoPhaseCommitParticipant(delegate);
  }

  private static Get get() {
    return Get.newBuilder().namespace(NS).table(TBL).partitionKey(Key.ofInt("pk", 1)).build();
  }

  private static Insert insert() {
    return Insert.newBuilder()
        .namespace(NS)
        .table(TBL)
        .partitionKey(Key.ofInt("pk", 1))
        .intValue("v", 1)
        .build();
  }

  private static Map<String, String> attrs(String k, String v) {
    Map<String, String> m = new HashMap<>();
    m.put(k, v);
    return m;
  }

  @Test
  void get_AfterJoinWithAttributes_ShouldMergeAttributesIntoOperation() throws Exception {
    participant.join(TX, false, attrs("k", "v"));

    participant.get(TX, get());

    ArgumentCaptor<Get> captor = ArgumentCaptor.forClass(Get.class);
    verify(delegate).get(eq(TX), captor.capture());
    assertThat(captor.getValue().getAttributes()).containsEntry("k", "v");
  }

  @Test
  void get_WhenOperationAlreadyHasTheAttribute_ShouldKeepOperationValue() throws Exception {
    participant.join(TX, false, attrs("k", "tx-value"));
    Get getWithAttribute = Get.newBuilder(get()).attribute("k", "op-value").build();

    participant.get(TX, getWithAttribute);

    ArgumentCaptor<Get> captor = ArgumentCaptor.forClass(Get.class);
    verify(delegate).get(eq(TX), captor.capture());
    // Operation-level attribute wins over the transaction-scoped one.
    assertThat(captor.getValue().getAttributes()).containsEntry("k", "op-value");
  }

  @Test
  void get_AfterJoinWithEmptyAttributes_ShouldForwardOperationUnchanged() throws Exception {
    participant.join(TX, false, Collections.emptyMap());
    Get original = get();

    participant.get(TX, original);

    // No transaction attributes → the operation is forwarded as-is (no attributes added).
    ArgumentCaptor<Get> captor = ArgumentCaptor.forClass(Get.class);
    verify(delegate).get(eq(TX), captor.capture());
    assertThat(captor.getValue().getAttributes()).isEmpty();
  }

  @Test
  void insert_AfterJoinWithAttributes_ShouldMergeAttributesIntoOperation() throws Exception {
    participant.join(TX, false, attrs("k", "v"));

    participant.insert(TX, insert());

    ArgumentCaptor<Insert> captor = ArgumentCaptor.forClass(Insert.class);
    verify(delegate).insert(eq(TX), captor.capture());
    assertThat(captor.getValue().getAttributes()).containsEntry("k", "v");
  }

  @Test
  @SuppressWarnings("deprecation")
  void put_AfterJoinWithAttributes_ShouldMergeAttributesIntoOperation() throws Exception {
    participant.join(TX, false, attrs("k", "v"));
    Put put =
        Put.newBuilder()
            .namespace(NS)
            .table(TBL)
            .partitionKey(Key.ofInt("pk", 1))
            .intValue("v", 1)
            .build();

    participant.put(TX, put);

    ArgumentCaptor<Put> captor = ArgumentCaptor.forClass(Put.class);
    verify(delegate).put(eq(TX), captor.capture());
    assertThat(captor.getValue().getAttributes()).containsEntry("k", "v");
  }

  @Test
  void upsert_AfterJoinWithAttributes_ShouldMergeAttributesIntoOperation() throws Exception {
    participant.join(TX, false, attrs("k", "v"));
    Upsert upsert =
        Upsert.newBuilder()
            .namespace(NS)
            .table(TBL)
            .partitionKey(Key.ofInt("pk", 1))
            .intValue("v", 1)
            .build();

    participant.upsert(TX, upsert);

    ArgumentCaptor<Upsert> captor = ArgumentCaptor.forClass(Upsert.class);
    verify(delegate).upsert(eq(TX), captor.capture());
    assertThat(captor.getValue().getAttributes()).containsEntry("k", "v");
  }

  @Test
  void update_AfterJoinWithAttributes_ShouldMergeAttributesIntoOperation() throws Exception {
    participant.join(TX, false, attrs("k", "v"));
    Update update =
        Update.newBuilder()
            .namespace(NS)
            .table(TBL)
            .partitionKey(Key.ofInt("pk", 1))
            .intValue("v", 1)
            .build();

    participant.update(TX, update);

    ArgumentCaptor<Update> captor = ArgumentCaptor.forClass(Update.class);
    verify(delegate).update(eq(TX), captor.capture());
    assertThat(captor.getValue().getAttributes()).containsEntry("k", "v");
  }

  @Test
  void delete_AfterJoinWithAttributes_ShouldMergeAttributesIntoOperation() throws Exception {
    participant.join(TX, false, attrs("k", "v"));
    Delete delete =
        Delete.newBuilder().namespace(NS).table(TBL).partitionKey(Key.ofInt("pk", 1)).build();

    participant.delete(TX, delete);

    ArgumentCaptor<Delete> captor = ArgumentCaptor.forClass(Delete.class);
    verify(delegate).delete(eq(TX), captor.capture());
    assertThat(captor.getValue().getAttributes()).containsEntry("k", "v");
  }

  @Test
  void scan_AfterJoinWithAttributes_ShouldMergeAttributesIntoOperation() throws Exception {
    participant.join(TX, false, attrs("k", "v"));
    Scan scan = Scan.newBuilder().namespace(NS).table(TBL).all().build();

    participant.scan(TX, scan);

    ArgumentCaptor<Scan> captor = ArgumentCaptor.forClass(Scan.class);
    verify(delegate).scan(eq(TX), captor.capture());
    assertThat(captor.getValue().getAttributes()).containsEntry("k", "v");
  }

  @Test
  void getScanner_AfterJoinWithAttributes_ShouldMergeAttributesIntoOperation() throws Exception {
    participant.join(TX, false, attrs("k", "v"));
    Scan scan = Scan.newBuilder().namespace(NS).table(TBL).all().build();

    participant.getScanner(TX, scan);

    ArgumentCaptor<Scan> captor = ArgumentCaptor.forClass(Scan.class);
    verify(delegate).getScanner(eq(TX), captor.capture());
    assertThat(captor.getValue().getAttributes()).containsEntry("k", "v");
  }

  @Test
  @SuppressWarnings("unchecked")
  void mutate_AfterJoinWithAttributes_ShouldMergeAttributesIntoEachOperation() throws Exception {
    participant.join(TX, false, attrs("k", "v"));
    Put put =
        Put.newBuilder()
            .namespace(NS)
            .table(TBL)
            .partitionKey(Key.ofInt("pk", 1))
            .intValue("v", 1)
            .build();
    Delete delete =
        Delete.newBuilder().namespace(NS).table(TBL).partitionKey(Key.ofInt("pk", 2)).build();

    participant.mutate(TX, Arrays.asList(put, delete));

    ArgumentCaptor<List<? extends Mutation>> captor = ArgumentCaptor.forClass(List.class);
    verify(delegate).mutate(eq(TX), captor.capture());
    for (Mutation mutation : captor.getValue()) {
      assertThat(mutation.getAttributes()).containsEntry("k", "v");
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void batch_AfterJoinWithAttributes_ShouldMergeAttributesIntoEachOperation() throws Exception {
    participant.join(TX, false, attrs("k", "v"));

    participant.batch(TX, Arrays.asList(get(), insert()));

    ArgumentCaptor<List<? extends Operation>> captor = ArgumentCaptor.forClass(List.class);
    verify(delegate).batch(eq(TX), captor.capture());
    for (Operation operation : captor.getValue()) {
      assertThat(operation.getAttributes()).containsEntry("k", "v");
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  void mutate_AfterJoinWithEmptyAttributes_ShouldForwardTheSameListUnchanged() throws Exception {
    participant.join(TX, false, Collections.emptyMap());
    Put put =
        Put.newBuilder()
            .namespace(NS)
            .table(TBL)
            .partitionKey(Key.ofInt("pk", 1))
            .intValue("v", 1)
            .build();
    Delete delete =
        Delete.newBuilder().namespace(NS).table(TBL).partitionKey(Key.ofInt("pk", 2)).build();
    List<Mutation> mutations = Arrays.asList(put, delete);

    participant.mutate(TX, mutations);

    // No transaction attributes → the empty-attributes fast path forwards the original list without
    // allocating a copy.
    ArgumentCaptor<List<? extends Mutation>> captor = ArgumentCaptor.forClass(List.class);
    verify(delegate).mutate(eq(TX), captor.capture());
    assertThat(captor.getValue()).isSameAs(mutations);
  }

  @Test
  @SuppressWarnings("unchecked")
  void batch_AfterJoinWithEmptyAttributes_ShouldForwardTheSameListUnchanged() throws Exception {
    participant.join(TX, false, Collections.emptyMap());
    List<Operation> operations = Arrays.asList(get(), insert());

    participant.batch(TX, operations);

    ArgumentCaptor<List<? extends Operation>> captor = ArgumentCaptor.forClass(List.class);
    verify(delegate).batch(eq(TX), captor.capture());
    assertThat(captor.getValue()).isSameAs(operations);
  }

  @Test
  void get_WithTwoTransactions_ShouldMergeEachTransactionsOwnAttributes() throws Exception {
    participant.join("tx-1", false, attrs("k", "v1"));
    participant.join("tx-2", false, attrs("k", "v2"));

    participant.get("tx-1", get());
    participant.get("tx-2", get());

    ArgumentCaptor<Get> captor1 = ArgumentCaptor.forClass(Get.class);
    verify(delegate).get(eq("tx-1"), captor1.capture());
    assertThat(captor1.getValue().getAttributes()).containsEntry("k", "v1");

    ArgumentCaptor<Get> captor2 = ArgumentCaptor.forClass(Get.class);
    verify(delegate).get(eq("tx-2"), captor2.capture());
    assertThat(captor2.getValue().getAttributes()).containsEntry("k", "v2");
  }

  @Test
  void commitRecords_ShouldForwardWithoutAttributeLogic() throws Exception {
    participant.join(TX, false, attrs("k", "v"));

    participant.commitRecords(TX, 1L);

    // The captured attributes are dropped at prepareRecords, which always precedes commitRecords,
    // so
    // commitRecords itself is pure forwarding with no attribute handling.
    verify(delegate).commitRecords(TX, 1L);
  }

  @Test
  void rollbackRecords_ShouldDropTheCapturedAttributes() throws Exception {
    participant.join(TX, false, attrs("k", "v"));

    participant.rollbackRecords(TX);

    participant.get(TX, get());
    ArgumentCaptor<Get> captor = ArgumentCaptor.forClass(Get.class);
    verify(delegate).get(eq(TX), captor.capture());
    assertThat(captor.getValue().getAttributes()).isEmpty();
  }

  @Test
  void releaseContext_ShouldDropTheCapturedAttributes() throws Exception {
    participant.join(TX, false, attrs("k", "v"));

    participant.releaseContext(TX);

    participant.get(TX, get());
    ArgumentCaptor<Get> captor = ArgumentCaptor.forClass(Get.class);
    verify(delegate).get(eq(TX), captor.capture());
    assertThat(captor.getValue().getAttributes()).isEmpty();
  }

  @Test
  void join_WhenDelegateThrows_ShouldNotCaptureAttributes() throws Exception {
    doThrow(new TransactionException("boom", TX)).when(delegate).join(eq(TX), eq(false), any());

    assertThatThrownBy(() -> participant.join(TX, false, attrs("k", "v")))
        .isInstanceOf(TransactionException.class);

    // A failed join stored nothing, so a later op for the same id merges nothing.
    participant.get(TX, get());
    ArgumentCaptor<Get> captor = ArgumentCaptor.forClass(Get.class);
    verify(delegate).get(eq(TX), captor.capture());
    assertThat(captor.getValue().getAttributes()).isEmpty();
  }

  @Test
  void prepareRecords_ShouldForwardAndDropTheCapturedAttributes() throws Exception {
    participant.join(TX, false, attrs("k", "v"));

    participant.prepareRecords(TX, 100L);

    verify(delegate).prepareRecords(TX, 100L);

    // prepareRecords ends the CRUD phase, so the captured attributes are dropped here. This is the
    // terminal step for a write-less transaction, whose commitRecords the Coordinator skips: a
    // later
    // op for the same id merges nothing.
    participant.get(TX, get());
    ArgumentCaptor<Get> captor = ArgumentCaptor.forClass(Get.class);
    verify(delegate).get(eq(TX), captor.capture());
    assertThat(captor.getValue().getAttributes()).isEmpty();
  }

  @Test
  void prepareRecords_ForOneTransaction_ShouldNotDropAnotherTransactionsAttributes()
      throws Exception {
    participant.join("tx-1", false, attrs("k", "v1"));
    participant.join("tx-2", false, attrs("k", "v2"));

    participant.prepareRecords("tx-1", 100L);

    // tx-2 is untouched by tx-1's terminal step: its attributes still merge.
    participant.get("tx-2", get());
    ArgumentCaptor<Get> captor = ArgumentCaptor.forClass(Get.class);
    verify(delegate).get(eq("tx-2"), captor.capture());
    assertThat(captor.getValue().getAttributes()).containsEntry("k", "v2");
  }

  @Test
  void validateRecords_ShouldForwardWithoutAttributeLogic() throws Exception {
    participant.join(TX, false, attrs("k", "v"));

    participant.validateRecords(TX);

    // validateRecords is pure forwarding; it does not read or drop the transaction-scoped
    // attributes.
    verify(delegate).validateRecords(TX);
  }
}
