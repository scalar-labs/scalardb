package com.scalar.db.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.BranchTransaction;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Insert;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Operation;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class AttributePropagatingBranchTransactionTest {

  private static final String NS = "ns";
  private static final String TBL = "tbl";

  @Mock private BranchTransaction delegate;

  @BeforeEach
  void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
  }

  private AttributePropagatingBranchTransaction branch(Map<String, String> attributes) {
    return new AttributePropagatingBranchTransaction(delegate, attributes);
  }

  private static Get get() {
    return Get.newBuilder().namespace(NS).table(TBL).partitionKey(Key.ofInt("pk", 1)).build();
  }

  private static Insert insert(int pk) {
    return Insert.newBuilder()
        .namespace(NS)
        .table(TBL)
        .partitionKey(Key.ofInt("pk", pk))
        .intValue("v", 1)
        .build();
  }

  private static Scan scan() {
    return Scan.newBuilder().namespace(NS).table(TBL).partitionKey(Key.ofInt("pk", 1)).build();
  }

  private static Put put(int pk) {
    return Put.newBuilder()
        .namespace(NS)
        .table(TBL)
        .partitionKey(Key.ofInt("pk", pk))
        .intValue("v", 1)
        .build();
  }

  private static Upsert upsert(int pk) {
    return Upsert.newBuilder()
        .namespace(NS)
        .table(TBL)
        .partitionKey(Key.ofInt("pk", pk))
        .intValue("v", 1)
        .build();
  }

  private static Update update(int pk) {
    return Update.newBuilder()
        .namespace(NS)
        .table(TBL)
        .partitionKey(Key.ofInt("pk", pk))
        .intValue("v", 1)
        .build();
  }

  private static Delete delete(int pk) {
    return Delete.newBuilder().namespace(NS).table(TBL).partitionKey(Key.ofInt("pk", pk)).build();
  }

  private static Map<String, String> attrs(String k, String v) {
    Map<String, String> m = new HashMap<>();
    m.put(k, v);
    return m;
  }

  @Test
  void get_WithBranchAttributes_ShouldMergeAttributesIntoOperation() throws Exception {
    branch(attrs("k", "v")).get(get());

    ArgumentCaptor<Get> captor = ArgumentCaptor.forClass(Get.class);
    verify(delegate).get(captor.capture());
    assertThat(captor.getValue().getAttributes()).containsEntry("k", "v");
  }

  @Test
  void get_WhenOperationAlreadyHasTheAttribute_ShouldKeepOperationValue() throws Exception {
    Get getWithAttribute = Get.newBuilder(get()).attribute("k", "op-value").build();

    branch(attrs("k", "branch-value")).get(getWithAttribute);

    ArgumentCaptor<Get> captor = ArgumentCaptor.forClass(Get.class);
    verify(delegate).get(captor.capture());
    // Operation-level attribute wins over the per-branch one.
    assertThat(captor.getValue().getAttributes()).containsEntry("k", "op-value");
  }

  @Test
  void get_WithEmptyBranchAttributes_ShouldForwardOperationUnchanged() throws Exception {
    branch(Collections.emptyMap()).get(get());

    ArgumentCaptor<Get> captor = ArgumentCaptor.forClass(Get.class);
    verify(delegate).get(captor.capture());
    assertThat(captor.getValue().getAttributes()).isEmpty();
  }

  @Test
  void insert_WithBranchAttributes_ShouldMergeAttributesIntoOperation() throws Exception {
    branch(attrs("k", "v")).insert(insert(1));

    ArgumentCaptor<Insert> captor = ArgumentCaptor.forClass(Insert.class);
    verify(delegate).insert(captor.capture());
    assertThat(captor.getValue().getAttributes()).containsEntry("k", "v");
  }

  @Test
  void mutate_WithBranchAttributes_ShouldMergeAttributesIntoEachOperation() throws Exception {
    branch(attrs("k", "v")).mutate(Arrays.asList(insert(1), insert(2)));

    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<? extends Mutation>> captor = ArgumentCaptor.forClass(List.class);
    verify(delegate).mutate(captor.capture());
    assertThat(captor.getValue())
        .allSatisfy(m -> assertThat(m.getAttributes()).containsEntry("k", "v"));
  }

  @Test
  void scan_WithBranchAttributes_ShouldMergeAttributesIntoOperation() throws Exception {
    branch(attrs("k", "v")).scan(scan());

    ArgumentCaptor<Scan> captor = ArgumentCaptor.forClass(Scan.class);
    verify(delegate).scan(captor.capture());
    assertThat(captor.getValue().getAttributes()).containsEntry("k", "v");
  }

  @Test
  void getScanner_WithBranchAttributes_ShouldMergeAttributesIntoOperation() throws Exception {
    branch(attrs("k", "v")).getScanner(scan());

    ArgumentCaptor<Scan> captor = ArgumentCaptor.forClass(Scan.class);
    verify(delegate).getScanner(captor.capture());
    assertThat(captor.getValue().getAttributes()).containsEntry("k", "v");
  }

  @Test
  void put_WithBranchAttributes_ShouldMergeAttributesIntoOperation() throws Exception {
    branch(attrs("k", "v")).put(put(1));

    ArgumentCaptor<Put> captor = ArgumentCaptor.forClass(Put.class);
    verify(delegate).put(captor.capture());
    assertThat(captor.getValue().getAttributes()).containsEntry("k", "v");
  }

  @Test
  void put_List_WithBranchAttributes_ShouldMergeAttributesIntoEachOperation() throws Exception {
    branch(attrs("k", "v")).put(Arrays.asList(put(1), put(2)));

    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<Put>> captor = ArgumentCaptor.forClass(List.class);
    verify(delegate).put(captor.capture());
    assertThat(captor.getValue())
        .allSatisfy(p -> assertThat(p.getAttributes()).containsEntry("k", "v"));
  }

  @Test
  void upsert_WithBranchAttributes_ShouldMergeAttributesIntoOperation() throws Exception {
    branch(attrs("k", "v")).upsert(upsert(1));

    ArgumentCaptor<Upsert> captor = ArgumentCaptor.forClass(Upsert.class);
    verify(delegate).upsert(captor.capture());
    assertThat(captor.getValue().getAttributes()).containsEntry("k", "v");
  }

  @Test
  void update_WithBranchAttributes_ShouldMergeAttributesIntoOperation() throws Exception {
    branch(attrs("k", "v")).update(update(1));

    ArgumentCaptor<Update> captor = ArgumentCaptor.forClass(Update.class);
    verify(delegate).update(captor.capture());
    assertThat(captor.getValue().getAttributes()).containsEntry("k", "v");
  }

  @Test
  void delete_WithBranchAttributes_ShouldMergeAttributesIntoOperation() throws Exception {
    branch(attrs("k", "v")).delete(delete(1));

    ArgumentCaptor<Delete> captor = ArgumentCaptor.forClass(Delete.class);
    verify(delegate).delete(captor.capture());
    assertThat(captor.getValue().getAttributes()).containsEntry("k", "v");
  }

  @Test
  void delete_List_WithBranchAttributes_ShouldMergeAttributesIntoEachOperation() throws Exception {
    branch(attrs("k", "v")).delete(Arrays.asList(delete(1), delete(2)));

    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<Delete>> captor = ArgumentCaptor.forClass(List.class);
    verify(delegate).delete(captor.capture());
    assertThat(captor.getValue())
        .allSatisfy(d -> assertThat(d.getAttributes()).containsEntry("k", "v"));
  }

  @Test
  void batch_WithBranchAttributes_ShouldMergeAttributesIntoEachOperation() throws Exception {
    branch(attrs("k", "v")).batch(Arrays.asList(insert(1), delete(2)));

    @SuppressWarnings("unchecked")
    ArgumentCaptor<List<? extends Operation>> captor = ArgumentCaptor.forClass(List.class);
    verify(delegate).batch(captor.capture());
    assertThat(captor.getValue())
        .allSatisfy(o -> assertThat(o.getAttributes()).containsEntry("k", "v"));
  }

  @Test
  void getId_ShouldDelegateToBranch() {
    when(delegate.getId()).thenReturn("tx-1");

    assertThat(branch(attrs("k", "v")).getId()).isEqualTo("tx-1");
    verify(delegate).getId();
  }

  @Test
  void end_ShouldDelegateToBranch() throws Exception {
    branch(attrs("k", "v")).end();

    verify(delegate).end();
  }
}
