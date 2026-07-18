package com.scalar.db.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.BranchTransaction;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.GlobalTransaction;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class DistributedTransactionBackedGlobalTransactionManagerTest {

  @Mock private DistributedTransactionManager manager;
  @Mock private DistributedTransaction transaction;

  private DistributedTransactionBackedGlobalTransactionManager globalManager;

  @BeforeEach
  void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
    globalManager = new DistributedTransactionBackedGlobalTransactionManager(manager);
  }

  private static Map<String, String> attrs(String k, String v) {
    Map<String, String> m = new HashMap<>();
    m.put(k, v);
    return m;
  }

  @Test
  void beginGlobal_ShouldDelegateToManagerBeginAndWrapTransaction() throws Exception {
    Map<String, String> attributes = attrs("k", "v");
    when(manager.begin(attributes)).thenReturn(transaction);
    when(transaction.getId()).thenReturn("tx-1");

    GlobalTransaction global = globalManager.beginGlobal(attributes);

    verify(manager).begin(attributes);
    assertThat(global).isInstanceOf(DistributedTransactionBackedGlobalTransaction.class);
    assertThat(global.getId()).isEqualTo("tx-1");
  }

  @Test
  void beginGlobalReadOnly_ShouldDelegateToManagerBeginReadOnlyAndWrapTransaction()
      throws Exception {
    Map<String, String> attributes = attrs("k", "v");
    when(manager.beginReadOnly(attributes)).thenReturn(transaction);
    when(transaction.getId()).thenReturn("tx-2");

    GlobalTransaction global = globalManager.beginGlobalReadOnly(attributes);

    verify(manager).beginReadOnly(attributes);
    assertThat(global).isInstanceOf(DistributedTransactionBackedGlobalTransaction.class);
    assertThat(global.getId()).isEqualTo("tx-2");
  }

  @Test
  void beginBranch_WithEmptyAttributes_ShouldReturnPlainBranchTransaction() throws Exception {
    when(manager.join("tx-1")).thenReturn(transaction);

    BranchTransaction branch = globalManager.beginBranch("tx-1", Collections.emptyMap());

    verify(manager).join("tx-1");
    assertThat(branch).isInstanceOf(DistributedTransactionBackedBranchTransaction.class);
  }

  @Test
  void beginBranch_WithNonEmptyAttributes_ShouldReturnAttributePropagatingBranchTransaction()
      throws Exception {
    when(manager.join("tx-1")).thenReturn(transaction);

    BranchTransaction branch = globalManager.beginBranch("tx-1", attrs("k", "v"));

    verify(manager).join("tx-1");
    assertThat(branch).isInstanceOf(AttributePropagatingBranchTransaction.class);
  }

  @Test
  void close_ShouldDelegateToManagerClose() {
    globalManager.close();

    verify(manager).close();
  }
}
