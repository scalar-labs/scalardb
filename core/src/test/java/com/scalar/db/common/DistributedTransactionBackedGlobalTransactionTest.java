package com.scalar.db.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.DistributedTransaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class DistributedTransactionBackedGlobalTransactionTest {

  @Mock private DistributedTransaction transaction;

  private DistributedTransactionBackedGlobalTransaction global;

  @BeforeEach
  void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
    global = new DistributedTransactionBackedGlobalTransaction(transaction);
  }

  @Test
  void getId_ShouldDelegateToTransaction() {
    when(transaction.getId()).thenReturn("tx-1");

    assertThat(global.getId()).isEqualTo("tx-1");
    verify(transaction).getId();
  }

  @Test
  void commit_ShouldDelegateToTransaction() throws Exception {
    global.commit();

    verify(transaction).commit();
  }

  @Test
  void rollback_ShouldDelegateToTransaction() throws Exception {
    global.rollback();

    verify(transaction).rollback();
  }
}
