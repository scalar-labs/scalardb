package com.scalar.db.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Insert;
import com.scalar.db.api.TwoPhaseCommit;
import com.scalar.db.api.TwoPhaseCommitTransactionManager;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.io.Key;
import java.util.Collections;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class AbstractDistributedTransactionProviderTest {

  private TwoPhaseCommit.Coordinator rawCoordinator;
  private TwoPhaseCommit.Participant rawParticipant;
  private AbstractDistributedTransactionProvider provider;
  private DatabaseConfig config;

  @BeforeEach
  void setUp() {
    rawCoordinator = mock(TwoPhaseCommit.Coordinator.class);
    rawParticipant = mock(TwoPhaseCommit.Participant.class);
    provider =
        new AbstractDistributedTransactionProvider() {
          @Override
          public String getName() {
            return "test";
          }

          @Override
          protected DistributedTransactionManager createRawDistributedTransactionManager(
              DatabaseConfig config) {
            return mock(DistributedTransactionManager.class);
          }

          @Override
          public DistributedTransactionAdmin createDistributedTransactionAdmin(
              DatabaseConfig config) {
            return mock(DistributedTransactionAdmin.class);
          }

          @Override
          protected TwoPhaseCommitTransactionManager createRawTwoPhaseCommitTransactionManager(
              DatabaseConfig config) {
            return mock(TwoPhaseCommitTransactionManager.class);
          }

          @Override
          protected TwoPhaseCommit.Coordinator createRawTwoPhaseCommitCoordinator(
              DatabaseConfig config) {
            return rawCoordinator;
          }

          @Override
          protected TwoPhaseCommit.Participant createRawTwoPhaseCommitParticipant(
              DatabaseConfig config) {
            return rawParticipant;
          }
        };

    config = mock(DatabaseConfig.class);
    // A non-positive expiration keeps the active-transaction registry from starting a reaper
    // thread.
    when(config.getActiveTransactionManagementExpirationTimeMillis()).thenReturn(-1L);
  }

  @Test
  void createTwoPhaseCommitCoordinator_WhenActiveTransactionManagementDisabled_ShouldReturnRaw() {
    when(config.isActiveTransactionManagementEnabled()).thenReturn(false);

    TwoPhaseCommit.Coordinator coordinator = provider.createTwoPhaseCommitCoordinator(config);

    assertThat(coordinator).isSameAs(rawCoordinator);
  }

  @Test
  void createTwoPhaseCommitCoordinator_WhenActiveTransactionManagementEnabled_ShouldWrap() {
    when(config.isActiveTransactionManagementEnabled()).thenReturn(true);

    TwoPhaseCommit.Coordinator coordinator = provider.createTwoPhaseCommitCoordinator(config);

    assertThat(coordinator).isInstanceOf(ActiveTransactionManagedTwoPhaseCommitCoordinator.class);
  }

  @Test
  void createTwoPhaseCommitCoordinator_WhenUnsupported_ShouldThrowUnsupportedOperationException() {
    // Active transaction management is enabled to prove the raw factory throws before wrapping.
    when(config.isActiveTransactionManagementEnabled()).thenReturn(true);

    assertThatThrownBy(() -> unsupportedProvider().createTwoPhaseCommitCoordinator(config))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void createTwoPhaseCommitParticipant_WhenAllDisabled_ShouldReturnRaw() {
    when(config.isAttributePropagationEnabled()).thenReturn(false);
    when(config.isActiveTransactionManagementEnabled()).thenReturn(false);

    TwoPhaseCommit.Participant participant = provider.createTwoPhaseCommitParticipant(config);

    assertThat(participant).isSameAs(rawParticipant);
  }

  @Test
  void createTwoPhaseCommitParticipant_WhenOnlyAttributePropagationEnabled_ShouldWrapWithIt() {
    when(config.isAttributePropagationEnabled()).thenReturn(true);
    when(config.isActiveTransactionManagementEnabled()).thenReturn(false);

    TwoPhaseCommit.Participant participant = provider.createTwoPhaseCommitParticipant(config);

    assertThat(participant).isInstanceOf(AttributePropagatingTwoPhaseCommitParticipant.class);
  }

  @Test
  void
      createTwoPhaseCommitParticipant_WhenOnlyActiveTransactionManagementEnabled_ShouldWrapWithIt() {
    when(config.isAttributePropagationEnabled()).thenReturn(false);
    when(config.isActiveTransactionManagementEnabled()).thenReturn(true);

    TwoPhaseCommit.Participant participant = provider.createTwoPhaseCommitParticipant(config);

    assertThat(participant).isInstanceOf(ActiveTransactionManagedTwoPhaseCommitParticipant.class);
  }

  @Test
  void createTwoPhaseCommitParticipant_WhenActiveTransactionManagementEnabled_ShouldBeOutermost() {
    when(config.isAttributePropagationEnabled()).thenReturn(true);
    when(config.isActiveTransactionManagementEnabled()).thenReturn(true);

    TwoPhaseCommit.Participant participant = provider.createTwoPhaseCommitParticipant(config);

    // Active transaction management is the outermost wrapping.
    assertThat(participant).isInstanceOf(ActiveTransactionManagedTwoPhaseCommitParticipant.class);
  }

  @Test
  void
      createTwoPhaseCommitParticipant_WhenBothEnabled_ShouldPropagateAttributesInsideActiveManagement()
          throws Exception {
    when(config.isAttributePropagationEnabled()).thenReturn(true);
    when(config.isActiveTransactionManagementEnabled()).thenReturn(true);

    TwoPhaseCommit.Participant participant = provider.createTwoPhaseCommitParticipant(config);

    // Drive a join carrying a transaction attribute, then a CRUD op, through the full stack. The
    // outermost-type check above does not catch a dropped attribute-propagation layer (the type
    // would still match); this behavioral check does, by confirming the attribute reaches the raw
    // participant — proving the AttributePropagating layer is present and sits inside active
    // transaction management.
    participant.join("tx-1", false, Collections.singletonMap("k", "v"));
    Insert insert =
        Insert.newBuilder()
            .namespace("ns")
            .table("tbl")
            .partitionKey(Key.ofInt("pk", 1))
            .intValue("v", 1)
            .build();
    participant.insert("tx-1", insert);

    ArgumentCaptor<Insert> captor = ArgumentCaptor.forClass(Insert.class);
    verify(rawParticipant).insert(eq("tx-1"), captor.capture());
    assertThat(captor.getValue().getAttributes()).containsEntry("k", "v");
  }

  @Test
  void createTwoPhaseCommitParticipant_WhenUnsupported_ShouldThrowUnsupportedOperationException() {
    // Both wrappings are enabled to prove the raw factory throws before either wrapping.
    when(config.isAttributePropagationEnabled()).thenReturn(true);
    when(config.isActiveTransactionManagementEnabled()).thenReturn(true);

    assertThatThrownBy(() -> unsupportedProvider().createTwoPhaseCommitParticipant(config))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  // A provider that does not support the two-phase commit interface: its raw two-phase-commit
  // factory
  // methods throw UnsupportedOperationException.
  private AbstractDistributedTransactionProvider unsupportedProvider() {
    return new AbstractDistributedTransactionProvider() {
      @Override
      public String getName() {
        return "unsupported";
      }

      @Override
      protected DistributedTransactionManager createRawDistributedTransactionManager(
          DatabaseConfig config) {
        return mock(DistributedTransactionManager.class);
      }

      @Override
      public DistributedTransactionAdmin createDistributedTransactionAdmin(DatabaseConfig config) {
        return mock(DistributedTransactionAdmin.class);
      }

      @Override
      protected TwoPhaseCommitTransactionManager createRawTwoPhaseCommitTransactionManager(
          DatabaseConfig config) {
        return null;
      }

      @Override
      protected TwoPhaseCommit.Coordinator createRawTwoPhaseCommitCoordinator(
          DatabaseConfig config) {
        throw new UnsupportedOperationException();
      }

      @Override
      protected TwoPhaseCommit.Participant createRawTwoPhaseCommitParticipant(
          DatabaseConfig config) {
        throw new UnsupportedOperationException();
      }
    };
  }
}
