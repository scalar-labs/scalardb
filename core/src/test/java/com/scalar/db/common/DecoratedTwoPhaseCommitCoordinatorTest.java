package com.scalar.db.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.TwoPhaseCommit;
import java.util.Collections;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class DecoratedTwoPhaseCommitCoordinatorTest {

  private static final String TX = "tx-1";

  @Mock private TwoPhaseCommit.Coordinator delegate;
  @Mock private TwoPhaseCommit.Participant participant;
  private DecoratedTwoPhaseCommitCoordinator coordinator;

  @BeforeEach
  void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
    // A no-op subclass that adds nothing — every method should delegate to the wrapped coordinator.
    coordinator = new DecoratedTwoPhaseCommitCoordinator(delegate) {};
  }

  @Test
  void begin_ShouldDelegate() throws Exception {
    when(delegate.begin(any(), eq(false), eq(Collections.emptyMap()), any())).thenReturn(TX);
    assertThat(coordinator.begin(null, false, Collections.emptyMap(), null)).isEqualTo(TX);
    verify(delegate).begin(null, false, Collections.emptyMap(), null);
  }

  @Test
  void registerParticipant_ShouldDelegate() throws Exception {
    coordinator.registerParticipant(TX, participant);
    verify(delegate).registerParticipant(TX, participant);
  }

  @Test
  void commit_ShouldDelegate() throws Exception {
    coordinator.commit(TX);
    verify(delegate).commit(TX);
  }

  @Test
  void rollback_ShouldDelegate() throws Exception {
    coordinator.rollback(TX);
    verify(delegate).rollback(TX);
  }

  @Test
  void releaseContext_ShouldDelegate() throws Exception {
    coordinator.releaseContext(TX);
    verify(delegate).releaseContext(TX);
  }

  @Test
  void close_ShouldDelegate() {
    coordinator.close();
    verify(delegate).close();
  }
}
