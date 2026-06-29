package com.scalar.db.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.Get;
import com.scalar.db.api.Insert;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TwoPhaseCommit;
import com.scalar.db.io.Key;
import java.util.Collections;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class DecoratedTwoPhaseCommitParticipantTest {

  private static final String NS = "ns";
  private static final String TBL = "tbl";
  private static final String TX = "tx-1";

  @Mock private TwoPhaseCommit.Participant delegate;
  private DecoratedTwoPhaseCommitParticipant participant;

  @BeforeEach
  void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
    // A no-op subclass that adds nothing — every method should delegate to the wrapped participant.
    participant = new DecoratedTwoPhaseCommitParticipant(delegate) {};
  }

  @Test
  void getId_ShouldDelegate() {
    when(delegate.getId()).thenReturn("participant-1");
    assertThat(participant.getId()).isEqualTo("participant-1");
    verify(delegate).getId();
  }

  @Test
  void join_ShouldDelegate() throws Exception {
    participant.join(TX, true, Collections.emptyMap());
    verify(delegate).join(TX, true, Collections.emptyMap());
  }

  @Test
  void get_ShouldDelegate() throws Exception {
    Get get = Get.newBuilder().namespace(NS).table(TBL).partitionKey(Key.ofInt("pk", 1)).build();
    when(delegate.get(eq(TX), any(Get.class))).thenReturn(Optional.empty());
    assertThat(participant.get(TX, get)).isEmpty();
    verify(delegate).get(TX, get);
  }

  @Test
  void scan_ShouldDelegate() throws Exception {
    Scan scan = Scan.newBuilder().namespace(NS).table(TBL).all().build();
    participant.scan(TX, scan);
    verify(delegate).scan(TX, scan);
  }

  @Test
  void insert_ShouldDelegate() throws Exception {
    Insert insert =
        Insert.newBuilder()
            .namespace(NS)
            .table(TBL)
            .partitionKey(Key.ofInt("pk", 1))
            .intValue("v", 1)
            .build();
    participant.insert(TX, insert);
    verify(delegate).insert(TX, insert);
  }

  @Test
  void prepareRecords_ShouldDelegate() throws Exception {
    participant.prepareRecords(TX, 100L);
    verify(delegate).prepareRecords(TX, 100L);
  }

  @Test
  void validateRecords_ShouldDelegate() throws Exception {
    participant.validateRecords(TX);
    verify(delegate).validateRecords(TX);
  }

  @Test
  void commitRecords_ShouldDelegate() throws Exception {
    participant.commitRecords(TX, 200L);
    verify(delegate).commitRecords(TX, 200L);
  }

  @Test
  void rollbackRecords_ShouldDelegate() throws Exception {
    participant.rollbackRecords(TX);
    verify(delegate).rollbackRecords(TX);
  }

  @Test
  void releaseContext_ShouldDelegate() {
    participant.releaseContext(TX);
    verify(delegate).releaseContext(TX);
  }

  @Test
  void close_ShouldDelegate() {
    participant.close();
    verify(delegate).close();
  }
}
