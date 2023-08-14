package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.azure.cosmos.implementation.guava25.collect.ImmutableList;
import com.azure.cosmos.implementation.guava25.collect.ImmutableSet;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Column;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Key;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Record;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Record.Value;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.ReplicationRecordRepository;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.server.RecordWriterThread.NextValue;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

class RecordWriterThreadTest {
  private final SecureRandom random = new SecureRandom();
  private RecordWriterThread recordWriterThread;
  @Mock BlockingQueue<com.scalar.db.io.Key> blockingQueue;

  @BeforeEach
  void setUp() {
    recordWriterThread =
        new RecordWriterThread(
            1,
            mock(ReplicationRecordRepository.class),
            mock(DistributedStorage.class),
            blockingQueue,
            mock(MetricsLogger.class));
  }

  @AfterEach
  void tearDown() {
    if (recordWriterThread != null) {
      recordWriterThread.close();
    }
  }

  private <T> Set<T> shuffledSet(List<T> orig) {
    List<T> list = new ArrayList<>(orig);
    list.sort((a, b) -> random.nextBoolean() ? 1 : -1);
    return ImmutableSet.<T>builder().addAll(list).build();
  }

  @Test
  void findNextValue_withoutRetry_shouldResultInExpectedState() {
    Key key = new Key(Collections.singletonList(new Column<>("k", 42)));
    Set<Value> values =
        shuffledSet(
            ImmutableList.<Value>builder()
                .add(
                    new Value(
                        "t1",
                        "t2",
                        2,
                        0,
                        0,
                        "update",
                        ImmutableList.of(new Column<>("c1", 2), new Column<>("c3", 222))))
                .add(
                    new Value(
                        "t0", "t1", 1, 0, 0, "update", ImmutableList.of(new Column<>("c2", 11))))
                .add(
                    new Value(
                        "t2",
                        "t3",
                        3,
                        0,
                        0,
                        "update",
                        ImmutableList.of(new Column<>("c1", 12), new Column<>("c2", 1212))))
                .add(
                    new Value(
                        null,
                        "t5",
                        1,
                        0,
                        0,
                        "insert",
                        ImmutableList.of(new Column<>("c1", 1), new Column<>("c4", 1111))))
                .add(new Value("t3", "t4", 3, 0, 0, "delete", ImmutableList.of()))
                .build());

    Record record =
        new Record(
            "ns",
            "tbl",
            key,
            new Key(ImmutableList.of()),
            1,
            "t0",
            null,
            values,
            ImmutableSet.of(),
            null,
            null);
    NextValue result = recordWriterThread.findNextValue(Key.toScalarDbKey(key), record);
    assertThat(result).isNotNull();
    assertThat(result.nextValue.prevTxId).isNull();
    assertThat(result.nextValue.txId).isEqualTo("t5");
    assertThat(result.nextValue.txVersion).isEqualTo(1);
    assertThat(result.nextValue.type).isEqualTo("insert");
    assertThat(result.restValues).isEmpty();
    assertThat(result.updatedColumns).size().isEqualTo(2);
    assertThat(result.updatedColumns).contains(new Column<>("c1", 1), new Column<>("c4", 1111));
    assertThat(result.shouldHandleTheSameKey).isFalse();
  }

  @Test
  void findNextValue_withRetry_shouldResultInExpectedState() {
    Key key = new Key(Collections.singletonList(new Column<>("k", 42)));
    // Assuming the following situation
    // - an insert operation (I0) in T0 and some update operations in other transactions happened.
    // - and then, a delete operation (D10) happened in T10.
    // - an insert operation (I11) in T11 and an update operation (U12) in T12
    //   followed the delete operation. But I11 is delayed.
    // - the old insert operation (I0) is also handled and added by other Distributor thread.
    Set<Value> values =
        shuffledSet(
            ImmutableList.<Value>builder()
                .add(
                    new Value(
                        "t11",
                        "t12",
                        2,
                        0,
                        0,
                        "update",
                        ImmutableList.of(new Column<>("c1", 2), new Column<>("c3", 222))))
                .add(
                    new Value(
                        null,
                        "t1",
                        1,
                        0,
                        0,
                        "insert",
                        ImmutableList.of(new Column<>("c1", 1), new Column<>("c3", 111))))
                .build());

    Record record =
        new Record(
            "ns",
            "tbl",
            key,
            new Key(ImmutableList.of()),
            10,
            null,
            null,
            values,
            ImmutableSet.of("t1"),
            null,
            null);

    // the insert handled again is already done, so it should be skipped
    NextValue result = recordWriterThread.findNextValue(Key.toScalarDbKey(key), record);
    assertThat(result).isNull();
  }

  @Test
  void findNextValue_givenInsertAndFollowingOperations_shouldReturnOnlyWithInsert() {
    Key key = new Key(Collections.singletonList(new Column<>("k", 42)));
    Set<Value> values =
        shuffledSet(
            ImmutableList.<Value>builder()
                .add(
                    new Value(
                        null, "t0", 1, 0, 0, "insert", ImmutableList.of(new Column<>("c1", 2))))
                .add(
                    new Value(
                        "t0", "t1", 2, 0, 0, "update", ImmutableList.of(new Column<>("c1", 2))))
                .build());

    Record record =
        new Record(
            "ns",
            "tbl",
            key,
            new Key(ImmutableList.of()),
            1,
            null,
            null,
            values,
            ImmutableSet.of(),
            null,
            null);
    NextValue result = recordWriterThread.findNextValue(Key.toScalarDbKey(key), record);
    assertThat(result).isNotNull();
    assertThat(result.nextValue.prevTxId).isNull();
    assertThat(result.nextValue.txId).isEqualTo("t0");
    assertThat(result.nextValue.txVersion).isEqualTo(1);
    assertThat(result.nextValue.type).isEqualTo("insert");
    assertThat(result.restValues.size()).isEqualTo(1);
    assertThat(result.restValues)
        .contains(
            new Value("t0", "t1", 2, 0, 0, "update", ImmutableList.of(new Column<>("c1", 2))));
    assertThat(result.updatedColumns).size().isEqualTo(1);
    assertThat(result.updatedColumns).contains(new Column<>("c1", 1));
    assertThat(result.shouldHandleTheSameKey).isTrue();
  }
}
