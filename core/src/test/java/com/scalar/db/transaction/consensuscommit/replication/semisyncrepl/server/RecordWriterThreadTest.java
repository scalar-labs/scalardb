package com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

import com.azure.cosmos.implementation.guava25.collect.ImmutableList;
import com.azure.cosmos.implementation.guava25.collect.ImmutableSet;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Column;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Key;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Record;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.model.Record.Value;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.repository.ReplicationRecordRepository;
import com.scalar.db.transaction.consensuscommit.replication.semisyncrepl.server.RecordWriterThread.NextValueAndRest;
import java.util.Collections;
import java.util.concurrent.BlockingQueue;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

class RecordWriterThreadTest {
  @Mock BlockingQueue<com.scalar.db.io.Key> blockingQueue;

  @Test
  void findNextValue() {
    RecordWriterThread recordWriterThread =
        new RecordWriterThread(
            1,
            mock(ReplicationRecordRepository.class),
            mock(DistributedStorage.class),
            blockingQueue,
            mock(MetricsLogger.class));

    Key key = new Key(Collections.singletonList(new Column<>("k", 42)));
    Record record =
        new Record(
            "ns",
            "tbl",
            key,
            new Key(ImmutableList.of()),
            7,
            "0000",
            null,
            new ImmutableSet.Builder<Value>()
                .add(
                    new Value(
                        "1111",
                        "2222",
                        11,
                        0,
                        0,
                        "update",
                        ImmutableList.of(new Column<>("c1", 3), new Column<>("c3", 333))))
                .add(
                    new Value(
                        "0000",
                        "1111",
                        10,
                        0,
                        0,
                        "update",
                        ImmutableList.of(new Column<>("c2", 22))))
                .add(
                    new Value(
                        "2222",
                        "3333",
                        12,
                        0,
                        0,
                        "update",
                        ImmutableList.of(new Column<>("c1", 4), new Column<>("c2", 44))))
                .add(
                    new Value(
                        null,
                        "5555",
                        1,
                        0,
                        0,
                        "insert",
                        ImmutableList.of(new Column<>("c1", 1), new Column<>("c4", 1111))))
                .add(new Value("3333", "4444", 13, 0, 0, "delete", ImmutableList.of()))
                .build(),
            null,
            null);
    NextValueAndRest result = recordWriterThread.findNextValue(Key.toScalarDbKey(key), record);
    assertThat(result).isNotNull();
    assertThat(result.nextValue.prevTxId).isNull();
    assertThat(result.nextValue.txId).isEqualTo("5555");
    assertThat(result.nextValue.txVersion).isEqualTo(1);
    assertThat(result.nextValue.type).isEqualTo("insert");
    assertThat(result.restValues).isEmpty();
    assertThat(result.updatedColumns).size().isEqualTo(2);
    assertThat(result.updatedColumns).contains(new Column<>("c1", 1), new Column<>("c4", 1111));
  }
}
