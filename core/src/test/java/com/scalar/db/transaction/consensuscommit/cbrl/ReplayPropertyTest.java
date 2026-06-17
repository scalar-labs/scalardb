package com.scalar.db.transaction.consensuscommit.cbrl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableList;
import com.scalar.db.transaction.consensuscommit.proto.v1.Entry;
import com.scalar.db.transaction.consensuscommit.proto.v1.Key;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.jupiter.api.Test;

/**
 * Property tests for the §5 replay core (P1 confluence, P2 idempotency, P3 connectivity, P4
 * single-owner).
 */
class ReplayPropertyTest {

  // Fixed seed list (determinism rule): same seeds every run, several of them for coverage.
  private static final long[] SEEDS = {1, 2, 3, 5, 8, 13, 21, 42, 100, 777, 31337, 987654321};
  private static final int NUM_KEYS = 12;

  private static final RestoredRecordReader ABSENT = key -> RecordState.absent();

  /** P1: replaying a shuffled op stream yields the reference final state for every key. */
  @Test
  void p1_confluence_shuffledInputYieldsReferenceState() {
    for (long seed : SEEDS) {
      List<RedoOp> ops = new RedoLogGenerator(seed).generate(NUM_KEYS);
      Map<RecordKey, RecordState> expected = new ReferenceApplier().finalStates(ops);

      List<RedoOp> shuffled = new ArrayList<>(ops);
      Collections.shuffle(shuffled, new Random(seed * 31 + 1));

      Map<RecordKey, RecordState> actual = replayPerKey(shuffled, ABSENT);

      assertThat(actual.keySet()).as("seed %d keys", seed).isEqualTo(expected.keySet());
      for (RecordKey key : expected.keySet()) {
        assertThat(actual.get(key).observablyEquals(expected.get(key)))
            .as("seed %d, %s: replay=%s expected=%s", seed, key, actual.get(key), expected.get(key))
            .isTrue();
      }
    }
  }

  /**
   * P1 via the full pipeline: shuffle into N buckets, apply with M workers, compare to reference.
   */
  @Test
  void p1_confluence_throughShufflerAndApplier() throws InterruptedException {
    for (long seed : SEEDS) {
      List<RedoOp> ops = new RedoLogGenerator(seed).generate(NUM_KEYS);
      Map<RecordKey, RecordState> expected = new ReferenceApplier().finalStates(ops);

      List<RedoOp> shuffled = new ArrayList<>(ops);
      Collections.shuffle(shuffled, new Random(seed));

      for (int bucketCount : new int[] {1, 3, NUM_KEYS}) {
        List<List<RedoOp>> buckets = new RecordShuffler().shuffle(shuffled, bucketCount);
        Map<RecordKey, RecordState> actual =
            new RecordApplier(ABSENT).apply(buckets, Math.max(1, bucketCount / 2));
        for (RecordKey key : expected.keySet()) {
          assertThat(actual.get(key).observablyEquals(expected.get(key)))
              .as("seed %d, N=%d, %s", seed, bucketCount, key)
              .isTrue();
        }
      }
    }
  }

  /** P2: re-running replay from the prior run's output changes nothing. */
  @Test
  void p2_idempotency_rerunFromOutputIsNoOp() {
    for (long seed : SEEDS) {
      List<RedoOp> ops = new RedoLogGenerator(seed).generate(NUM_KEYS);

      Map<RecordKey, RecordState> firstRun = replayPerKey(ops, ABSENT);
      Map<RecordKey, RecordState> secondRun =
          replayPerKey(ops, key -> firstRun.getOrDefault(key, RecordState.absent()));

      assertThat(secondRun).as("seed %d idempotent", seed).isEqualTo(firstRun);
    }
  }

  /** P2: re-applying the same buckets to one applier is skipped via per-bucket checkpoint. */
  @Test
  void p2_idempotency_secondApplyIsCheckpointed() throws InterruptedException {
    List<RedoOp> ops = new RedoLogGenerator(7).generate(NUM_KEYS);
    List<List<RedoOp>> buckets = new RecordShuffler().shuffle(ops, 4);
    RecordApplier applier = new RecordApplier(ABSENT);

    Map<RecordKey, RecordState> first = applier.apply(buckets, 4);
    Map<RecordKey, RecordState> second = applier.apply(buckets, 4);

    assertThat(first).isNotEmpty();
    assertThat(second).as("completed buckets are skipped on re-apply").isEmpty();
  }

  /** P3: dropping a mid-chain op makes the next op dangle — replay must fail loud, naming it. */
  @Test
  void p3_connectivity_droppedMidChainOpFailsLoud() {
    // INSERT t0 -> UPDATE t1(prev t0) -> UPDATE t2(prev t1). Drop t1 so t2 dangles.
    RedoOp t0 = new RedoOp("t0", 1, write(0, null, 10));
    RedoOp t2 = new RedoOp("t2", 3, write(0, "t1", 30));
    List<RedoOp> withGap = new ArrayList<>(ImmutableList.of(t0, t2));

    assertThatThrownBy(() -> new RecordApplier(ABSENT).replayKey(t0.key(), withGap))
        .isInstanceOf(CbrlReplayException.class)
        .hasMessageContaining("t1");
  }

  /** P4: every op of a given key maps to exactly one bucket, for any bucket count. */
  @Test
  void p4_singleOwner_keyOpsShareOneBucket() {
    for (long seed : SEEDS) {
      List<RedoOp> ops = new RedoLogGenerator(seed).generate(NUM_KEYS);
      for (int bucketCount : new int[] {1, 2, 3, 7, NUM_KEYS}) {
        Map<RecordKey, Integer> bucketByKey = new LinkedHashMap<>();
        for (RedoOp op : ops) {
          int bucket = RecordShuffler.bucketOf(op.key(), bucketCount);
          Integer prior = bucketByKey.putIfAbsent(op.key(), bucket);
          if (prior != null) {
            assertThat(bucket).as("seed %d N=%d %s", seed, bucketCount, op.key()).isEqualTo(prior);
          }
        }
      }
    }
  }

  private static Map<RecordKey, RecordState> replayPerKey(
      List<RedoOp> ops, RestoredRecordReader reader) {
    Map<RecordKey, List<RedoOp>> byKey = new LinkedHashMap<>();
    for (RedoOp op : ops) {
      byKey.computeIfAbsent(op.key(), k -> new ArrayList<>()).add(op);
    }
    RecordApplier applier = new RecordApplier(reader);
    Map<RecordKey, RecordState> result = new LinkedHashMap<>();
    for (Map.Entry<RecordKey, List<RedoOp>> entry : byKey.entrySet()) {
      result.put(entry.getKey(), applier.replayKey(entry.getKey(), entry.getValue()));
    }
    return result;
  }

  private static Entry write(int keyIndex, String prevTxId, int value) {
    Entry.Builder builder =
        Entry.newBuilder()
            .setEntryType(Entry.EntryType.ENTRY_TYPE_WRITE)
            .setNamespaceName(RedoLogGenerator.NAMESPACE)
            .setTableName(RedoLogGenerator.TABLE)
            .setPartitionKey(
                Key.newBuilder()
                    .addColumns(RedoLogGenerator.intColumn(RedoLogGenerator.PK, keyIndex)))
            .addColumns(RedoLogGenerator.intColumn(RedoLogGenerator.COL_V, value));
    if (prevTxId != null) {
      builder.setPrevTxId(prevTxId);
    }
    return builder.build();
  }
}
