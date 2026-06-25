package com.scalar.db.transaction.consensuscommit.cbrl;

import static com.scalar.db.config.ConfigUtils.getInt;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig;
import com.scalar.db.transaction.consensuscommit.Coordinator;
import java.util.Properties;
import javax.annotation.concurrent.ThreadSafe;

/**
 * CBRL restore configuration derived from ScalarDB {@link Properties} (the common ScalarDB
 * approach, mirroring {@code LogApplierConfig}). The restore namespace is an operation argument,
 * but everything else — the coordinator namespace (from {@link ConsensusCommitConfig}) and the
 * replay concurrency — comes from the configuration, so callers never hand-derive it.
 */
@ThreadSafe
public final class CbrlConfig {
  public static final String PREFIX = ConsensusCommitConfig.PREFIX + "cbrl.restore.";
  public static final String REPLAY_BUCKETS = PREFIX + "replay_buckets";
  public static final String REPLAY_WORKERS = PREFIX + "replay_workers";

  private static final int DEFAULT_REPLAY_BUCKETS = 8;
  private static final int DEFAULT_REPLAY_WORKERS = 4;

  private final String coordinatorNamespace;
  private final int replayBuckets;
  private final int replayWorkers;

  public CbrlConfig(Properties properties) {
    coordinatorNamespace =
        new ConsensusCommitConfig(new DatabaseConfig(properties))
            .getCoordinatorNamespace()
            .orElse(Coordinator.NAMESPACE);
    replayBuckets = getInt(properties, REPLAY_BUCKETS, DEFAULT_REPLAY_BUCKETS);
    replayWorkers = getInt(properties, REPLAY_WORKERS, DEFAULT_REPLAY_WORKERS);
  }

  public String getCoordinatorNamespace() {
    return coordinatorNamespace;
  }

  public int getReplayBuckets() {
    return replayBuckets;
  }

  public int getReplayWorkers() {
    return replayWorkers;
  }
}
