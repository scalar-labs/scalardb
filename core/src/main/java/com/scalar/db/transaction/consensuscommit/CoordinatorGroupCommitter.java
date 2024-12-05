package com.scalar.db.transaction.consensuscommit;

import com.scalar.db.util.groupcommit.DefaultGroupCommitKeyManipulator;
import com.scalar.db.util.groupcommit.GroupCommitConfig;
import com.scalar.db.util.groupcommit.GroupCommitter;
import java.util.Optional;

public class CoordinatorGroupCommitter
    extends GroupCommitter<String, String, String, String, String, Snapshot> {
  CoordinatorGroupCommitter(GroupCommitConfig config) {
    super("coordinator", config, new CoordinatorGroupCommitKeyManipulator());
  }

  public CoordinatorGroupCommitter(ConsensusCommitConfig config) {
    this(
        new GroupCommitConfig(
            config.getCoordinatorGroupCommitSlotCapacity(),
            config.getCoordinatorGroupCommitGroupSizeFixTimeoutMillis(),
            config.getCoordinatorGroupCommitDelayedSlotMoveTimeoutMillis(),
            config.getCoordinatorGroupCommitOldGroupAbortTimeoutMillis(),
            config.getCoordinatorGroupCommitTimeoutCheckIntervalMillis(),
            config.isCoordinatorGroupCommitMetricsMonitorLogEnabled()));
  }

  public static Optional<CoordinatorGroupCommitter> from(ConsensusCommitConfig config) {
    if (isEnabled(config)) {
      return Optional.of(new CoordinatorGroupCommitter(config));
    } else {
      return Optional.empty();
    }
  }

  public static boolean isEnabled(ConsensusCommitConfig config) {
    return config.isCoordinatorGroupCommitEnabled();
  }

  // The behavior of this class is completely the same as the parent class for now.
  public static class CoordinatorGroupCommitKeyManipulator
      extends DefaultGroupCommitKeyManipulator {}
}
