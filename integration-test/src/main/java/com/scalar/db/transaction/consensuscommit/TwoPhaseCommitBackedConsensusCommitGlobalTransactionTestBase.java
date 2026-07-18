package com.scalar.db.transaction.consensuscommit;

import com.scalar.db.api.GlobalTransactionTestBase;
import com.scalar.db.api.TwoPhaseCommitCoordinator;
import com.scalar.db.api.TwoPhaseCommitParticipant;
import com.scalar.db.common.TwoPhaseCommitBackedGlobalTransactionManager;
import com.scalar.db.service.TwoPhaseCommitFactory;
import java.util.Properties;

/**
 * Runs the {@link GlobalTransactionTestBase} corpus against the consensus-commit implementation.
 *
 * <p>The two managers share a single {@link TwoPhaseCommitCoordinator} but front two distinct
 * {@link TwoPhaseCommitParticipant}s (distinct participant IDs). A global transaction begun on
 * {@code manager1} and joined by a branch on {@code manager2} is therefore prepared and committed
 * across both participants by the one shared coordinator — a genuine two-participant two-phase
 * commit.
 */
public abstract class TwoPhaseCommitBackedConsensusCommitGlobalTransactionTestBase
    extends GlobalTransactionTestBase {

  private static final String PARTICIPANT_ID_1 = "participant-1";
  private static final String PARTICIPANT_ID_2 = "participant-2";

  private TwoPhaseCommitCoordinator coordinator;
  private TwoPhaseCommitParticipant participant1;
  private TwoPhaseCommitParticipant participant2;

  @Override
  protected String getTestName() {
    return "global_tx_cc";
  }

  @Override
  protected final Properties getProperties(String testName) {
    // Used for the admin (schema); the consensus-commit participant requires a participant ID.
    return propertiesWithParticipantId(testName, PARTICIPANT_ID_1);
  }

  protected abstract Properties getProps(String testName);

  @Override
  protected void setUpManagers() {
    TwoPhaseCommitFactory factory1 =
        TwoPhaseCommitFactory.create(propertiesWithParticipantId(getTestName(), PARTICIPANT_ID_1));
    TwoPhaseCommitFactory factory2 =
        TwoPhaseCommitFactory.create(propertiesWithParticipantId(getTestName(), PARTICIPANT_ID_2));
    // One shared coordinator; two distinct participants (one per manager).
    coordinator = factory1.getTwoPhaseCommitCoordinator();
    participant1 = factory1.getTwoPhaseCommitParticipant();
    participant2 = factory2.getTwoPhaseCommitParticipant();
    manager1 = new TwoPhaseCommitBackedGlobalTransactionManager(coordinator, participant1);
    manager2 = new TwoPhaseCommitBackedGlobalTransactionManager(coordinator, participant2);
  }

  @Override
  protected void tearDownManagers() {
    // The managers share the coordinator, so close the underlying roles directly (once each) rather
    // than via manager.close(), which would close the shared coordinator twice.
    closeQuietly(participant1);
    closeQuietly(participant2);
    closeQuietly(coordinator);
  }

  private Properties propertiesWithParticipantId(String testName, String participantId) {
    Properties properties = new Properties();
    properties.putAll(getProps(testName));
    properties.setProperty(ConsensusCommitConfig.PARTICIPANT_ID, participantId);
    return properties;
  }

  private static void closeQuietly(AutoCloseable closeable) {
    if (closeable == null) {
      return;
    }
    try {
      closeable.close();
    } catch (Exception ignored) {
      // Best-effort close during teardown.
    }
  }
}
