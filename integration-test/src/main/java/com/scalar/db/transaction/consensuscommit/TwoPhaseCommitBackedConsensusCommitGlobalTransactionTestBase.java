package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.api.BranchTransaction;
import com.scalar.db.api.GlobalTransaction;
import com.scalar.db.api.GlobalTransactionTestBase;
import com.scalar.db.api.TwoPhaseCommitCoordinator;
import com.scalar.db.api.TwoPhaseCommitParticipant;
import com.scalar.db.common.TwoPhaseCommitBackedGlobalTransactionManager;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.service.TwoPhaseCommitFactory;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

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

  /**
   * Backing-specific scenario, so it lives here rather than in the shared corpus: the coordinator
   * observes only begin and joinParticipant — the CRUD a branch issues goes to the participant — so
   * a healthy long-running global transaction is silent from the coordinator's point of view for
   * its whole lifetime, and only the expiry-time participant probe keeps it from being reaped as
   * abandoned.
   */
  @Test
  public void
      commit_GlobalTransactionExceedingExpirationWithContinuousCrud_ShouldCommitSuccessfully()
          throws Exception {
    // Arrange: a committed record to work on, and a dedicated coordinator/participant pair whose
    // active-transaction expiration is much shorter than the transaction below.
    putThenCommit(0, 0, INITIAL_BALANCE);
    Properties properties =
        propertiesWithParticipantId(getTestName(), "participant-short-expiration");
    properties.setProperty(
        DatabaseConfig.ACTIVE_TRANSACTION_MANAGEMENT_EXPIRATION_TIME_MILLIS, "2000");
    TwoPhaseCommitFactory factory = TwoPhaseCommitFactory.create(properties);
    try (TwoPhaseCommitBackedGlobalTransactionManager shortExpirationManager =
        new TwoPhaseCommitBackedGlobalTransactionManager(
            factory.getTwoPhaseCommitCoordinator(), factory.getTwoPhaseCommitParticipant())) {
      // Act: keep issuing CRUD for ~3x the expiration, then commit. Continuous CRUD keeps the
      // participant-side idle timer fresh while the coordinator sees nothing.
      GlobalTransaction global = shortExpirationManager.beginGlobal();
      BranchTransaction branch = shortExpirationManager.beginBranch(global.getId());
      int balance = 0;
      long deadlineMillis = System.currentTimeMillis() + 6000;
      while (System.currentTimeMillis() < deadlineMillis) {
        balance = branch.get(prepareGet(0, 0)).get().getInt(BALANCE);
        TimeUnit.MILLISECONDS.sleep(100);
      }
      branch.put(preparePut(0, 0, balance + 100));
      branch.end();
      global.commit();
    }

    // Assert: the transaction outlived several expiration periods and still committed.
    assertThat(get(0, 0).get().getInt(BALANCE)).isEqualTo(INITIAL_BALANCE + 100);
  }
}
