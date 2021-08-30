package com.scalar.db.transaction.consensuscommit;

import static org.mockito.Mockito.spy;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.storage.cassandra.Cassandra;
import com.scalar.db.storage.cassandra.CassandraAdmin;
import java.util.Collections;
import java.util.Properties;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public class ConsensusCommitWithCassandraIntegrationTest
    extends ConsensusCommitIntegrationTestBase {

  private static final String USERNAME = "cassandra";
  private static final String PASSWORD = "cassandra";
  private static final String CONTACT_POINT = "localhost";

  private static DistributedStorage originalStorage;
  private static DatabaseConfig config;

  @BeforeClass
  public static void setUpBeforeClass() throws ExecutionException {
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, CONTACT_POINT);
    props.setProperty(DatabaseConfig.USERNAME, USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, PASSWORD);
    config = new DatabaseConfig(props);
    admin = new CassandraAdmin(config);
    createTables(Collections.emptyMap());

    originalStorage = new Cassandra(config);
  }

  @AfterClass
  public static void tearDownAfterClass() throws ExecutionException {
    originalStorage.close();
    deleteTables();
  }

  @Before
  public void setUp() {
    DistributedStorage storage = spy(originalStorage);
    Coordinator coordinator = spy(new Coordinator(storage));
    RecoveryHandler recovery = spy(new RecoveryHandler(storage, coordinator));
    CommitHandler commit = spy(new CommitHandler(storage, coordinator, recovery));
    ConsensusCommitManager manager =
        new ConsensusCommitManager(storage, config, coordinator, recovery, commit);
    setUp(manager, storage, coordinator, recovery);
  }

  @After
  public void tearDown() throws ExecutionException {
    truncateTables();
  }
}
