package com.scalar.db.transaction.consensuscommit;

import static org.mockito.Mockito.spy;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.storage.cosmos.Cosmos;
import com.scalar.db.storage.cosmos.CosmosAdmin;
import java.io.IOException;
import java.util.Optional;
import java.util.Properties;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public class ConsensusCommitWithCosmosIntegrationTest extends ConsensusCommitIntegrationTestBase {

  private static Optional<String> namespacePrefix;
  private static DistributedStorage originalStorage;
  private static DatabaseConfig config;

  @BeforeClass
  public static void setUpBeforeClass() throws IOException, ExecutionException {
    String contactPoint = System.getProperty("scalardb.cosmos.uri");
    String username = System.getProperty("scalardb.cosmos.username");
    String password = System.getProperty("scalardb.cosmos.password");
    namespacePrefix = Optional.ofNullable(System.getProperty("scalardb.namespace_prefix"));

    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, contactPoint);
    props.setProperty(DatabaseConfig.USERNAME, username);
    props.setProperty(DatabaseConfig.PASSWORD, password);
    namespacePrefix.ifPresent(n -> props.setProperty(DatabaseConfig.NAMESPACE_PREFIX, n));
    config = new DatabaseConfig(props);
    originalStorage = new Cosmos(config);
    admin = new CosmosAdmin(config);
    createTables(ImmutableMap.of(CosmosAdmin.RU, "4000"));
  }

  @AfterClass
  public static void tearDownAfterClass() throws ExecutionException {
    deleteTables();
    admin.close();
    originalStorage.close();
  }

  @Before
  public void setUp() throws IOException {
    DistributedStorage storage = spy(originalStorage);
    Coordinator coordinator = spy(new Coordinator(storage));
    RecoveryHandler recovery = spy(new RecoveryHandler(storage, coordinator));
    CommitHandler commit = spy(new CommitHandler(storage, coordinator, recovery));
    ConsensusCommitManager manager =
        new ConsensusCommitManager(
            storage,
            new ConsensusCommitConfig(config.getProperties()),
            coordinator,
            recovery,
            commit);
    setUp(manager, storage, coordinator, recovery);
  }

  @After
  public void tearDown() throws ExecutionException {
    truncateTables();
  }
}
