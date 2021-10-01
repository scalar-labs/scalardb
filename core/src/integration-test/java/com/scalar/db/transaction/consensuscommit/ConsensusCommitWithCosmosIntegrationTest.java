package com.scalar.db.transaction.consensuscommit;

import static org.mockito.Mockito.spy;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.storage.cosmos.Cosmos;
import com.scalar.db.storage.cosmos.CosmosAdmin;
import com.scalar.db.storage.cosmos.CosmosConfig;
import java.io.IOException;
import java.util.Optional;
import java.util.Properties;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public class ConsensusCommitWithCosmosIntegrationTest extends ConsensusCommitIntegrationTestBase {
  private static final String PROP_COSMOSDB_URI = "scalardb.cosmos.uri";
  private static final String PROP_COSMOSDB_PASSWORD = "scalardb.cosmos.password";
  private static final String PROP_NAMESPACE_PREFIX = "scalardb.namespace_prefix";

  private static DistributedStorage originalStorage;
  private static CosmosConfig config;

  @BeforeClass
  public static void setUpBeforeClass() throws IOException, ExecutionException {
    String contactPoint = System.getProperty(PROP_COSMOSDB_URI);
    String password = System.getProperty(PROP_COSMOSDB_PASSWORD);
    Optional<String> namespacePrefix =
        Optional.ofNullable(System.getProperty(PROP_NAMESPACE_PREFIX));

    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, contactPoint);
    props.setProperty(DatabaseConfig.PASSWORD, password);
    props.setProperty(DatabaseConfig.STORAGE, "cosmos");
    namespacePrefix.ifPresent(n -> props.setProperty(DatabaseConfig.NAMESPACE_PREFIX, n));
    config = new CosmosConfig(props);
    originalStorage = new Cosmos(config);
    admin = new CosmosAdmin(config);
    createTables(ImmutableMap.of(CosmosAdmin.REQUEST_UNIT, "4000"));
  }

  @AfterClass
  public static void tearDownAfterClass() throws ExecutionException {
    deleteTables();
    admin.close();
    originalStorage.close();
  }

  @Before
  public void setUp() throws IOException {
    ConsensusCommitConfig consensusCommitConfig = new ConsensusCommitConfig(config.getProperties());
    DistributedStorage storage = spy(originalStorage);
    Coordinator coordinator = spy(new Coordinator(storage, consensusCommitConfig));
    RecoveryHandler recovery = spy(new RecoveryHandler(storage, coordinator));
    CommitHandler commit = spy(new CommitHandler(storage, coordinator, recovery));
    ConsensusCommitManager manager =
        new ConsensusCommitManager(storage, consensusCommitConfig, coordinator, recovery, commit);
    setUp(manager, storage, coordinator, recovery);
  }

  @After
  public void tearDown() throws ExecutionException {
    truncateTables();
  }
}
