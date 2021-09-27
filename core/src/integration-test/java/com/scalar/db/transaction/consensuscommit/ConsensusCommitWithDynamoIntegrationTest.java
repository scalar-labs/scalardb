package com.scalar.db.transaction.consensuscommit;

import static org.mockito.Mockito.spy;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.storage.dynamo.Dynamo;
import com.scalar.db.storage.dynamo.DynamoAdmin;
import com.scalar.db.storage.dynamo.DynamoConfig;
import java.util.Optional;
import java.util.Properties;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public class ConsensusCommitWithDynamoIntegrationTest extends ConsensusCommitIntegrationTestBase {
  private static Optional<String> namespacePrefix;
  private static DistributedStorage originalStorage;
  private static DynamoConfig config;

  @Before
  public void setUp() {
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

  @BeforeClass
  public static void setUpBeforeClass() throws ExecutionException {
    String endpointOverride =
        System.getProperty("scalardb.dynamo.endpoint_override", "http://localhost:8000");
    String region = System.getProperty("scalardb.dynamo.region", "us-west-2");
    String accessKeyId = System.getProperty("scalardb.dynamo.access_key_id", "fakeMyKeyId");
    String secretAccessKey =
        System.getProperty("scalardb.dynamo.secret_access_key", "fakeSecretAccessKey");
    namespacePrefix = Optional.ofNullable(System.getProperty("scalardb.namespace_prefix"));

    Properties props = new Properties();
    if (endpointOverride != null) {
      props.setProperty(DynamoConfig.ENDPOINT_OVERRIDE, endpointOverride);
    }
    props.setProperty(DatabaseConfig.STORAGE, "dynamo");
    props.setProperty(DatabaseConfig.CONTACT_POINTS, region);
    props.setProperty(DatabaseConfig.USERNAME, accessKeyId);
    props.setProperty(DatabaseConfig.PASSWORD, secretAccessKey);
    namespacePrefix.ifPresent(n -> props.setProperty(DatabaseConfig.NAMESPACE_PREFIX, n));
    config = new DynamoConfig(props);
    originalStorage = new Dynamo(config);
    admin = new DynamoAdmin(new DynamoConfig(props));

    createTables(
        ImmutableMap.of(
            DynamoAdmin.REQUEST_UNIT, "4000", "no-scaling", "true", "no-backup", "true"));
  }

  @AfterClass
  public static void tearDownAfterClass() throws ExecutionException {
    deleteTables();
    admin.close();
    originalStorage.close();
  }
}
