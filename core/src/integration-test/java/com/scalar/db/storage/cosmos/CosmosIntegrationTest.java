package com.scalar.db.storage.cosmos;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.storage.IntegrationTestBase;
import java.io.IOException;
import java.util.HashMap;
import java.util.Optional;
import java.util.Properties;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public class CosmosIntegrationTest extends IntegrationTestBase {
  private static final String PROP_COSMOSDB_URI = "scalardb.cosmos.uri";
  private static final String PROP_COSMOSDB_USERNAME = "scalardb.cosmos.username";
  private static final String PROP_COSMOSDB_PASSWORD = "scalardb.cosmos.password";
  private static final String PROP_NAMESPACE_PREFIX = "scalardb.namespace_prefix";

  private static DistributedStorage storage;

  @BeforeClass
  public static void setUpBeforeClass() throws IOException, ExecutionException {
    String contactPoint = System.getProperty(PROP_COSMOSDB_URI);
    String username = System.getProperty(PROP_COSMOSDB_USERNAME);
    String password = System.getProperty(PROP_COSMOSDB_PASSWORD);
    Optional<String> namespacePrefix =
        Optional.ofNullable(System.getProperty(PROP_NAMESPACE_PREFIX));

    // reuse this storage instance through the tests
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, contactPoint);
    props.setProperty(DatabaseConfig.USERNAME, username);
    props.setProperty(DatabaseConfig.PASSWORD, password);
    namespacePrefix.ifPresent(n -> props.setProperty(DatabaseConfig.NAMESPACE_PREFIX, n));

    DatabaseConfig config = new DatabaseConfig(props);
    admin = new CosmosAdmin(config);
    storage = new Cosmos(config);
    HashMap<String, String> options = new HashMap<>();
    options.put(CosmosAdmin.RU, "5000");
    createTable(options);
  }

  @AfterClass
  public static void tearDownAfterClass() throws ExecutionException {
    deleteTable();
    admin.close();
    storage.close();
  }

  @Before
  public void setUp() throws Exception {
    storage.with(NAMESPACE, TABLE);
    setUp(storage);
  }

  @After
  public void tearDown() throws ExecutionException {
    deleteData();
  }
}
