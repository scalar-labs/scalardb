package com.scalar.db.storage.dynamo;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.storage.IntegrationTestBase;
import java.util.Optional;
import java.util.Properties;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;

public class DynamoIntegrationTest extends IntegrationTestBase {
  private static Optional<String> namespacePrefix;
  private static DistributedStorage storage;

  @Before
  public void setUp() {
    storage.with(NAMESPACE, TABLE);
    setUp(storage);
  }

  @After
  public void tearDown() throws ExecutionException {
    // truncate the TABLE
    deleteData();
  }

  // Ignore this test for now since the DynamoDB adapter doesn't support scan with exclusive range
  @Ignore
  @Override
  public void scan_ScanWithEndInclusiveRangeGiven_ShouldRetrieveResultsOfGivenRange() {}

  // Ignore this test for now since the DynamoDB adapter doesn't support scan with exclusive range
  @Ignore
  @Override
  public void scan_ScanWithStartInclusiveRangeGiven_ShouldRetrieveResultsOfGivenRange() {}

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
    props.setProperty(DatabaseConfig.CONTACT_POINTS, region);
    props.setProperty(DatabaseConfig.USERNAME, accessKeyId);
    props.setProperty(DatabaseConfig.PASSWORD, secretAccessKey);
    props.setProperty(DatabaseConfig.STORAGE, "dynamo");
    namespacePrefix.ifPresent(n -> props.setProperty(DatabaseConfig.NAMESPACE_PREFIX, n));
    DynamoConfig config = new DynamoConfig(props);
    admin = new DynamoAdmin(config);
    storage = new Dynamo(config);
    createTable(ImmutableMap.of("no-scaling", "true", "no-backup", "true"));
  }

  @AfterClass
  public static void tearDownAfterClass() throws ExecutionException {
    deleteTable();
    admin.close();
    storage.close();
  }
}
