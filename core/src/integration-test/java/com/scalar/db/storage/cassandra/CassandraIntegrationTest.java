package com.scalar.db.storage.cassandra;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.IntegrationTestBase;
import java.util.Collections;
import java.util.Properties;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public class CassandraIntegrationTest extends IntegrationTestBase {

  private static final String CONTACT_POINT = "localhost";
  private static final String USERNAME = "cassandra";
  private static final String PASSWORD = "cassandra";
  private static DistributedStorage storage;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // reuse this storage instance through the tests
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, CONTACT_POINT);
    props.setProperty(DatabaseConfig.USERNAME, USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, PASSWORD);
    DatabaseConfig config = new DatabaseConfig(props);

    admin = new CassandraAdmin(config);
    createTable(Collections.emptyMap());
    storage = new Cassandra(config);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    deleteTable();
    admin.close();
  }

  @Before
  public void setUp() {
    storage.with(NAMESPACE, TABLE);
    setUp(storage);
  }

  @After
  public void tearDown() throws Exception {
    deleteData();
  }
}
