package com.scalar.db.storage.cassandra;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.MultipleClusteringKeysIntegrationTestBase;
import java.util.Collections;
import java.util.Properties;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class CassandraMultipleClusteringKeysIntegrationTest
    extends MultipleClusteringKeysIntegrationTestBase {

  private static final String CONTACT_POINT = "localhost";
  private static final String USERNAME = "cassandra";
  private static final String PASSWORD = "cassandra";

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {

    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, CONTACT_POINT);
    props.setProperty(DatabaseConfig.USERNAME, USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, PASSWORD);
    DatabaseConfig config = new DatabaseConfig(props);

    admin = new CassandraAdmin(config);
    distributedStorage = new Cassandra(config);
    createTestTables(Collections.emptyMap());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    deleteTestTables();
    admin.close();
    distributedStorage.close();
  }
}
