package com.scalar.db.storage.cassandra;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.io.DataType;
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
  private static final TableMetadata TABLE_METADATA =
      TableMetadata.newBuilder()
          .addPartitionKey("c1")
          .addClusteringKey("c4")
          .addColumn("c1", DataType.INT)
          .addColumn("c2", DataType.TEXT)
          .addColumn("c3", DataType.INT)
          .addColumn("c4", DataType.INT)
          .addColumn("c5", DataType.BOOLEAN)
          .addSecondaryIndex("c3")
          .build();
  private static DistributedStorage storage;
  private static CassandraAdmin cassandraAdmin;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // reuse this storage instance through the tests
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, CONTACT_POINT);
    props.setProperty(DatabaseConfig.USERNAME, USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, PASSWORD);
    DatabaseConfig config = new DatabaseConfig(props);

    cassandraAdmin = new CassandraAdmin(config);
    createTable(Collections.emptyMap());
    storage = new Cassandra(config);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    deleteTable();
    cassandraAdmin.close();
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
