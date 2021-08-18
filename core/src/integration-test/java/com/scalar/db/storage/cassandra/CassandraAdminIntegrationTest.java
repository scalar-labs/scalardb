package com.scalar.db.storage.cassandra;

import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.AdminIntegrationTestBase;
import java.util.HashMap;
import java.util.Properties;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public class CassandraAdminIntegrationTest extends AdminIntegrationTestBase {
  private static final String CONTACT_POINT = "localhost";
  private static final String USERNAME = "cassandra";
  private static final String PASSWORD = "cassandra";

  private static TableMetadata tableMetadata =
      TableMetadata.newBuilder()
          .addPartitionKey("c2")
          .addPartitionKey("c1")
          .addClusteringKey("c4", Order.ASC)
          .addClusteringKey("c3", Order.DESC)
          .addColumn("c1", DataType.INT)
          .addColumn("c2", DataType.TEXT)
          .addColumn("c3", DataType.TEXT)
          .addColumn("c4", DataType.INT)
          .addColumn("c5", DataType.INT)
          .addColumn("c6", DataType.TEXT)
          .addColumn("c7", DataType.BIGINT)
          .addColumn("c8", DataType.FLOAT)
          .addColumn("c9", DataType.DOUBLE)
          .addColumn("c10", DataType.BOOLEAN)
          .addColumn("c11", DataType.BLOB)
          .addSecondaryIndex("c5")
          .addSecondaryIndex("c6")
          .build();
  private static DistributedStorageAdmin admin;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, CONTACT_POINT);
    props.setProperty(DatabaseConfig.USERNAME, USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, PASSWORD);
    DatabaseConfig config = new DatabaseConfig(props);

    admin = new CassandraAdmin(config);
    admin.createTable(NAMESPACE, TABLE, tableMetadata, new HashMap<>());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    admin.dropTable(NAMESPACE, TABLE);
    admin.close();
  }

  @Before
  public void setUp() throws Exception {
    setUp(admin);
  }
}
