package com.scalar.db.storage.jdbc;

import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.AdminIntegrationTestBase;
import com.scalar.db.storage.jdbc.test.TestEnv;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public class JdbcDatabaseAdminIntegrationTest extends AdminIntegrationTestBase {

  private static TestEnv testEnv;
  private static DistributedStorageAdmin admin;

  @Before
  public void setUp() throws Exception {
    setUp(admin);
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    testEnv = new TestEnv();
    testEnv.register(
        NAMESPACE,
        TABLE,
        TableMetadata.newBuilder()
            .addColumn(COL_NAME1, DataType.INT)
            .addColumn(COL_NAME2, DataType.TEXT)
            .addColumn(COL_NAME3, DataType.TEXT)
            .addColumn(COL_NAME4, DataType.INT)
            .addColumn(COL_NAME5, DataType.INT)
            .addColumn(COL_NAME6, DataType.TEXT)
            .addColumn(COL_NAME7, DataType.BIGINT)
            .addColumn(COL_NAME8, DataType.FLOAT)
            .addColumn(COL_NAME9, DataType.DOUBLE)
            .addColumn(COL_NAME10, DataType.BOOLEAN)
            .addColumn(COL_NAME11, DataType.BLOB)
            .addPartitionKey(COL_NAME2)
            .addPartitionKey(COL_NAME1)
            .addClusteringKey(COL_NAME4, Scan.Ordering.Order.ASC)
            .addClusteringKey(COL_NAME3, Scan.Ordering.Order.DESC)
            .addSecondaryIndex(COL_NAME5)
            .addSecondaryIndex(COL_NAME6)
            .build());
    testEnv.createMetadataTable();
    testEnv.insertMetadata();

    admin = new JdbcDatabaseAdmin(testEnv.getJdbcDatabaseConfig());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    testEnv.dropMetadataTable();
    testEnv.close();
    admin.close();
  }
}
