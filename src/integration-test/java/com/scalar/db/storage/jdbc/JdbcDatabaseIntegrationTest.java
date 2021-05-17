package com.scalar.db.storage.jdbc;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.IntegrationTestBase;
import com.scalar.db.storage.jdbc.test.TestEnv;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public class JdbcDatabaseIntegrationTest extends IntegrationTestBase {

  private static TestEnv testEnv;
  private static DistributedStorage storage;

  @Before
  public void setUp() throws Exception {
    storage.with(NAMESPACE, TABLE);
    setUp(storage);
  }

  @After
  public void tearDown() throws Exception {
    testEnv.deleteTableData();
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
            .addColumn(COL_NAME3, DataType.INT)
            .addColumn(COL_NAME4, DataType.INT)
            .addColumn(COL_NAME5, DataType.BOOLEAN)
            .addPartitionKey(COL_NAME1)
            .addClusteringKey(COL_NAME4)
            .addSecondaryIndex(COL_NAME3)
            .build());
    testEnv.createMetadataTable();
    testEnv.createTables();
    testEnv.insertMetadata();

    storage = new JdbcDatabase(testEnv.getJdbcDatabaseConfig());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    storage.close();
    testEnv.dropMetadataTable();
    testEnv.dropTables();
    testEnv.close();
  }
}
