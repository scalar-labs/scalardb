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
  public void setUp() {
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
    testEnv.createTable(
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

    storage = new JdbcDatabase(testEnv.getJdbcConfig());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    storage.close();
    testEnv.deleteTables();
    testEnv.close();
  }
}
