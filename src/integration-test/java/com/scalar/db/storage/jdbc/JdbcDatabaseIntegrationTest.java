package com.scalar.db.storage.jdbc;

import com.scalar.db.api.Scan;
import com.scalar.db.storage.IntegrationTestBase;
import com.scalar.db.storage.common.metadata.DataType;
import com.scalar.db.storage.jdbc.test.TestEnv;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.Collections;
import java.util.HashMap;

public class JdbcDatabaseIntegrationTest extends IntegrationTestBase {

  private static TestEnv testEnv;

  @Before
  public void setUp() throws Exception {
    storage.with(NAMESPACE, TABLE);
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
        Collections.singletonList(COL_NAME1),
        Collections.singletonList(COL_NAME4),
        new HashMap<String, Scan.Ordering.Order>() {
          {
            put(COL_NAME4, Scan.Ordering.Order.ASC);
          }
        },
        new HashMap<String, DataType>() {
          {
            put(COL_NAME1, DataType.INT);
            put(COL_NAME2, DataType.TEXT);
            put(COL_NAME3, DataType.INT);
            put(COL_NAME4, DataType.INT);
            put(COL_NAME5, DataType.BOOLEAN);
          }
        },
        Collections.singletonList(COL_NAME3),
        new HashMap<String, Scan.Ordering.Order>() {
          {
            put(COL_NAME3, Scan.Ordering.Order.ASC);
          }
        });
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
