package com.scalar.db.transaction.consensuscommit;

import static com.scalar.db.transaction.consensuscommit.Attribute.BEFORE_COMMITTED_AT;
import static com.scalar.db.transaction.consensuscommit.Attribute.BEFORE_ID;
import static com.scalar.db.transaction.consensuscommit.Attribute.BEFORE_PREFIX;
import static com.scalar.db.transaction.consensuscommit.Attribute.BEFORE_PREPARED_AT;
import static com.scalar.db.transaction.consensuscommit.Attribute.BEFORE_STATE;
import static com.scalar.db.transaction.consensuscommit.Attribute.BEFORE_VERSION;
import static com.scalar.db.transaction.consensuscommit.Attribute.COMMITTED_AT;
import static com.scalar.db.transaction.consensuscommit.Attribute.CREATED_AT;
import static com.scalar.db.transaction.consensuscommit.Attribute.ID;
import static com.scalar.db.transaction.consensuscommit.Attribute.PREPARED_AT;
import static com.scalar.db.transaction.consensuscommit.Attribute.STATE;
import static com.scalar.db.transaction.consensuscommit.Attribute.VERSION;
import static org.mockito.Mockito.spy;

import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Scan;
import com.scalar.db.storage.common.metadata.DataType;
import com.scalar.db.storage.jdbc.JdbcDatabase;
import com.scalar.db.storage.jdbc.test.TestEnv;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public class ConsensusCommitWithJdbcDatabaseIntegrationTest
    extends ConsensusCommitIntegrationTestBase {

  private static TestEnv testEnv;
  private static DistributedStorage originalStorage;

  @Before
  public void setUp() throws SQLException {
    DistributedStorage storage = spy(originalStorage);
    Coordinator coordinator = spy(new Coordinator(storage));
    RecoveryHandler recovery = spy(new RecoveryHandler(storage, coordinator));
    CommitHandler commit = spy(new CommitHandler(storage, coordinator, recovery));
    ConsensusCommitManager manager =
        new ConsensusCommitManager(
            storage, testEnv.getJdbcDatabaseConfig(), coordinator, recovery, commit);
    setUp(manager, storage, coordinator, recovery);
  }

  @After
  public void tearDown() throws Exception {
    testEnv.deleteTableData();
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    testEnv = new TestEnv();

    // For the coordinator table
    testEnv.register(
        Coordinator.NAMESPACE,
        Coordinator.TABLE,
        Collections.singletonList(ID),
        Collections.emptyList(),
        new HashMap<String, Scan.Ordering.Order>() {},
        new HashMap<String, DataType>() {
          {
            put(ID, DataType.TEXT);
            put(STATE, DataType.INT);
            put(CREATED_AT, DataType.BIGINT);
          }
        });

    // For the test tables
    for (String table : Arrays.asList(TABLE_1, TABLE_2)) {
      testEnv.register(
          NAMESPACE,
          table,
          Collections.singletonList(ACCOUNT_ID),
          Collections.singletonList(ACCOUNT_TYPE),
          new HashMap<String, Scan.Ordering.Order>() {
            {
              put(ACCOUNT_TYPE, Scan.Ordering.Order.ASC);
            }
          },
          new HashMap<String, DataType>() {
            {
              put(ACCOUNT_ID, DataType.INT);
              put(ACCOUNT_TYPE, DataType.INT);
              put(BALANCE, DataType.INT);
              put(ID, DataType.TEXT);
              put(STATE, DataType.INT);
              put(VERSION, DataType.INT);
              put(PREPARED_AT, DataType.BIGINT);
              put(COMMITTED_AT, DataType.BIGINT);
              put(BEFORE_PREFIX + BALANCE, DataType.INT);
              put(BEFORE_ID, DataType.TEXT);
              put(BEFORE_STATE, DataType.INT);
              put(BEFORE_VERSION, DataType.INT);
              put(BEFORE_PREPARED_AT, DataType.BIGINT);
              put(BEFORE_COMMITTED_AT, DataType.BIGINT);
            }
          });
    }

    testEnv.createMetadataTable();
    testEnv.createTables();
    testEnv.insertMetadata();

    originalStorage = new JdbcDatabase(testEnv.getJdbcDatabaseConfig());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    testEnv.dropMetadataTable();
    testEnv.dropTables();
    testEnv.close();
  }
}
