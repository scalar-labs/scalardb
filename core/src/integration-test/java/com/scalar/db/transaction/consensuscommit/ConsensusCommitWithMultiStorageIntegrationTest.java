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
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.cassandra.CassandraAdmin;
import com.scalar.db.storage.jdbc.test.TestEnv;
import com.scalar.db.storage.multistorage.MultiStorage;
import com.scalar.db.storage.multistorage.MultiStorageConfig;
import java.util.Optional;
import java.util.Properties;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public class ConsensusCommitWithMultiStorageIntegrationTest
    extends ConsensusCommitIntegrationTestBase {

  private static final String CASSANDRA_CONTACT_POINT = "localhost";
  private static final String CASSANDRA_USERNAME = "cassandra";
  private static final String CASSANDRA_PASSWORD = "cassandra";

  private static final String MYSQL_CONTACT_POINT = "jdbc:mysql://localhost:3306/";
  private static final String MYSQL_USERNAME = "root";
  private static final String MYSQL_PASSWORD = "mysql";

  private static TestEnv testEnv;
  private static CassandraAdmin cassandraAdmin;
  private static DistributedStorage originalStorage;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    initCassandra();
    initMySql();
    initMultiStorage();
  }

  private static void initCassandra() throws Exception {
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, CASSANDRA_CONTACT_POINT);
    props.setProperty(DatabaseConfig.USERNAME, CASSANDRA_USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, CASSANDRA_PASSWORD);
    cassandraAdmin = new CassandraAdmin(new DatabaseConfig(props));

    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn(ACCOUNT_ID, DataType.INT)
            .addColumn(ACCOUNT_TYPE, DataType.INT)
            .addColumn(BALANCE, DataType.INT)
            .addColumn(ID, DataType.TEXT)
            .addColumn(STATE, DataType.INT)
            .addColumn(VERSION, DataType.INT)
            .addColumn(PREPARED_AT, DataType.BIGINT)
            .addColumn(COMMITTED_AT, DataType.BIGINT)
            .addColumn(BEFORE_PREFIX + BALANCE, DataType.INT)
            .addColumn(BEFORE_ID, DataType.TEXT)
            .addColumn(BEFORE_STATE, DataType.INT)
            .addColumn(BEFORE_VERSION, DataType.INT)
            .addColumn(BEFORE_PREPARED_AT, DataType.BIGINT)
            .addColumn(BEFORE_COMMITTED_AT, DataType.BIGINT)
            .addPartitionKey(ACCOUNT_ID)
            .addClusteringKey(ACCOUNT_TYPE)
            .build();
    cassandraAdmin.createNamespace(NAMESPACE);
    cassandraAdmin.createTable(NAMESPACE, TABLE_1, tableMetadata);

    TableMetadata coordinatorTableMetadata =
        TableMetadata.newBuilder()
            .addColumn(ID, DataType.TEXT)
            .addColumn(STATE, DataType.INT)
            .addColumn(CREATED_AT, DataType.BIGINT)
            .addPartitionKey(ID)
            .build();
    cassandraAdmin.createNamespace(Coordinator.NAMESPACE);
    cassandraAdmin.createTable(Coordinator.NAMESPACE, Coordinator.TABLE, coordinatorTableMetadata);
  }

  private static void initMySql() throws Exception {
    testEnv = new TestEnv(MYSQL_CONTACT_POINT, MYSQL_USERNAME, MYSQL_PASSWORD, Optional.empty());
    testEnv.createTable(
        NAMESPACE,
        TABLE_2,
        TableMetadata.newBuilder()
            .addColumn(ACCOUNT_ID, DataType.INT)
            .addColumn(ACCOUNT_TYPE, DataType.INT)
            .addColumn(BALANCE, DataType.INT)
            .addColumn(ID, DataType.TEXT)
            .addColumn(STATE, DataType.INT)
            .addColumn(VERSION, DataType.INT)
            .addColumn(PREPARED_AT, DataType.BIGINT)
            .addColumn(COMMITTED_AT, DataType.BIGINT)
            .addColumn(BEFORE_PREFIX + BALANCE, DataType.INT)
            .addColumn(BEFORE_ID, DataType.TEXT)
            .addColumn(BEFORE_STATE, DataType.INT)
            .addColumn(BEFORE_VERSION, DataType.INT)
            .addColumn(BEFORE_PREPARED_AT, DataType.BIGINT)
            .addColumn(BEFORE_COMMITTED_AT, DataType.BIGINT)
            .addPartitionKey(ACCOUNT_ID)
            .addClusteringKey(ACCOUNT_TYPE)
            .build());
  }

  private static void initMultiStorage() {
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.STORAGE, "multi-storage");

    // Define storages, cassandra and mysql
    props.setProperty(MultiStorageConfig.STORAGES, "cassandra,mysql");
    props.setProperty(MultiStorageConfig.STORAGES + ".cassandra.storage", "cassandra");
    props.setProperty(
        MultiStorageConfig.STORAGES + ".cassandra.contact_points", CASSANDRA_CONTACT_POINT);
    props.setProperty(MultiStorageConfig.STORAGES + ".cassandra.username", CASSANDRA_USERNAME);
    props.setProperty(MultiStorageConfig.STORAGES + ".cassandra.password", CASSANDRA_PASSWORD);
    props.setProperty(MultiStorageConfig.STORAGES + ".mysql.storage", "jdbc");
    props.setProperty(MultiStorageConfig.STORAGES + ".mysql.contact_points", MYSQL_CONTACT_POINT);
    props.setProperty(MultiStorageConfig.STORAGES + ".mysql.username", MYSQL_USERNAME);
    props.setProperty(MultiStorageConfig.STORAGES + ".mysql.password", MYSQL_PASSWORD);

    // Define table mapping from table1 to cassandra, from table2 to mysql, and from the
    // coordinator table to cassandra
    props.setProperty(
        MultiStorageConfig.TABLE_MAPPING,
        NAMESPACE
            + "."
            + TABLE_1
            + ":cassandra,"
            + NAMESPACE
            + "."
            + TABLE_2
            + ":mysql,"
            + Coordinator.NAMESPACE
            + "."
            + Coordinator.TABLE
            + ":cassandra");

    // The default storage is cassandra
    props.setProperty(MultiStorageConfig.DEFAULT_STORAGE, "cassandra");

    originalStorage = new MultiStorage(new MultiStorageConfig(props));
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    originalStorage.close();
    cleanUpCassandra();
    cleanUpMySql();
  }

  private static void cleanUpCassandra() throws Exception {
    cassandraAdmin.dropTable(NAMESPACE, TABLE_1);
    cassandraAdmin.dropTable(Coordinator.NAMESPACE, Coordinator.TABLE);
    cassandraAdmin.dropNamespace(NAMESPACE);
    cassandraAdmin.dropNamespace(Coordinator.NAMESPACE);
    cassandraAdmin.close();
  }

  private static void cleanUpMySql() throws Exception {
    testEnv.deleteTables();
    testEnv.close();
  }

  @Before
  public void setUp() {
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, "dummy");
    props.setProperty(DatabaseConfig.USERNAME, "dummy");
    props.setProperty(DatabaseConfig.PASSWORD, "dummy");

    ConsensusCommitConfig consensusCommitConfig = new ConsensusCommitConfig(props);

    DistributedStorage storage = spy(originalStorage);
    Coordinator coordinator = spy(new Coordinator(storage, consensusCommitConfig));
    RecoveryHandler recovery = spy(new RecoveryHandler(storage, coordinator));
    CommitHandler commit = spy(new CommitHandler(storage, coordinator, recovery));

    ConsensusCommitManager manager =
        new ConsensusCommitManager(storage, consensusCommitConfig, coordinator, recovery, commit);
    setUp(manager, storage, coordinator, recovery);
  }

  @After
  public void tearDown() throws Exception {
    truncateCassandra();
    truncateMySql();
  }

  private void truncateCassandra() throws Exception {
    cassandraAdmin.truncateTable(NAMESPACE, TABLE_1);
    cassandraAdmin.truncateTable(Coordinator.NAMESPACE, Coordinator.TABLE);
  }

  private void truncateMySql() throws Exception {
    testEnv.deleteTableData();
  }
}
