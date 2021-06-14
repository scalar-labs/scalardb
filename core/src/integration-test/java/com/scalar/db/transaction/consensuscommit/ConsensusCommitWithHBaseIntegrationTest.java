package com.scalar.db.transaction.consensuscommit;

import static com.scalar.db.storage.hbase.HBaseIntegrationTest.addEndpoint;
import static com.scalar.db.storage.hbase.HBaseIntegrationTest.createMetadataTable;
import static com.scalar.db.storage.hbase.HBaseIntegrationTest.deleteTableData;
import static com.scalar.db.storage.hbase.HBaseIntegrationTest.dropMetadataTable;
import static com.scalar.db.storage.hbase.HBaseIntegrationTest.dropSchema;
import static com.scalar.db.storage.hbase.HBaseIntegrationTest.dropTable;
import static com.scalar.db.storage.hbase.HBaseIntegrationTest.upsertMetadata;
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
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.hbase.HBase;
import com.scalar.db.storage.hbase.HBaseConnection;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Optional;
import java.util.Properties;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public class ConsensusCommitWithHBaseIntegrationTest extends ConsensusCommitIntegrationTestBase {

  private static DatabaseConfig config;
  private static HBase hbase;
  private static HBaseConnection hbaseConnection;

  @Before
  public void setUp() throws SQLException {
    DistributedStorage storage = spy(hbase);
    Coordinator coordinator = spy(new Coordinator(storage));
    RecoveryHandler recovery = spy(new RecoveryHandler(storage, coordinator));
    CommitHandler commit = spy(new CommitHandler(storage, coordinator, recovery));
    ConsensusCommitManager manager =
        new ConsensusCommitManager(storage, config, coordinator, recovery, commit);
    setUp(manager, storage, coordinator, recovery);
  }

  @After
  public void tearDown() throws Exception {
    try (PhoenixConnection connection = hbaseConnection.getConnection();
        Statement statement = connection.createStatement()) {
      deleteTableData(statement, Coordinator.NAMESPACE, Coordinator.TABLE);
      for (String table : Arrays.asList(TABLE_1, TABLE_2)) {
        deleteTableData(statement, NAMESPACE, table);
      }
    }
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    String jdbcUrl =
        System.getProperty("scalardb.hbase.jdbc_url", "jdbc:phoenix:localhost:2181:/hbase");
    Optional<String> namespacePrefix =
        Optional.ofNullable(System.getProperty("scalardb.namespace_prefix"));

    hbaseConnection = new HBaseConnection(jdbcUrl);

    try (PhoenixConnection connection = hbaseConnection.getConnection();
        Statement statement = connection.createStatement()) {
      createMetadataTable(statement);
      createCoordinatorTable(connection, statement);
      createTestSchema(statement);
      for (String table : Arrays.asList(TABLE_1, TABLE_2)) {
        createTestTable(connection, statement, table);
      }
    }

    Properties props = new Properties();
    props.setProperty(DatabaseConfig.STORAGE, "hbase");
    props.setProperty(DatabaseConfig.CONTACT_POINTS, jdbcUrl);
    namespacePrefix.ifPresent(n -> props.setProperty(DatabaseConfig.NAMESPACE_PREFIX, n));
    config = new DatabaseConfig(props);
    hbase = new HBase(config);
  }

  private static void createCoordinatorTable(PhoenixConnection connection, Statement statement)
      throws SQLException {
    statement.execute("CREATE SCHEMA \"" + Coordinator.NAMESPACE + "\"");

    statement.execute(
        "CREATE TABLE \""
            + Coordinator.NAMESPACE
            + "\".\""
            + Coordinator.TABLE
            + "\" (\"hash\" INTEGER NOT NULL,\"tx_id\" VARCHAR NOT NULL,"
            + "\"tx_state\" INTEGER,\"tx_created_at\" BIGINT,"
            + "CONSTRAINT pk PRIMARY KEY (\"hash\", \"tx_id\"))");

    int ordinalPosition = 1;
    upsertMetadata(
        statement,
        Coordinator.NAMESPACE,
        Coordinator.TABLE,
        ID,
        DataType.TEXT,
        "PARTITION",
        null,
        false,
        ordinalPosition++);
    upsertMetadata(
        statement,
        Coordinator.NAMESPACE,
        Coordinator.TABLE,
        STATE,
        DataType.INT,
        null,
        null,
        false,
        ordinalPosition++);
    upsertMetadata(
        statement,
        Coordinator.NAMESPACE,
        Coordinator.TABLE,
        CREATED_AT,
        DataType.BIGINT,
        null,
        null,
        false,
        ordinalPosition);

    addEndpoint(connection, Coordinator.NAMESPACE, Coordinator.TABLE);
  }

  private static void createTestSchema(Statement statement) throws SQLException {
    statement.execute("CREATE SCHEMA \"" + NAMESPACE + "\"");
  }

  private static void createTestTable(
      PhoenixConnection connection, Statement statement, String table) throws SQLException {

    statement.execute(
        "CREATE TABLE \""
            + NAMESPACE
            + "\".\""
            + table
            + "\" (\"hash\" INTEGER NOT NULL,\"account_id\" INTEGER NOT NULL,"
            + "\"account_type\" INTEGER NOT NULL,\"balance\" INTEGER,"
            + "\"tx_id\" VARCHAR,\"tx_state\" INTEGER,"
            + "\"tx_version\" INTEGER,\"tx_prepared_at\" BIGINT,"
            + "\"tx_committed_at\" BIGINT,\"before_balance\" INTEGER,"
            + "\"before_tx_id\" VARCHAR,\"before_tx_state\" INTEGER,"
            + "\"before_tx_version\" INTEGER,\"before_tx_prepared_at\" BIGINT,"
            + "\"before_tx_committed_at\" BIGINT,"
            + "CONSTRAINT pk PRIMARY KEY (\"hash\", \"account_id\", \"account_type\"))");

    int ordinalPosition = 1;
    upsertMetadata(
        statement,
        NAMESPACE,
        table,
        ACCOUNT_ID,
        DataType.INT,
        "PARTITION",
        null,
        false,
        ordinalPosition++);
    upsertMetadata(
        statement,
        NAMESPACE,
        table,
        ACCOUNT_TYPE,
        DataType.INT,
        "CLUSTERING",
        "ASC",
        false,
        ordinalPosition++);
    upsertMetadata(
        statement, NAMESPACE, table, BALANCE, DataType.INT, null, null, false, ordinalPosition++);
    upsertMetadata(
        statement, NAMESPACE, table, ID, DataType.TEXT, null, null, false, ordinalPosition++);
    upsertMetadata(
        statement, NAMESPACE, table, STATE, DataType.INT, null, null, false, ordinalPosition++);
    upsertMetadata(
        statement, NAMESPACE, table, VERSION, DataType.INT, null, null, false, ordinalPosition++);
    upsertMetadata(
        statement,
        NAMESPACE,
        table,
        PREPARED_AT,
        DataType.BIGINT,
        null,
        null,
        false,
        ordinalPosition++);
    upsertMetadata(
        statement,
        NAMESPACE,
        table,
        COMMITTED_AT,
        DataType.BIGINT,
        null,
        null,
        false,
        ordinalPosition++);
    upsertMetadata(
        statement,
        NAMESPACE,
        table,
        BEFORE_PREFIX + BALANCE,
        DataType.INT,
        null,
        null,
        false,
        ordinalPosition++);
    upsertMetadata(
        statement,
        NAMESPACE,
        table,
        BEFORE_ID,
        DataType.TEXT,
        null,
        null,
        false,
        ordinalPosition++);
    upsertMetadata(
        statement,
        NAMESPACE,
        table,
        BEFORE_STATE,
        DataType.INT,
        null,
        null,
        false,
        ordinalPosition++);
    upsertMetadata(
        statement,
        NAMESPACE,
        table,
        BEFORE_VERSION,
        DataType.INT,
        null,
        null,
        false,
        ordinalPosition++);
    upsertMetadata(
        statement,
        NAMESPACE,
        table,
        BEFORE_PREPARED_AT,
        DataType.BIGINT,
        null,
        null,
        false,
        ordinalPosition++);
    upsertMetadata(
        statement,
        NAMESPACE,
        table,
        BEFORE_COMMITTED_AT,
        DataType.BIGINT,
        null,
        null,
        false,
        ordinalPosition);

    addEndpoint(connection, NAMESPACE, table);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    hbase.close();

    try (Connection connection = hbaseConnection.getConnection();
        Statement statement = connection.createStatement()) {
      for (String table : Arrays.asList(TABLE_1, TABLE_2)) {
        dropTable(statement, NAMESPACE, table);
      }
      dropSchema(statement, NAMESPACE);
      dropTable(statement, Coordinator.NAMESPACE, Coordinator.TABLE);
      dropSchema(statement, Coordinator.NAMESPACE);
      dropMetadataTable(statement);
    }
  }
}
