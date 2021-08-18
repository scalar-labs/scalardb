package com.scalar.db.transaction.consensuscommit;

import static com.scalar.db.transaction.consensuscommit.Attribute.BEFORE_COMMITTED_AT;
import static com.scalar.db.transaction.consensuscommit.Attribute.BEFORE_ID;
import static com.scalar.db.transaction.consensuscommit.Attribute.BEFORE_PREFIX;
import static com.scalar.db.transaction.consensuscommit.Attribute.BEFORE_PREPARED_AT;
import static com.scalar.db.transaction.consensuscommit.Attribute.BEFORE_STATE;
import static com.scalar.db.transaction.consensuscommit.Attribute.BEFORE_VERSION;
import static com.scalar.db.transaction.consensuscommit.Attribute.COMMITTED_AT;
import static com.scalar.db.transaction.consensuscommit.Attribute.ID;
import static com.scalar.db.transaction.consensuscommit.Attribute.PREPARED_AT;
import static com.scalar.db.transaction.consensuscommit.Attribute.STATE;
import static com.scalar.db.transaction.consensuscommit.Attribute.VERSION;
import static org.mockito.Mockito.spy;

import com.google.common.base.Joiner;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.jdbc.test.TestEnv;
import com.scalar.db.storage.multistorage.MultiStorage;
import com.scalar.db.storage.multistorage.MultiStorageConfig;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Optional;
import java.util.Properties;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
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
  private static DistributedStorage originalStorage;

  @Before
  public void setUp() throws SQLException {
    DistributedStorage storage = spy(originalStorage);
    Coordinator coordinator = spy(new Coordinator(storage));
    RecoveryHandler recovery = spy(new RecoveryHandler(storage, coordinator));
    CommitHandler commit = spy(new CommitHandler(storage, coordinator, recovery));

    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, "dummy");
    props.setProperty(DatabaseConfig.USERNAME, "dummy");
    props.setProperty(DatabaseConfig.PASSWORD, "dummy");

    ConsensusCommitManager manager =
        new ConsensusCommitManager(
            storage, new DatabaseConfig(props), coordinator, recovery, commit);
    setUp(manager, storage, coordinator, recovery);
  }

  @After
  public void tearDown() throws Exception {
    truncateCassandra();
    truncateMySql();
  }

  private void truncateCassandra() throws Exception {
    executeStatement(truncateTableStatement(NAMESPACE, TABLE_1));
    executeStatement(truncateTableStatement(Coordinator.NAMESPACE, Coordinator.TABLE));
  }

  private void truncateMySql() throws Exception {
    testEnv.deleteTableData();
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    initCassandra();
    initMySql();
    initMultiStorage();
  }

  private static void initCassandra() throws Exception {
    executeStatement(createNamespaceStatement(Coordinator.NAMESPACE));
    executeStatement(createCoordinatorTableStatement(Coordinator.NAMESPACE, Coordinator.TABLE));
    executeStatement(createNamespaceStatement(NAMESPACE));
    executeStatement(createTableStatement(NAMESPACE, TABLE_1));
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
    executeStatement(dropNamespaceStatement(NAMESPACE));
    executeStatement(dropNamespaceStatement(Coordinator.NAMESPACE));
  }

  private static void cleanUpMySql() throws Exception {
    testEnv.deleteTables();
    testEnv.close();
  }

  private static String createNamespaceStatement(String namespace) {
    return Joiner.on(" ")
        .skipNulls()
        .join(
            new String[] {
              "CREATE KEYSPACE",
              namespace,
              "WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }",
            });
  }

  private static String dropNamespaceStatement(String namespace) {
    return Joiner.on(" ")
        .skipNulls()
        .join(
            new String[] {
              "DROP KEYSPACE", namespace,
            });
  }

  private static String createTableStatement(String namespace, String table) {
    return Joiner.on(" ")
        .skipNulls()
        .join(
            new String[] {
              "CREATE TABLE",
              namespace + "." + table,
              "(",
              ACCOUNT_ID,
              "int,",
              ACCOUNT_TYPE,
              "int,",
              BALANCE,
              "int,",
              Attribute.ID,
              "text,",
              Attribute.VERSION,
              "int,",
              Attribute.STATE,
              "int,",
              Attribute.PREPARED_AT,
              "bigint,",
              Attribute.COMMITTED_AT,
              "bigint,",
              Attribute.BEFORE_PREFIX + BALANCE,
              "int,",
              Attribute.BEFORE_ID,
              "text,",
              Attribute.BEFORE_VERSION,
              "int,",
              Attribute.BEFORE_STATE,
              "int,",
              Attribute.BEFORE_PREPARED_AT,
              "bigint,",
              Attribute.BEFORE_COMMITTED_AT,
              "bigint,",
              "PRIMARY KEY",
              "((" + ACCOUNT_ID + "),",
              ACCOUNT_TYPE + ")",
              ")",
            });
  }

  private static String createCoordinatorTableStatement(String namespace, String table) {
    return Joiner.on(" ")
        .skipNulls()
        .join(
            new String[] {
              "CREATE TABLE",
              namespace + "." + table,
              "(",
              Attribute.ID,
              "text,",
              Attribute.STATE,
              "int,",
              Attribute.CREATED_AT,
              "bigint,",
              "PRIMARY KEY",
              "(" + Attribute.ID + ")",
              ")",
            });
  }

  private static String truncateTableStatement(String namespace, String table) {
    return Joiner.on(" ")
        .skipNulls()
        .join(
            new String[] {
              "TRUNCATE", namespace + "." + table,
            });
  }

  private static void executeStatement(String statement) throws IOException {
    ProcessBuilder builder =
        new ProcessBuilder(
            "cqlsh", "-u", CASSANDRA_USERNAME, "-p", CASSANDRA_PASSWORD, "-e", statement);
    builder.redirectErrorStream(true);

    BufferedReader reader = null;
    try {
      Process process = builder.start();
      reader =
          new BufferedReader(
              new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
      while (true) {
        String line = reader.readLine();
        if (line == null) break;
        System.out.println(line);
      }

      int ret = process.waitFor();
      if (ret != 0) {
        Assert.fail("failed to execute " + statement);
      }
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    } finally {
      if (reader != null) {
        reader.close();
      }
    }
  }
}
