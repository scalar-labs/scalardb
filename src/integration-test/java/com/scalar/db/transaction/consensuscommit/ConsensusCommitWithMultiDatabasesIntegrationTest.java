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
import com.scalar.db.api.Scan;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.common.metadata.DataType;
import com.scalar.db.storage.jdbc.test.TestEnv;
import com.scalar.db.storage.multi.MultiDatabases;
import com.scalar.db.storage.multi.MultiDatabasesConfig;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Optional;
import java.util.Properties;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;

public class ConsensusCommitWithMultiDatabasesIntegrationTest
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
    initMultiDatabases();
  }

  private static void initCassandra() throws Exception {
    executeStatement(createNamespaceStatement(Coordinator.NAMESPACE));
    executeStatement(createCoordinatorTableStatement(Coordinator.NAMESPACE, Coordinator.TABLE));
    executeStatement(createNamespaceStatement(NAMESPACE));
    executeStatement(createTableStatement(NAMESPACE, TABLE_1));
  }

  private static void initMySql() throws Exception {
    testEnv = new TestEnv(MYSQL_CONTACT_POINT, MYSQL_USERNAME, MYSQL_PASSWORD, Optional.empty());
    testEnv.register(
        NAMESPACE,
        TABLE_2,
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

    testEnv.createMetadataTable();
    testEnv.createTables();
    testEnv.insertMetadata();
  }

  private static void initMultiDatabases() {
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.STORAGE, "multi");

    // Define databases, cassandra and mysql
    props.setProperty(MultiDatabasesConfig.DATABASES, "cassandra,mysql");
    props.setProperty(MultiDatabasesConfig.DATABASES + ".cassandra.storage", "cassandra");
    props.setProperty(
        MultiDatabasesConfig.DATABASES + ".cassandra.contact_points", CASSANDRA_CONTACT_POINT);
    props.setProperty(MultiDatabasesConfig.DATABASES + ".cassandra.username", CASSANDRA_USERNAME);
    props.setProperty(MultiDatabasesConfig.DATABASES + ".cassandra.password", CASSANDRA_PASSWORD);
    props.setProperty(MultiDatabasesConfig.DATABASES + ".mysql.storage", "jdbc");
    props.setProperty(
        MultiDatabasesConfig.DATABASES + ".mysql.contact_points", MYSQL_CONTACT_POINT);
    props.setProperty(MultiDatabasesConfig.DATABASES + ".mysql.username", MYSQL_USERNAME);
    props.setProperty(MultiDatabasesConfig.DATABASES + ".mysql.password", MYSQL_PASSWORD);

    // Define table mapping from table1 to cassandra, from table2 to mysql, and from the
    // coordinator table to cassandra
    props.setProperty(
        MultiDatabasesConfig.TABLE_MAPPING,
        NAMESPACE
            + "."
            + TABLE_1
            + ","
            + NAMESPACE
            + "."
            + TABLE_2
            + ","
            + Coordinator.NAMESPACE
            + "."
            + Coordinator.TABLE);
    props.setProperty(
        MultiDatabasesConfig.TABLE_MAPPING + "." + NAMESPACE + "." + TABLE_1, "cassandra");
    props.setProperty(
        MultiDatabasesConfig.TABLE_MAPPING + "." + NAMESPACE + "." + TABLE_2, "mysql");
    props.setProperty(
        MultiDatabasesConfig.TABLE_MAPPING + "." + Coordinator.NAMESPACE + "." + Coordinator.TABLE,
        "cassandra");

    // The default database is cassandra
    props.setProperty(MultiDatabasesConfig.DEFAULT_DATABASE, "cassandra");

    originalStorage = new MultiDatabases(new MultiDatabasesConfig(props));
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
    testEnv.dropMetadataTable();
    testEnv.dropTables();
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
      reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
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
