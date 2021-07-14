package com.scalar.db.transaction.consensuscommit;

import static org.mockito.Mockito.spy;

import com.google.common.base.Joiner;
import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.cassandra.Cassandra;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;

public class ConsensusCommitWithCassandraIntegrationTest
    extends ConsensusCommitIntegrationTestBase {

  private static final String USERNAME = "cassandra";
  private static final String PASSWORD = "cassandra";
  private static final String CONTACT_POINT = "localhost";

  private static DistributedStorage originalStorage;
  private static DatabaseConfig config;

  @Before
  public void setUp() {
    DistributedStorage storage = spy(originalStorage);
    Coordinator coordinator = spy(new Coordinator(storage));
    RecoveryHandler recovery = spy(new RecoveryHandler(storage, coordinator));
    CommitHandler commit = spy(new CommitHandler(storage, coordinator, recovery));
    ConsensusCommitManager manager =
        new ConsensusCommitManager(storage, config, coordinator, recovery, commit);
    setUp(manager, storage, coordinator, recovery);
  }

  @After
  public void tearDown() throws IOException {
    executeStatement(
        truncateTableStatement(
            ConsensusCommitIntegrationTestBase.NAMESPACE,
            ConsensusCommitIntegrationTestBase.TABLE_1));
    executeStatement(
        truncateTableStatement(
            ConsensusCommitIntegrationTestBase.NAMESPACE,
            ConsensusCommitIntegrationTestBase.TABLE_2));
    executeStatement(truncateTableStatement(Coordinator.NAMESPACE, Coordinator.TABLE));
  }

  @BeforeClass
  public static void setUpBeforeClass() throws IOException {
    executeStatement(createNamespaceStatement(ConsensusCommitIntegrationTestBase.NAMESPACE));
    System.out.println(createNamespaceStatement(ConsensusCommitIntegrationTestBase.NAMESPACE));
    executeStatement(
        createTableStatement(
            ConsensusCommitIntegrationTestBase.NAMESPACE,
            ConsensusCommitIntegrationTestBase.TABLE_1));
    System.out.println(
        createTableStatement(
            ConsensusCommitIntegrationTestBase.NAMESPACE,
            ConsensusCommitIntegrationTestBase.TABLE_1));
    executeStatement(
        createTableStatement(
            ConsensusCommitIntegrationTestBase.NAMESPACE,
            ConsensusCommitIntegrationTestBase.TABLE_2));
    executeStatement(createNamespaceStatement(Coordinator.NAMESPACE));
    System.out.println(createNamespaceStatement(Coordinator.NAMESPACE));
    executeStatement(createCoordinatorTableStatement(Coordinator.NAMESPACE, Coordinator.TABLE));
    System.out.println(createCoordinatorTableStatement(Coordinator.NAMESPACE, Coordinator.TABLE));

    // it's supposed to be unnecessary, but just in case schema agreement takes some time
    Uninterruptibles.sleepUninterruptibly(1000, TimeUnit.MILLISECONDS);

    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, CONTACT_POINT);
    props.setProperty(DatabaseConfig.USERNAME, USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, PASSWORD);
    config = new DatabaseConfig(props);
    originalStorage = new Cassandra(config);
  }

  @AfterClass
  public static void tearDownAfterClass() throws IOException {
    originalStorage.close();

    executeStatement(dropNamespaceStatement(ConsensusCommitIntegrationTestBase.NAMESPACE));
    executeStatement(dropNamespaceStatement(Coordinator.NAMESPACE));
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
              ConsensusCommitIntegrationTestBase.ACCOUNT_ID,
              "int,",
              ConsensusCommitIntegrationTestBase.ACCOUNT_TYPE,
              "int,",
              ConsensusCommitIntegrationTestBase.BALANCE,
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
              Attribute.BEFORE_PREFIX + ConsensusCommitIntegrationTestBase.BALANCE,
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
              "((" + ConsensusCommitIntegrationTestBase.ACCOUNT_ID + "),",
              ConsensusCommitIntegrationTestBase.ACCOUNT_TYPE + ")",
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
        new ProcessBuilder("cqlsh", "-u", USERNAME, "-p", PASSWORD, "-e", statement);
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
