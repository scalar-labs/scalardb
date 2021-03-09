package com.scalar.db.storage.cassandra;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.IntegrationTestBase;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.Properties;

public class CassandraIntegrationTest extends IntegrationTestBase {

  private static final String INDEX = "test_index";
  private static final String CONTACT_POINT = "localhost";
  private static final String USERNAME = "cassandra";
  private static final String PASSWORD = "cassandra";
  private static final String CREATE_KEYSPACE_STMT =
      "CREATE KEYSPACE "
          + NAMESPACE
          + " WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }";
  private static final String CREATE_TABLE_STMT =
      "CREATE TABLE "
          + NAMESPACE
          + "."
          + TABLE
          + " (c1 int, c2 text, c3 int, c4 int, c5 boolean, PRIMARY KEY((c1), c4))";
  private static final String CREATE_INDEX_STMT =
      "CREATE INDEX " + INDEX + " ON " + NAMESPACE + "." + TABLE + " (c3)";
  private static final String DROP_KEYSPACE_STMT = "DROP KEYSPACE " + NAMESPACE;
  private static final String TRUNCATE_TABLE_STMT = "TRUNCATE " + NAMESPACE + "." + TABLE;

  @Before
  public void setUp() throws Exception {
    storage.with(NAMESPACE, TABLE);
  }

  @After
  public void tearDown() throws Exception {
    ProcessBuilder builder;
    Process process;
    int ret;

    // truncate the TABLE
    builder =
        new ProcessBuilder("cqlsh", "-u", USERNAME, "-p", PASSWORD, "-e", TRUNCATE_TABLE_STMT);
    process = builder.start();
    ret = process.waitFor();
    if (ret != 0) {
      Assert.fail("TRUNCATE TABLE failed.");
    }
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    ProcessBuilder builder;
    Process process;
    int ret;

    builder =
        new ProcessBuilder("cqlsh", "-u", USERNAME, "-p", PASSWORD, "-e", CREATE_KEYSPACE_STMT);
    process = builder.start();
    ret = process.waitFor();
    if (ret != 0) {
      Assert.fail("CREATE KEYSPACE failed.");
    }

    builder = new ProcessBuilder("cqlsh", "-u", USERNAME, "-p", PASSWORD, "-e", CREATE_TABLE_STMT);
    process = builder.start();
    ret = process.waitFor();
    if (ret != 0) {
      Assert.fail("CREATE TABLE failed.");
    }

    builder = new ProcessBuilder("cqlsh", "-u", USERNAME, "-p", PASSWORD, "-e", CREATE_INDEX_STMT);
    process = builder.start();
    ret = process.waitFor();
    if (ret != 0) {
      Assert.fail("CREATE INDEX failed.");
    }

    // reuse this storage instance through the tests
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, CONTACT_POINT);
    props.setProperty(DatabaseConfig.USERNAME, USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, PASSWORD);
    storage = new Cassandra(new DatabaseConfig(props));
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    ProcessBuilder builder;
    Process process;
    int ret;

    builder = new ProcessBuilder("cqlsh", "-u", USERNAME, "-p", PASSWORD, "-e", DROP_KEYSPACE_STMT);
    process = builder.start();
    ret = process.waitFor();
    if (ret != 0) {
      Assert.fail("DROP KEYSPACE failed.");
    }
  }
}
