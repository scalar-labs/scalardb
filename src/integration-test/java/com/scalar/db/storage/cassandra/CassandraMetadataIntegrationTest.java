package com.scalar.db.storage.cassandra;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.MetadataIntegrationTestBase;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.Properties;

public class CassandraMetadataIntegrationTest extends MetadataIntegrationTestBase {

  private static final String INDEX1 = "test_index1";
  private static final String INDEX2 = "test_index2";
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
          + " (c1 int, c2 text, c3 text, c4 int, c5 int, c6 text, c7 bigint, c8 float, c9 double,"
          + " c10 boolean, c11 blob, PRIMARY KEY((c2, c1), c4, c3))"
          + " WITH CLUSTERING ORDER BY (c4 ASC, c3 DESC)";
  private static final String CREATE_INDEX_STMT1 =
      "CREATE INDEX " + INDEX1 + " ON " + NAMESPACE + "." + TABLE + " (c5)";
  private static final String CREATE_INDEX_STMT2 =
      "CREATE INDEX " + INDEX2 + " ON " + NAMESPACE + "." + TABLE + " (c6)";
  private static final String DROP_KEYSPACE_STMT = "DROP KEYSPACE " + NAMESPACE;

  private static CassandraTableMetadata tableMetadata;

  @Before
  public void setUp() throws Exception {
    setUp(tableMetadata);
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

    builder = new ProcessBuilder("cqlsh", "-u", USERNAME, "-p", PASSWORD, "-e", CREATE_INDEX_STMT1);
    process = builder.start();
    ret = process.waitFor();
    if (ret != 0) {
      Assert.fail("CREATE INDEX failed.");
    }

    builder = new ProcessBuilder("cqlsh", "-u", USERNAME, "-p", PASSWORD, "-e", CREATE_INDEX_STMT2);
    process = builder.start();
    ret = process.waitFor();
    if (ret != 0) {
      Assert.fail("CREATE INDEX failed.");
    }

    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, CONTACT_POINT);
    props.setProperty(DatabaseConfig.USERNAME, USERNAME);
    props.setProperty(DatabaseConfig.PASSWORD, PASSWORD);
    ClusterManager clusterManager = new ClusterManager(new DatabaseConfig(props));
    clusterManager.getSession();
    tableMetadata = new CassandraTableMetadata(clusterManager.getMetadata(NAMESPACE, TABLE));
    clusterManager.close();
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
