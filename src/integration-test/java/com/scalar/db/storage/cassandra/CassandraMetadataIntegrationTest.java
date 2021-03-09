package com.scalar.db.storage.cassandra;

import com.scalar.db.api.Scan;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.common.metadata.DataType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class CassandraMetadataIntegrationTest {

  private static final String KEYSPACE = "integration_testing";
  private static final String TABLE = "test_table";
  private static final String INDEX1 = "test_index1";
  private static final String INDEX2 = "test_index2";
  private static final String CONTACT_POINT = "localhost";
  private static final String USERNAME = "cassandra";
  private static final String PASSWORD = "cassandra";
  private static final String CREATE_KEYSPACE_STMT =
      "CREATE KEYSPACE "
          + KEYSPACE
          + " WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }";
  private static final String CREATE_TABLE_STMT =
      "CREATE TABLE "
          + KEYSPACE
          + "."
          + TABLE
          + " (c1 int, c2 text, c3 text, c4 int, c5 int, c6 text, c7 bigint, c8 float, c9 double,"
          + " c10 boolean, c11 blob, PRIMARY KEY((c2, c1), c4, c3))"
          + " WITH CLUSTERING ORDER BY (c4 ASC, c3 DESC)";
  private static final String CREATE_INDEX_STMT1 =
      "CREATE INDEX " + INDEX1 + " ON " + KEYSPACE + "." + TABLE + " (c5)";
  private static final String CREATE_INDEX_STMT2 =
      "CREATE INDEX " + INDEX2 + " ON " + KEYSPACE + "." + TABLE + " (c6)";
  private static final String DROP_KEYSPACE_STMT = "DROP KEYSPACE " + KEYSPACE;
  private static final String COL_NAME1 = "c1";
  private static final String COL_NAME2 = "c2";
  private static final String COL_NAME3 = "c3";
  private static final String COL_NAME4 = "c4";
  private static final String COL_NAME5 = "c5";
  private static final String COL_NAME6 = "c6";
  private static final String COL_NAME7 = "c7";
  private static final String COL_NAME8 = "c8";
  private static final String COL_NAME9 = "c9";
  private static final String COL_NAME10 = "c10";
  private static final String COL_NAME11 = "c11";

  private ClusterManager clusterManager;

  @Before
  public void setUp() throws Exception {
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
    clusterManager = new ClusterManager(new DatabaseConfig(props));
    clusterManager.getSession();
  }

  @After
  public void tearDown() throws Exception {
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

  @Test
  public void test() throws Exception {
    CassandraTableMetadata tableMetadata =
        new CassandraTableMetadata(clusterManager.getMetadata(KEYSPACE, TABLE));

    assertThat(tableMetadata).isNotNull();

    assertThat(tableMetadata.getPartitionKeyNames().size()).isEqualTo(2);
    Iterator<String> iterator = tableMetadata.getPartitionKeyNames().iterator();
    assertThat(iterator.next()).isEqualTo(COL_NAME2);
    assertThat(iterator.next()).isEqualTo(COL_NAME1);

    assertThat(tableMetadata.getClusteringKeyNames().size()).isEqualTo(2);
    iterator = tableMetadata.getClusteringKeyNames().iterator();
    assertThat(iterator.next()).isEqualTo(COL_NAME4);
    assertThat(iterator.next()).isEqualTo(COL_NAME3);

    assertThat(tableMetadata.getColumnNames().size()).isEqualTo(11);
    assertThat(tableMetadata.getColumnNames().contains(COL_NAME1)).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(COL_NAME2)).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(COL_NAME3)).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(COL_NAME4)).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(COL_NAME5)).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(COL_NAME6)).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(COL_NAME7)).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(COL_NAME8)).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(COL_NAME9)).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(COL_NAME10)).isTrue();
    assertThat(tableMetadata.getColumnNames().contains(COL_NAME11)).isTrue();

    assertThat(tableMetadata.getColumnDataType(COL_NAME1)).isEqualTo(DataType.INT);
    assertThat(tableMetadata.getColumnDataType(COL_NAME2)).isEqualTo(DataType.TEXT);
    assertThat(tableMetadata.getColumnDataType(COL_NAME3)).isEqualTo(DataType.TEXT);
    assertThat(tableMetadata.getColumnDataType(COL_NAME4)).isEqualTo(DataType.INT);
    assertThat(tableMetadata.getColumnDataType(COL_NAME5)).isEqualTo(DataType.INT);
    assertThat(tableMetadata.getColumnDataType(COL_NAME6)).isEqualTo(DataType.TEXT);
    assertThat(tableMetadata.getColumnDataType(COL_NAME7)).isEqualTo(DataType.BIGINT);
    assertThat(tableMetadata.getColumnDataType(COL_NAME8)).isEqualTo(DataType.FLOAT);
    assertThat(tableMetadata.getColumnDataType(COL_NAME9)).isEqualTo(DataType.DOUBLE);
    assertThat(tableMetadata.getColumnDataType(COL_NAME10)).isEqualTo(DataType.BOOLEAN);
    assertThat(tableMetadata.getColumnDataType(COL_NAME11)).isEqualTo(DataType.BLOB);

    assertThat(tableMetadata.getClusteringOrder(COL_NAME1)).isNull();
    assertThat(tableMetadata.getClusteringOrder(COL_NAME2)).isNull();
    assertThat(tableMetadata.getClusteringOrder(COL_NAME3)).isEqualTo(Scan.Ordering.Order.DESC);
    assertThat(tableMetadata.getClusteringOrder(COL_NAME4)).isEqualTo(Scan.Ordering.Order.ASC);
    assertThat(tableMetadata.getClusteringOrder(COL_NAME5)).isNull();
    assertThat(tableMetadata.getClusteringOrder(COL_NAME6)).isNull();
    assertThat(tableMetadata.getClusteringOrder(COL_NAME7)).isNull();
    assertThat(tableMetadata.getClusteringOrder(COL_NAME8)).isNull();
    assertThat(tableMetadata.getClusteringOrder(COL_NAME9)).isNull();
    assertThat(tableMetadata.getClusteringOrder(COL_NAME10)).isNull();
    assertThat(tableMetadata.getClusteringOrder(COL_NAME11)).isNull();

    assertThat(tableMetadata.getSecondaryIndexNames().size()).isEqualTo(2);
    assertThat(tableMetadata.getSecondaryIndexNames().contains(COL_NAME5)).isTrue();
    assertThat(tableMetadata.getSecondaryIndexNames().contains(COL_NAME6)).isTrue();
  }
}
