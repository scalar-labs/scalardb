package com.scalar.db.storage;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.service.StorageFactory;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

@SuppressFBWarnings(
    value = {"MS_CANNOT_BE_FINAL", "MS_PKGPROTECT", "ST_WRITE_TO_STATIC_FROM_INSTANCE_METHOD"})
public abstract class AdminIntegrationTestBase {

  protected static final String NAMESPACE = "integration_testing";
  protected static final String TABLE = "test_table";
  protected static final String COL_NAME1 = "c1";
  protected static final String COL_NAME2 = "c2";
  protected static final String COL_NAME3 = "c3";
  protected static final String COL_NAME4 = "c4";
  protected static final String COL_NAME5 = "c5";
  protected static final String COL_NAME6 = "c6";
  protected static final String COL_NAME7 = "c7";
  protected static final String COL_NAME8 = "c8";
  protected static final String COL_NAME9 = "c9";
  protected static final String COL_NAME10 = "c10";
  protected static final String COL_NAME11 = "c11";

  private static boolean initialized;
  protected static DistributedStorageAdmin admin;
  protected static String namespace;

  @Before
  public void setUp() throws Exception {
    if (!initialized) {
      initialize();
      StorageFactory factory = new StorageFactory(getDatabaseConfig());
      admin = factory.getAdmin();
      namespace = getNamespace();
      createTable();
      initialized = true;
    }
  }

  protected void initialize() throws Exception {}

  protected abstract DatabaseConfig getDatabaseConfig();

  protected String getNamespace() {
    return NAMESPACE;
  }

  private void createTable() throws ExecutionException {
    Map<String, String> options = getCreateOptions();
    admin.createNamespace(namespace, true, options);
    admin.createTable(
        namespace,
        TABLE,
        TableMetadata.newBuilder()
            .addColumn(COL_NAME1, DataType.INT)
            .addColumn(COL_NAME2, DataType.TEXT)
            .addColumn(COL_NAME3, DataType.TEXT)
            .addColumn(COL_NAME4, DataType.INT)
            .addColumn(COL_NAME5, DataType.INT)
            .addColumn(COL_NAME6, DataType.TEXT)
            .addColumn(COL_NAME7, DataType.BIGINT)
            .addColumn(COL_NAME8, DataType.FLOAT)
            .addColumn(COL_NAME9, DataType.DOUBLE)
            .addColumn(COL_NAME10, DataType.BOOLEAN)
            .addColumn(COL_NAME11, DataType.BLOB)
            .addPartitionKey(COL_NAME2)
            .addPartitionKey(COL_NAME1)
            .addClusteringKey(COL_NAME4, Scan.Ordering.Order.ASC)
            .addClusteringKey(COL_NAME3, Scan.Ordering.Order.DESC)
            .addSecondaryIndex(COL_NAME5)
            .addSecondaryIndex(COL_NAME6)
            .build(),
        true,
        options);
  }

  protected Map<String, String> getCreateOptions() {
    return Collections.emptyMap();
  }

  @AfterClass
  public static void tearDownAfterClass() throws ExecutionException {
    deleteTable();
    admin.close();
  }

  private static void deleteTable() throws ExecutionException {
    admin.dropTable(namespace, TABLE);
    admin.dropNamespace(namespace);
  }

  @Test
  public void getTableMetadata_CorrectTableGiven_MetadataShouldNotBeNull()
      throws ExecutionException {
    TableMetadata tableMetadata = admin.getTableMetadata(namespace, TABLE);
    assertThat(tableMetadata).isNotNull();
  }

  @Test
  public void getTableMetadata_CorrectTableGiven_ShouldReturnCorrectPartitionKeyNames()
      throws ExecutionException {
    TableMetadata tableMetadata = admin.getTableMetadata(namespace, TABLE);

    assertThat(tableMetadata.getPartitionKeyNames().size()).isEqualTo(2);
    Iterator<String> iterator = tableMetadata.getPartitionKeyNames().iterator();
    assertThat(iterator.next()).isEqualTo(COL_NAME2);
    assertThat(iterator.next()).isEqualTo(COL_NAME1);
  }

  @Test
  public void getTableMetadata_CorrectTableGiven_ShouldReturnCorrectClusteringKeyNames()
      throws ExecutionException {
    TableMetadata tableMetadata = admin.getTableMetadata(namespace, TABLE);

    assertThat(tableMetadata.getClusteringKeyNames().size()).isEqualTo(2);
    Iterator<String> iterator = tableMetadata.getClusteringKeyNames().iterator();
    assertThat(iterator.next()).isEqualTo(COL_NAME4);
    assertThat(iterator.next()).isEqualTo(COL_NAME3);
  }

  @Test
  public void getTableMetadata_CorrectTableGiven_ShouldReturnCorrectColumnNames()
      throws ExecutionException {
    TableMetadata tableMetadata = admin.getTableMetadata(namespace, TABLE);

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
  }

  @Test
  public void getTableMetadata_CorrectTableGiven_ShouldReturnCorrectColumnDataTypes()
      throws ExecutionException {
    TableMetadata tableMetadata = admin.getTableMetadata(namespace, TABLE);

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
  }

  @Test
  public void getTableMetadata_CorrectTableGiven_ShouldReturnCorrectClusteringOrders()
      throws ExecutionException {
    TableMetadata tableMetadata = admin.getTableMetadata(namespace, TABLE);

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
  }

  @Test
  public void getTableMetadata_CorrectTableGiven_ShouldReturnCorrectSecondaryIndexNames()
      throws ExecutionException {
    TableMetadata tableMetadata = admin.getTableMetadata(namespace, TABLE);

    assertThat(tableMetadata.getSecondaryIndexNames().size()).isEqualTo(2);
    assertThat(tableMetadata.getSecondaryIndexNames().contains(COL_NAME5)).isTrue();
    assertThat(tableMetadata.getSecondaryIndexNames().contains(COL_NAME6)).isTrue();
  }

  @Test
  public void getTableMetadata_WrongTableGiven_ShouldReturnNull() throws ExecutionException {
    TableMetadata tableMetadata = admin.getTableMetadata("wrong_ns", "wrong_table");
    assertThat(tableMetadata).isNull();
  }
}
