package com.scalar.db.storage.cosmos;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.DataType;
import com.scalar.db.storage.AdminIntegrationTestBase;
import java.util.Optional;
import java.util.Properties;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class CosmosAdminIntegrationTest extends AdminIntegrationTestBase {
  private static final String PROP_COSMOSDB_URI = "scalardb.cosmos.uri";
  private static final String PROP_COSMOSDB_PASSWORD = "scalardb.cosmos.password";
  private static final String PROP_NAMESPACE_PREFIX = "scalardb.namespace_prefix";

  private static DistributedStorageAdmin admin;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    String contactPoint = System.getProperty(PROP_COSMOSDB_URI);
    String password = System.getProperty(PROP_COSMOSDB_PASSWORD);
    Optional<String> namespacePrefix =
        Optional.ofNullable(System.getProperty(PROP_NAMESPACE_PREFIX));

    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, contactPoint);
    props.setProperty(DatabaseConfig.PASSWORD, password);
    props.setProperty(DatabaseConfig.STORAGE, "cosmos");
    namespacePrefix.ifPresent(n -> props.setProperty(DatabaseConfig.NAMESPACE_PREFIX, n));
    admin = new CosmosAdmin(new CosmosConfig(props));
    admin.createNamespace(NAMESPACE, ImmutableMap.of(CosmosAdmin.REQUEST_UNIT, "4000"));
    admin.createTable(
        NAMESPACE,
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
            .build());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    admin.dropTable(NAMESPACE, TABLE);
    admin.dropNamespace(NAMESPACE);
    admin.close();
  }

  @Before
  public void setUp() {
    setUp(admin);
  }

  @Test
  @Override
  public void getTableMetadata_CorrectTableGiven_ShouldReturnCorrectClusteringOrders()
      throws ExecutionException {
    TableMetadata tableMetadata = admin.getTableMetadata(NAMESPACE, TABLE);

    // Fow now, the clustering order is always ASC in the Cosmos DB adapter
    assertThat(tableMetadata.getClusteringOrder(COL_NAME1)).isNull();
    assertThat(tableMetadata.getClusteringOrder(COL_NAME2)).isNull();
    assertThat(tableMetadata.getClusteringOrder(COL_NAME3)).isEqualTo(Scan.Ordering.Order.ASC);
    assertThat(tableMetadata.getClusteringOrder(COL_NAME4)).isEqualTo(Scan.Ordering.Order.ASC);
    assertThat(tableMetadata.getClusteringOrder(COL_NAME5)).isNull();
    assertThat(tableMetadata.getClusteringOrder(COL_NAME6)).isNull();
    assertThat(tableMetadata.getClusteringOrder(COL_NAME7)).isNull();
    assertThat(tableMetadata.getClusteringOrder(COL_NAME8)).isNull();
    assertThat(tableMetadata.getClusteringOrder(COL_NAME9)).isNull();
    assertThat(tableMetadata.getClusteringOrder(COL_NAME10)).isNull();
    assertThat(tableMetadata.getClusteringOrder(COL_NAME11)).isNull();
  }
}
