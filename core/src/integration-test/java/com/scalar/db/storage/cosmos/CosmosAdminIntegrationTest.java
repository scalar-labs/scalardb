package com.scalar.db.storage.cosmos;

import static org.assertj.core.api.Assertions.assertThat;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.ThroughputProperties;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.storage.AdminIntegrationTestBase;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class CosmosAdminIntegrationTest extends AdminIntegrationTestBase {

  private static final String METADATA_DATABASE = "scalardb";
  private static final String METADATA_CONTAINER = "metadata";

  private static Optional<String> namespacePrefix;
  private static CosmosClient client;
  private static DistributedStorageAdmin admin;

  @Before
  public void setUp() throws Exception {
    setUp(admin);
  }

  @Test
  @Override
  public void getTableMetadata_CorrectTableGiven_ShouldReturnCorrectClusteringOrders() {
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

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    String contactPoint = System.getProperty("scalardb.cosmos.uri");
    String password = System.getProperty("scalardb.cosmos.password");
    namespacePrefix = Optional.ofNullable(System.getProperty("scalardb.namespace_prefix"));

    client =
        new CosmosClientBuilder().endpoint(contactPoint).key(password).directMode().buildClient();

    // create the metadata database and container
    client.createDatabaseIfNotExists(
        database(METADATA_DATABASE), ThroughputProperties.createAutoscaledThroughput(4000));
    CosmosContainerProperties containerProperties =
        new CosmosContainerProperties(METADATA_CONTAINER, "/id");
    client.getDatabase(database(METADATA_DATABASE)).createContainerIfNotExists(containerProperties);

    // insert metadata
    CosmosTableMetadata metadata = new CosmosTableMetadata();
    metadata.setId(table(NAMESPACE, TABLE));
    metadata.setPartitionKeyNames(Arrays.asList(COL_NAME2, COL_NAME1));
    metadata.setClusteringKeyNames(Arrays.asList(COL_NAME4, COL_NAME3));
    metadata.setSecondaryIndexNames(new HashSet<>(Arrays.asList(COL_NAME5, COL_NAME6)));
    Map<String, String> columns = new HashMap<>();
    columns.put(COL_NAME1, "int");
    columns.put(COL_NAME2, "text");
    columns.put(COL_NAME3, "text");
    columns.put(COL_NAME4, "int");
    columns.put(COL_NAME5, "int");
    columns.put(COL_NAME6, "text");
    columns.put(COL_NAME7, "bigint");
    columns.put(COL_NAME8, "float");
    columns.put(COL_NAME9, "double");
    columns.put(COL_NAME10, "boolean");
    columns.put(COL_NAME11, "blob");
    metadata.setColumns(columns);
    client
        .getDatabase(database(METADATA_DATABASE))
        .getContainer(METADATA_CONTAINER)
        .createItem(metadata);

    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, contactPoint);
    props.setProperty(DatabaseConfig.PASSWORD, password);
    props.setProperty(DatabaseConfig.STORAGE, "cosmos");
    namespacePrefix.ifPresent(n -> props.setProperty(DatabaseConfig.NAMESPACE_PREFIX, n));
    admin = new CosmosAdmin(new DatabaseConfig(props));
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    CosmosDatabase database = client.getDatabase(database(METADATA_DATABASE));
    database.getContainer(METADATA_CONTAINER).delete();
    database.delete();

    client.close();
    admin.close();
  }

  private static String namespacePrefix() {
    return namespacePrefix.map(n -> n + "_").orElse("");
  }

  private static String database(String database) {
    return namespacePrefix() + database;
  }

  private static String table(String database, String table) {
    return database(database) + "." + table;
  }
}
