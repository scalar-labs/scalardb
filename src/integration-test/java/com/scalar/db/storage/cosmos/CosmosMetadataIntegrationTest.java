package com.scalar.db.storage.cosmos;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.ThroughputProperties;
import com.scalar.db.api.Get;
import com.scalar.db.api.Scan;
import com.scalar.db.io.Key;
import com.scalar.db.storage.common.metadata.DataType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class CosmosMetadataIntegrationTest {

  private static final String METADATA_DATABASE = "scalardb";
  private static final String METADATA_CONTAINER = "metadata";
  private static final String DATABASE = "integration_testing";
  private static final String CONTAINER = "test_table";

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

  private Optional<String> namespacePrefix;
  private CosmosClient client;

  @Before
  public void setUp() throws Exception {
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
    metadata.setId(table(DATABASE, CONTAINER));
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
  }

  @After
  public void tearDown() throws Exception {
    CosmosDatabase database = client.getDatabase(database(METADATA_DATABASE));
    database.getContainer(METADATA_CONTAINER).delete();
    database.delete();

    client.close();
  }

  private String namespacePrefix() {
    return namespacePrefix.map(n -> n + "_").orElse("");
  }

  private String database(String database) {
    return namespacePrefix() + database;
  }

  private String table(String database, String table) {
    return database(database) + "." + table;
  }

  @Test
  public void test() throws Exception {
    CosmosContainer container =
        client.getDatabase(database(METADATA_DATABASE)).getContainer(METADATA_CONTAINER);
    TableMetadataManager tableMetadataManager = new TableMetadataManager(container);

    Get dummyOperation = new Get(new Key()).forNamespace(DATABASE).forTable(CONTAINER);
    namespacePrefix.ifPresent(n -> dummyOperation.forNamespacePrefix(namespacePrefix()));
    CosmosTableMetadata tableMetadata = tableMetadataManager.getTableMetadata(dummyOperation);

    assertThat(tableMetadata).isNotNull();

    assertThat(tableMetadata.getId()).isEqualTo(table(DATABASE, CONTAINER));

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
    assertThat(tableMetadata.getClusteringOrder(COL_NAME3)).isEqualTo(Scan.Ordering.Order.ASC);
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
