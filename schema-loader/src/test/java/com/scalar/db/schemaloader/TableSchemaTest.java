package com.scalar.db.schemaloader;

import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class TableSchemaTest {

  @Mock private JsonObject tableDefinition;
  private TableSchema tableSchema;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
  }

  @Test
  public void buildOptions_OptionsMapFromTableDefinitionAndOptionsGiven_ShouldReturnMergedMap() {
    // Arrange
    Map<String, String> options =
        ImmutableMap.<String, String>builder().put("mo1", "vmo1").put("mo2", "vmo2").build();
    Map<String, String> expectedOptions = new HashMap<>(options);
    expectedOptions.put("to1", "vto1");
    expectedOptions.put("to2", "vto2");

    tableSchema = new TableSchema(ImmutableSet.<String>builder().add("traveled1").build());

    when(tableDefinition.entrySet())
        .thenReturn(
            ImmutableMap.<String, JsonElement>builder()
                .put("traveled1", new JsonPrimitive("vtl1"))
                .put("to1", new JsonPrimitive("vto1"))
                .put("to2", new JsonPrimitive("vto2"))
                .build()
                .entrySet());

    // Act
    Map<String, String> actual = tableSchema.buildOptions(tableDefinition, options);

    // Assert
    Assertions.assertThat(actual).isEqualTo(expectedOptions);
  }

  @Test
  public void
      buildTableMetadata_MissingPartitionKeyInTableDefinition_ShouldThrowSchemaLoaderException() {
    // Arrange
    when(tableDefinition.keySet())
        .thenReturn(ImmutableSet.<String>builder().add("clustering-key").add("columns").build());
    tableSchema = new TableSchema();

    // Act Assert
    Assertions.assertThatThrownBy(() -> tableSchema.buildTableMetadata(tableDefinition))
        .isInstanceOf(SchemaLoaderException.class);
  }

  @Test
  public void
      buildTableMetadata_InvalidClusteringKeyInTableDefinition_ShouldThrowSchemaLoaderException() {
    // Arrange
    String tableDefinitionJson =
        "{\"transaction\": false,"
            + "\"partition-key\": [\"c1\"],"
            + "\"clustering-key\": [\"c2 invalid format\"]}";
    JsonObject invalidTableDefinition =
        JsonParser.parseString(tableDefinitionJson).getAsJsonObject();

    // Act Assert
    Assertions.assertThatThrownBy(
            () -> new TableSchema("ns.tb", invalidTableDefinition, Collections.emptyMap()))
        .isInstanceOf(SchemaLoaderException.class);
  }

  @Test
  public void
      buildTableMetadata_InvalidOrderOfClusteringKeyInTableDefinition_ShouldThrowSchemaLoaderException() {
    // Arrange
    String tableDefinitionJson =
        "{\"transaction\": false,"
            + "\"partition-key\": [\"c1\"],"
            + "\"clustering-key\": [\"c2 invalid_order\"]}";
    JsonObject invalidTableDefinition =
        JsonParser.parseString(tableDefinitionJson).getAsJsonObject();

    // Act Assert
    Assertions.assertThatThrownBy(
            () -> new TableSchema("ns.tb", invalidTableDefinition, Collections.emptyMap()))
        .isInstanceOf(SchemaLoaderException.class);
  }

  @Test
  public void
      buildTableMetadata_MissingColumnDefinitionInTableDefinition_ShouldThrowSchemaLoaderException() {
    // Arrange
    String tableDefinitionJson =
        "{\"transaction\": false,"
            + "\"partition-key\": [\"c1\"],"
            + "\"clustering-key\": [\"c2 ASC\"]}";
    JsonObject invalidTableDefinition =
        JsonParser.parseString(tableDefinitionJson).getAsJsonObject();

    // Act Assert
    Assertions.assertThatThrownBy(
            () -> new TableSchema("ns.tb", invalidTableDefinition, Collections.emptyMap()))
        .isInstanceOf(SchemaLoaderException.class);
  }

  @Test
  public void
      buildTableMetadata_InvalidColumnTypeDefinitionInTableDefinition_ShouldThrowSchemaLoaderException() {
    // Arrange
    String tableDefinitionJson =
        "{\"transaction\": false,"
            + "\"partition-key\": [\"c1\"],"
            + "\"clustering-key\": [\"c2 ASC\"],"
            + "\"columns\": {\"c1\": \"INT\",\"c2\": \"TEXT\",\"c3\": \"INVALID_TYPE\"}}";
    JsonObject invalidTableDefinition =
        JsonParser.parseString(tableDefinitionJson).getAsJsonObject();

    // Act Assert
    Assertions.assertThatThrownBy(
            () -> new TableSchema("ns.tb", invalidTableDefinition, Collections.emptyMap()))
        .isInstanceOf(SchemaLoaderException.class);
  }

  @Test
  public void buildTableMetadata_ProperFormatTableDefinitionGiven_ShouldReturnProperTableMetadata()
      throws SchemaLoaderException {
    String tableDefinitionJson =
        "{\"transaction\": false,"
            + "\"partition-key\": [\"c1\"],"
            + "\"clustering-key\": [\"c3\",\"c4 ASC\",\"c6 DESC\"],"
            + "\"columns\": {"
            + "  \"c1\": \"INT\","
            + "  \"c2\": \"TEXT\","
            + "  \"c3\": \"BLOB\","
            + "  \"c4\": \"INT\","
            + "  \"c5\": \"BOOLEAN\","
            + "  \"c6\": \"INT\""
            + "},"
            + "\"ru\": 5000,"
            + "\"compaction-strategy\": \"LCS\","
            + "\"secondary-index\": [\"c2\",\"c4\"]}";
    JsonObject tableDefinition = JsonParser.parseString(tableDefinitionJson).getAsJsonObject();

    TableMetadata.Builder tableBuilder = TableMetadata.newBuilder();
    tableBuilder.addPartitionKey("c1");
    tableBuilder.addClusteringKey("c3");
    tableBuilder.addClusteringKey("c4");
    tableBuilder.addClusteringKey("c6", Order.DESC);
    tableBuilder.addSecondaryIndex("c2");
    tableBuilder.addSecondaryIndex("c4");
    tableBuilder.addColumn("c1", DataType.INT);
    tableBuilder.addColumn("c2", DataType.TEXT);
    tableBuilder.addColumn("c3", DataType.BLOB);
    tableBuilder.addColumn("c4", DataType.INT);
    tableBuilder.addColumn("c5", DataType.BOOLEAN);
    tableBuilder.addColumn("c6", DataType.INT);
    TableMetadata expectedTableMetadata = tableBuilder.build();

    // Act
    TableMetadata tableMetadata =
        new TableSchema("ns.tb", tableDefinition, Collections.emptyMap()).getTableMetadata();

    // Assert
    Assertions.assertThat(tableMetadata).isEqualTo(expectedTableMetadata);
  }

  @Test
  public void Table_WrongFormatTableFullNameGiven_ShouldThrowSchemaLoaderException() {
    // Arrange
    String tableFullName = "namespace_and_table_without_dot_separator";

    // Act Assert
    Assertions.assertThatThrownBy(
            () -> new TableSchema(tableFullName, tableDefinition, Collections.emptyMap()))
        .isInstanceOf(SchemaLoaderException.class);
  }
}
