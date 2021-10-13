package com.scalar.db.schemaloader.schema;

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

public class TableTest {

  private Table table;

  @Mock private JsonObject tableDefinition;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void
      buildOptions_OptionsMapFromTableDefinitionAndMetaOptionsGiven_ShouldReturnMergedMap() {
    // Arrange
    Map<String, String> metaOptions =
        ImmutableMap.<String, String>builder().put("mo1", "vmo1").put("mo2", "vmo2").build();
    Map<String, String> expectedOptions = new HashMap<>(metaOptions);
    expectedOptions.put("to1", "vto1");
    expectedOptions.put("to2", "vto2");

    table = new Table(ImmutableSet.<String>builder().add("traveled1").build());

    when(tableDefinition.entrySet())
        .thenReturn(
            ImmutableMap.<String, JsonElement>builder()
                .put("traveled1", new JsonPrimitive("vtl1"))
                .put("to1", new JsonPrimitive("vto1"))
                .put("to2", new JsonPrimitive("vto2"))
                .build()
                .entrySet());

    // Act
    Map<String, String> options = table.buildOptions(tableDefinition, metaOptions);

    // Assert
    Assertions.assertThat(options).isEqualTo(expectedOptions);
  }

  @Test
  public void buildTableMetadata_MissingPartitionKeyInTableDefinition_ShouldThrowSchemaException() {
    // Arrange
    when(tableDefinition.keySet())
        .thenReturn(ImmutableSet.<String>builder().add("clustering-key").add("columns").build());
    table = new Table();

    // Act
    // Assert
    Assertions.assertThatThrownBy(() -> table.buildTableMetadata(tableDefinition))
        .isInstanceOf(SchemaException.class);
  }

  @Test
  public void
      buildTableMetadata_InvalidClusteringKeyInTableDefinition_ShouldThrowSchemaException() {
    // Arrange
    String tableDefinitionJson =
        "{\n"
            + "    \"transaction\": false,\n"
            + "    \"partition-key\": [\n"
            + "      \"c1\"\n"
            + "    ],\n"
            + "    \"clustering-key\": [\n"
            + "      \"c2 invalid format\"\n"
            + "    ]\n"
            + "  }";
    JsonObject invalidTableDefinition =
        JsonParser.parseString(tableDefinitionJson).getAsJsonObject();

    // Act
    // Assert
    Assertions.assertThatThrownBy(
            () -> new Table("ns.tb", invalidTableDefinition, Collections.emptyMap()))
        .isInstanceOf(SchemaException.class);
  }

  @Test
  public void
      buildTableMetadata_InvalidOrderOfClusteringKeyInTableDefinition_ShouldThrowSchemaException() {
    // Arrange
    String tableDefinitionJson =
        "{\n"
            + "    \"transaction\": false,\n"
            + "    \"partition-key\": [\n"
            + "      \"c1\"\n"
            + "    ],\n"
            + "    \"clustering-key\": [\n"
            + "      \"c2 invalid_order\"\n"
            + "    ]\n"
            + "  }";
    JsonObject invalidTableDefinition =
        JsonParser.parseString(tableDefinitionJson).getAsJsonObject();

    // Act
    // Assert
    Assertions.assertThatThrownBy(
            () -> new Table("ns.tb", invalidTableDefinition, Collections.emptyMap()))
        .isInstanceOf(SchemaException.class);
  }

  @Test
  public void
      buildTableMetadata_MissingColumnDefinitionInTableDefinition_ShouldThrowSchemaException() {
    // Arrange
    String tableDefinitionJson =
        "{\n"
            + "    \"transaction\": false,\n"
            + "    \"partition-key\": [\n"
            + "      \"c1\"\n"
            + "    ],\n"
            + "    \"clustering-key\": [\n"
            + "      \"c2 ASC\"\n"
            + "    ]\n"
            + "  }";
    JsonObject invalidTableDefinition =
        JsonParser.parseString(tableDefinitionJson).getAsJsonObject();

    // Act
    // Assert
    Assertions.assertThatThrownBy(
            () -> new Table("ns.tb", invalidTableDefinition, Collections.emptyMap()))
        .isInstanceOf(SchemaException.class);
  }

  @Test
  public void
      buildTableMetadata_InvalidColumnTypeDefinitionInTableDefinition_ShouldThrowSchemaException() {
    // Arrange
    String tableDefinitionJson =
        "{\n"
            + "    \"transaction\": false,\n"
            + "    \"partition-key\": [\n"
            + "      \"c1\"\n"
            + "    ],\n"
            + "    \"clustering-key\": [\n"
            + "      \"c2 ASC\"\n"
            + "    ],\n"
            + "    \"columns\": {\n"
            + "      \"c1\": \"INT\",\n"
            + "      \"c2\": \"TEXT\",\n"
            + "      \"c3\": \"INVALID_TYPE\"\n"
            + "    }\n"
            + "  }";
    JsonObject invalidTableDefinition =
        JsonParser.parseString(tableDefinitionJson).getAsJsonObject();

    // Act
    // Assert
    Assertions.assertThatThrownBy(
            () -> new Table("ns.tb", invalidTableDefinition, Collections.emptyMap()))
        .isInstanceOf(SchemaException.class);
  }

  @Test
  public void
      buildTableMetadata_ProperFormatTableDefinitionGiven_ShouldReturnProperTableMetadata() {
    String tableDefinitionJson =
        "{\n"
            + "    \"transaction\": false,\n"
            + "    \"partition-key\": [\n"
            + "      \"c1\"\n"
            + "    ],\n"
            + "    \"clustering-key\": [\n"
            + "      \"c3\",\n"
            + "      \"c4 ASC\",\n"
            + "      \"c6 DESC\"\n"
            + "    ],\n"
            + "    \"columns\": {\n"
            + "      \"c1\": \"INT\",\n"
            + "      \"c2\": \"TEXT\",\n"
            + "      \"c3\": \"BLOB\",\n"
            + "      \"c4\": \"INT\",\n"
            + "      \"c5\": \"BOOLEAN\",\n"
            + "      \"c6\": \"INT\"\n"
            + "    },\n"
            + "    \"ru\": 5000,\n"
            + "    \"compaction-strategy\": \"LCS\",\n"
            + "    \"secondary-index\": [\n"
            + "      \"c2\",\n"
            + "      \"c4\"\n"
            + "    ]\n"
            + "  }";
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
        new Table("ns.tb", tableDefinition, Collections.emptyMap()).getTableMetadata();

    // Assert
    Assertions.assertThat(tableMetadata).isEqualTo(expectedTableMetadata);
  }

  @Test
  public void Table_WrongFormatTableFullNameGiven_ShouldThrowSchemaException() {
    // Arrange
    String tableFullName = "namespace_and_table_without_dot_separator";

    // Act
    // Assert
    Assertions.assertThatThrownBy(
            () -> new Table(tableFullName, tableDefinition, Collections.emptyMap()))
        .isInstanceOf(SchemaException.class);
  }
}
