package com.scalar.db.schemaloader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.scalar.db.io.DataType;
import java.util.Collections;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class ImportTableSchemaTest {

  @Mock private JsonObject tableDefinition;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
  }

  @Test
  public void constructor_DefinitionWithTransactionTrueGiven_ShouldConstructProperTableSchema() {
    String tableDefinitionJson = "{\"transaction\": true}";
    JsonObject tableDefinition = JsonParser.parseString(tableDefinitionJson).getAsJsonObject();

    // Act
    ImportTableSchema tableSchema =
        new ImportTableSchema("ns.tbl", tableDefinition, Collections.emptyMap());

    // Assert
    assertThat(tableSchema.getNamespace()).isEqualTo("ns");
    assertThat(tableSchema.getTable()).isEqualTo("tbl");
    assertThat(tableSchema.isTransactionTable()).isEqualTo(true);
    assertThat(tableSchema.getOptions()).isEmpty();
    assertThat(tableSchema.getOverrideColumnsType()).isEmpty();
  }

  @Test
  public void constructor_DefinitionWithTransactionFalseGiven_ShouldConstructProperTableSchema() {
    String tableDefinitionJson = "{\"transaction\": false}";
    JsonObject tableDefinition = JsonParser.parseString(tableDefinitionJson).getAsJsonObject();

    // Act
    ImportTableSchema tableSchema =
        new ImportTableSchema("ns.tbl", tableDefinition, Collections.emptyMap());

    // Assert
    assertThat(tableSchema.getNamespace()).isEqualTo("ns");
    assertThat(tableSchema.getTable()).isEqualTo("tbl");
    assertThat(tableSchema.isTransactionTable()).isEqualTo(false);
    assertThat(tableSchema.getOptions()).isEmpty();
    assertThat(tableSchema.getOverrideColumnsType()).isEmpty();
  }

  @Test
  public void
      constructor_DefinitionWithOverrideColumnsTypeGiven_ShouldConstructProperTableSchema() {
    String tableDefinitionJson =
        "{"
            + "    \"transaction\": true,"
            + "    \"override-columns-type\": {"
            + "      \"c3\": \"TIME\","
            + "      \"c5\": \"TIMESTAMP\""
            + "    }"
            + "  }";
    JsonObject tableDefinition = JsonParser.parseString(tableDefinitionJson).getAsJsonObject();

    // Act
    ImportTableSchema tableSchema =
        new ImportTableSchema("ns.tbl", tableDefinition, Collections.emptyMap());

    // Assert
    assertThat(tableSchema.getNamespace()).isEqualTo("ns");
    assertThat(tableSchema.getTable()).isEqualTo("tbl");
    assertThat(tableSchema.isTransactionTable()).isEqualTo(true);
    assertThat(tableSchema.getOptions()).isEmpty();
    assertThat(tableSchema.getOverrideColumnsType())
        .containsOnly(entry("c3", DataType.TIME), entry("c5", DataType.TIMESTAMP));
  }

  @Test
  public void constructor_WrongFormatTableFullNameGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    String tableFullName = "namespace_and_table_without_dot_separator";

    // Act Assert
    Assertions.assertThatThrownBy(
            () -> new ImportTableSchema(tableFullName, tableDefinition, Collections.emptyMap()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void constructor_DefinitionWithoutTransactionGiven_ShouldConstructProperTableSchema() {
    String tableDefinitionJson = "{}";
    JsonObject tableDefinition = JsonParser.parseString(tableDefinitionJson).getAsJsonObject();

    // Act
    ImportTableSchema tableSchema =
        new ImportTableSchema("ns.tbl", tableDefinition, Collections.emptyMap());

    // Assert
    assertThat(tableSchema.getNamespace()).isEqualTo("ns");
    assertThat(tableSchema.getTable()).isEqualTo("tbl");
    assertThat(tableSchema.isTransactionTable()).isEqualTo(true);
    assertThat(tableSchema.getOptions()).isEmpty();
    assertThat(tableSchema.getOverrideColumnsType()).isEmpty();
  }

  @Test
  public void constructor_DefinitionWithGlobalAndSchemaOptions_ShouldConstructWithProperOptions() {
    String tableDefinitionJson =
        "{\"partition-key\": \"ignored\", \"columns\": \"ignored\", \"clustering-key\": \"ignored\", \"secondary-index\": \"ignored\",\"transaction\": false, \"opt1\": \"schema-opt1\", \"opt3\": \"schema-opt3\", \"override-columns-type\": {\"c1\": \"DOUBLE\"}}";
    JsonObject tableDefinition = JsonParser.parseString(tableDefinitionJson).getAsJsonObject();

    // Act
    ImportTableSchema tableSchema =
        new ImportTableSchema(
            "ns.tbl",
            tableDefinition,
            ImmutableMap.of("opt1", "global-opt1", "opt2", "global-opt2"));

    // Assert
    assertThat(tableSchema.getNamespace()).isEqualTo("ns");
    assertThat(tableSchema.getTable()).isEqualTo("tbl");
    assertThat(tableSchema.isTransactionTable()).isEqualTo(false);
    assertThat(tableSchema.getOptions())
        .containsOnly(
            entry("opt1", "schema-opt1"),
            entry("opt2", "global-opt2"),
            entry("opt3", "schema-opt3"));
    assertThat(tableSchema.getOverrideColumnsType()).containsOnly(entry("c1", DataType.DOUBLE));
  }
}
