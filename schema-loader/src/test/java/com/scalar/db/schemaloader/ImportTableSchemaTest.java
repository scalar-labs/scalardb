package com.scalar.db.schemaloader;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
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
  public void constructor_DefinitionWithTransactionTrueGiven_ShouldConstructProperTableSchema()
      throws SchemaLoaderException {
    String tableDefinitionJson = "{\"transaction\": true}";
    JsonObject tableDefinition = JsonParser.parseString(tableDefinitionJson).getAsJsonObject();

    // Act
    ImportTableSchema tableSchema = new ImportTableSchema("ns.tbl", tableDefinition);

    // Assert
    Assertions.assertThat(tableSchema.getNamespace()).isEqualTo("ns");
    Assertions.assertThat(tableSchema.getTable()).isEqualTo("tbl");
    Assertions.assertThat(tableSchema.isTransactionTable()).isEqualTo(true);
  }

  @Test
  public void constructor_DefinitionWithTransactionFalseGiven_ShouldConstructProperTableSchema()
      throws SchemaLoaderException {
    String tableDefinitionJson = "{\"transaction\": false}";
    JsonObject tableDefinition = JsonParser.parseString(tableDefinitionJson).getAsJsonObject();

    // Act
    ImportTableSchema tableSchema = new ImportTableSchema("ns.tbl", tableDefinition);

    // Assert
    Assertions.assertThat(tableSchema.getNamespace()).isEqualTo("ns");
    Assertions.assertThat(tableSchema.getTable()).isEqualTo("tbl");
    Assertions.assertThat(tableSchema.isTransactionTable()).isEqualTo(false);
  }

  @Test
  public void constructor_WrongFormatTableFullNameGiven_ShouldThrowSchemaLoaderException() {
    // Arrange
    String tableFullName = "namespace_and_table_without_dot_separator";

    // Act Assert
    Assertions.assertThatThrownBy(() -> new ImportTableSchema(tableFullName, tableDefinition))
        .isInstanceOf(SchemaLoaderException.class);
  }

  @Test
  public void constructor_DefinitionWithoutTransactionGiven_ShouldConstructProperTableSchema()
      throws SchemaLoaderException {
    String tableDefinitionJson = "{}";
    JsonObject tableDefinition = JsonParser.parseString(tableDefinitionJson).getAsJsonObject();

    // Act
    ImportTableSchema tableSchema = new ImportTableSchema("ns.tbl", tableDefinition);

    // Assert
    Assertions.assertThat(tableSchema.getNamespace()).isEqualTo("ns");
    Assertions.assertThat(tableSchema.getTable()).isEqualTo("tbl");
    Assertions.assertThat(tableSchema.isTransactionTable()).isEqualTo(true);
  }
}
