package com.scalar.db.schemaloader.schema;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.concurrent.Immutable;

@Immutable
public class SchemaParser {
  public static List<Table> parse(Path jsonFilePath, Map<String, String> metaOptions)
      throws SchemaException, IOException {
    JsonObject schemaJson;
    try (Reader reader = Files.newBufferedReader(jsonFilePath)) {
      schemaJson = JsonParser.parseReader(reader).getAsJsonObject();
    }

    return parse(schemaJson, metaOptions);
  }

  public static List<Table> parse(String serializedSchemaJson, Map<String, String> metaOptions)
      throws SchemaException {
    JsonObject schemaJson = JsonParser.parseString(serializedSchemaJson).getAsJsonObject();

    return parse(schemaJson, metaOptions);
  }

  private static List<Table> parse(JsonObject schemaJson, Map<String, String> metaOptions) {
    return schemaJson.entrySet().stream()
        .map(table -> new Table(table.getKey(), table.getValue().getAsJsonObject(), metaOptions))
        .collect(Collectors.toList());
  }
}
