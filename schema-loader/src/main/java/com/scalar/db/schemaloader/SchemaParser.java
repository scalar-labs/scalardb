package com.scalar.db.schemaloader;

import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class SchemaParser {

  private final JsonObject schemaJson;
  private final Map<String, String> options;

  public SchemaParser(Path jsonFilePath, Map<String, String> options) throws IOException {
    try (Reader reader = Files.newBufferedReader(jsonFilePath)) {
      schemaJson = JsonParser.parseReader(reader).getAsJsonObject();
    }
    this.options = ImmutableMap.copyOf(options);
  }

  public SchemaParser(String serializedSchemaJson, Map<String, String> options) {
    schemaJson = JsonParser.parseString(serializedSchemaJson).getAsJsonObject();
    this.options = ImmutableMap.copyOf(options);
  }

  public List<TableSchema> parse() {
    List<TableSchema> tableSchemaList = new ArrayList<>();
    for (Map.Entry<String, JsonElement> entry : schemaJson.entrySet()) {
      tableSchemaList.add(
          new TableSchema(entry.getKey(), entry.getValue().getAsJsonObject(), options));
    }
    return tableSchemaList;
  }
}
