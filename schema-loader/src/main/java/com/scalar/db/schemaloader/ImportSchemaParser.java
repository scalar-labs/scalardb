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
public class ImportSchemaParser {
  private final JsonObject schemaJson;
  private final Map<String, String> options;

  public ImportSchemaParser(Path jsonFilePath, Map<String, String> options) throws IOException {
    try (Reader reader = Files.newBufferedReader(jsonFilePath)) {
      schemaJson = JsonParser.parseReader(reader).getAsJsonObject();
    }
    this.options = ImmutableMap.copyOf(options);
  }

  public ImportSchemaParser(String serializedSchemaJson, Map<String, String> options) {
    schemaJson = JsonParser.parseString(serializedSchemaJson).getAsJsonObject();
    this.options = ImmutableMap.copyOf(options);
  }

  // For the SpotBugs warning CT_CONSTRUCTOR_THROW
  @Override
  protected final void finalize() {}

  public List<ImportTableSchema> parse() {
    List<ImportTableSchema> tableSchemaList = new ArrayList<>();
    for (Map.Entry<String, JsonElement> entry : schemaJson.entrySet()) {
      tableSchemaList.add(
          new ImportTableSchema(entry.getKey(), entry.getValue().getAsJsonObject(), options));
    }
    return tableSchemaList;
  }
}
