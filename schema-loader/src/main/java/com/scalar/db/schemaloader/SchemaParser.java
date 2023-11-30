package com.scalar.db.schemaloader;

import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
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

  public SchemaParser(Path jsonFilePath, Map<String, String> options) throws SchemaLoaderException {
    try (Reader reader = Files.newBufferedReader(jsonFilePath)) {
      schemaJson = JsonParser.parseReader(reader).getAsJsonObject();
    } catch (IOException | JsonParseException e) {
      throw new SchemaLoaderException("Parsing the schema JSON failed", e);
    }
    this.options = ImmutableMap.copyOf(options);
  }

  public SchemaParser(String serializedSchemaJson, Map<String, String> options)
      throws SchemaLoaderException {
    try {
      schemaJson = JsonParser.parseString(serializedSchemaJson).getAsJsonObject();
    } catch (JsonParseException e) {
      throw new SchemaLoaderException("Parsing the schema JSON failed", e);
    }
    this.options = ImmutableMap.copyOf(options);
  }

  // For the SpotBugs warning CT_CONSTRUCTOR_THROW
  @Override
  protected final void finalize() {}

  public List<TableSchema> parse() throws SchemaLoaderException {
    ArrayList<TableSchema> tableSchemaList = new ArrayList<>();
    for (Map.Entry<String, JsonElement> entry : schemaJson.entrySet()) {
      tableSchemaList.add(
          new TableSchema(entry.getKey(), entry.getValue().getAsJsonObject(), options));
    }
    return tableSchemaList;
  }
}
