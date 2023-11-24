package com.scalar.db.schemaloader;

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
public class ImportSchemaParser {
  private final JsonObject schemaJson;

  public ImportSchemaParser(Path jsonFilePath) throws SchemaLoaderException {
    try (Reader reader = Files.newBufferedReader(jsonFilePath)) {
      schemaJson = JsonParser.parseReader(reader).getAsJsonObject();
    } catch (IOException | JsonParseException e) {
      throw new SchemaLoaderException("Parsing the schema JSON failed", e);
    }
  }

  public ImportSchemaParser(String serializedSchemaJson) throws SchemaLoaderException {
    try {
      schemaJson = JsonParser.parseString(serializedSchemaJson).getAsJsonObject();
    } catch (JsonParseException e) {
      throw new SchemaLoaderException("Parsing the schema JSON failed", e);
    }
  }

  // For the SpotBugs warning CT_CONSTRUCTOR_THROW
  @Override
  protected final void finalize() {}

  public List<ImportTableSchema> parse() throws SchemaLoaderException {
    List<ImportTableSchema> tableSchemaList = new ArrayList<>();
    for (Map.Entry<String, JsonElement> entry : schemaJson.entrySet()) {
      tableSchemaList.add(
          new ImportTableSchema(entry.getKey(), entry.getValue().getAsJsonObject()));
    }
    return tableSchemaList;
  }
}
