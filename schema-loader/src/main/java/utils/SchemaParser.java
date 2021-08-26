package utils;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SchemaParser {

  List<Table> tableList;
  boolean hasTransactionTable = false;

  public SchemaParser(String jsonFilePath, Map<String, String> metaOptions) throws Exception {
    tableList = new LinkedList<Table>();
    Reader reader = Files.newBufferedReader(Paths.get(jsonFilePath));
    JsonObject schemaJson = JsonParser.parseReader(reader).getAsJsonObject();

    for (Map.Entry<String, JsonElement> table : schemaJson.entrySet()) {
      Logger.getGlobal().log(Level.FINE, "table full name: " + table.getKey());
      Table t = new Table(table.getKey(), table.getValue().getAsJsonObject(), metaOptions);
      tableList.add(t);
      if (t.isTransactionTable()) {
        hasTransactionTable = true;
      }
    }
    reader.close();
  }

  public List<Table> getTables() {
    return tableList;
  }

  public boolean hasTransactionTable() {
    return hasTransactionTable;
  }
}
