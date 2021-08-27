package schema;

import com.google.common.collect.ImmutableList;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.concurrent.Immutable;

@Immutable
public class SchemaParser {

  List<Table> tableList;

  public SchemaParser(String jsonFilePath, Map<String, String> metaOptions) throws Exception {
    tableList = new LinkedList<>();
    Reader reader = Files.newBufferedReader(Paths.get(jsonFilePath));
    JsonObject schemaJson = JsonParser.parseReader(reader).getAsJsonObject();

    tableList = schemaJson.entrySet().stream().map(
        table -> new Table(table.getKey(), table.getValue().getAsJsonObject(), metaOptions))
        .collect(
            Collectors.toList());

    reader.close();
  }

  public List<Table> getTables() {
    return ImmutableList.copyOf(tableList);
  }
}
