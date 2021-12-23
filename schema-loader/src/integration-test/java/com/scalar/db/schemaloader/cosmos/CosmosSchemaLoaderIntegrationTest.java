package com.scalar.db.schemaloader.cosmos;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.scalar.db.schemaloader.SchemaLoaderIntegrationTestBase;
import com.scalar.db.storage.cosmos.CosmosEnv;
import java.io.BufferedWriter;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.AfterClass;

public class CosmosSchemaLoaderIntegrationTest extends SchemaLoaderIntegrationTestBase {

  @Override
  protected void initialize() throws Exception {
    config = CosmosEnv.getCosmosConfig();
    if (CosmosEnv.getDatabasePrefix().isPresent()) {
      manipulateNamespacePrefixInSchemaFile(CosmosEnv.getDatabasePrefix().get(), false);
    }
  }

  private static void manipulateNamespacePrefixInSchemaFile(String prefix, boolean removePrefix)
      throws Exception {
    JsonObject schemaJson;
    try (Reader reader =
        Files.newBufferedReader(Paths.get(SchemaLoaderIntegrationTestBase.SCHEMA_FILE))) {
      schemaJson = JsonParser.parseReader(reader).getAsJsonObject();
    }

    List<SimpleImmutableEntry<String, JsonObject>> tableMapList =
        schemaJson.entrySet().stream()
            .map(
                table -> {
                  String fullTableName = table.getKey();
                  if (fullTableName.startsWith(prefix) && removePrefix) {
                    fullTableName = fullTableName.replaceFirst(prefix, "");
                  }
                  if (!fullTableName.startsWith(prefix) && !removePrefix) {
                    fullTableName = prefix + fullTableName;
                  }
                  return new SimpleImmutableEntry<>(
                      fullTableName, table.getValue().getAsJsonObject());
                })
            .collect(Collectors.toList());
    JsonObject newSchemaJson = new JsonObject();
    tableMapList.forEach(
        table -> {
          newSchemaJson.add(table.getKey(), table.getValue());
        });
    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    try (final BufferedWriter fileWriter = Files.newBufferedWriter(Paths.get(SCHEMA_FILE))) {
      gson.toJson(newSchemaJson, fileWriter);
      fileWriter.newLine();
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (CosmosEnv.getDatabasePrefix().isPresent()) {
      manipulateNamespacePrefixInSchemaFile(CosmosEnv.getDatabasePrefix().get(), true);
    }
  }

  @Override
  protected List<String> getStorageSpecificCreationCommandArgs() {
    ImmutableList.Builder<String> listBuilder = ImmutableList.builder();
    listBuilder.addAll(
        ImmutableList.of(
            "java",
            "-jar",
            "scalardb-schema-loader.jar",
            "--cosmos",
            "-h",
            config.getContactPoints().get(0),
            "--schema-file",
            SCHEMA_FILE,
            "-p",
            config.getPassword().get()));
    if (CosmosEnv.getDatabasePrefix().isPresent()) {
      listBuilder.addAll(ImmutableList.of("--prefix", CosmosEnv.getDatabasePrefix().get()));
    }
    return listBuilder.build();
  }

  @Override
  protected List<String> getSchemaLoaderCreationCommandArgs() {
    return ImmutableList.of(
        "java",
        "-jar",
        "scalardb-schema-loader.jar",
        "--config",
        CONFIG_FILE,
        "--schema-file",
        SCHEMA_FILE,
        "--coordinator");
  }
}
