package com.scalar.db.storage.cosmos;

import com.google.common.collect.ImmutableList;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.schemaloader.SchemaLoaderIntegrationTestBase;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;

public class CosmosSchemaLoaderWithStorageSpecificArgsIntegrationTest
    extends SchemaLoaderIntegrationTestBase {

  @Override
  protected Properties getProperties() {
    return CosmosEnv.getProperties();
  }

  @Override
  protected String getNamespace1() {
    return getNamespace(super.getNamespace1());
  }

  @Override
  protected String getNamespace2() {
    return getNamespace(super.getNamespace2());
  }

  private String getNamespace(String namespace) {
    Optional<String> databasePrefix = CosmosEnv.getDatabasePrefix();
    return databasePrefix.map(prefix -> prefix + namespace).orElse(namespace);
  }

  @Override
  protected List<String> getCommandArgsForCreationWithCoordinator(
      Path configFilePath, Path schemaFilePath) throws IOException {
    CosmosConfig config = new CosmosConfig(new DatabaseConfig(configFilePath));
    ImmutableList.Builder<String> builder =
        ImmutableList.<String>builder()
            .add("--cosmos")
            .add("-h")
            .add(config.getEndpoint())
            .add("--schema-file")
            .add(schemaFilePath.toString())
            .add("-p")
            .add(config.getKey());
    CosmosEnv.getDatabasePrefix()
        .ifPresent(
            (prefix) ->
                builder
                    .add("--table-metadata-database-prefix")
                    .add(prefix)
                    .add("--coordinator-namespace-prefix")
                    .add(prefix));
    return builder.build();
  }

  @Override
  protected List<String> getCommandArgsForTableReparationWithCoordinator(
      Path configFilePath, Path schemaFilePath) throws Exception {
    return ImmutableList.<String>builder()
        .addAll(getCommandArgsForCreationWithCoordinator(configFilePath, schemaFilePath))
        .add("--repair-all")
        .build();
  }

  @Override
  protected List<String> getCommandArgsForAlteration(Path configFilePath, Path schemaFilePath)
      throws Exception {
    return ImmutableList.<String>builder()
        .addAll(getCommandArgsForCreationWithCoordinator(configFilePath, schemaFilePath))
        .add("--alter")
        .build();
  }

  @Disabled
  @Override
  public void createTablesThenDeleteTables_ShouldExecuteProperly() {}

  @Disabled
  @Override
  public void createTableThenDropMetadataTableThenRepairTables_ShouldExecuteProperly() {}
}
