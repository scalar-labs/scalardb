package com.scalar.db.storage.jdbc;

import com.google.common.collect.ImmutableList;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.schemaloader.SchemaLoaderIntegrationTestBase;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Disabled;

public class JdbcSchemaLoaderWithStorageSpecificArgsIntegrationTest
    extends SchemaLoaderIntegrationTestBase {

  @Override
  protected Properties getProperties() {
    return JdbcEnv.getProperties();
  }

  @Override
  protected List<String> getCommandArgsForCreationWithCoordinator(
      Path configFilePath, Path schemaFilePath) throws IOException {
    JdbcConfig config = new JdbcConfig(new DatabaseConfig(configFilePath));
    return ImmutableList.of(
        "--jdbc",
        "-j",
        config.getJdbcUrl(),
        "--schema-file",
        schemaFilePath.toString(),
        "-u",
        config.getUsername().get(),
        "-p",
        config.getPassword().get());
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
