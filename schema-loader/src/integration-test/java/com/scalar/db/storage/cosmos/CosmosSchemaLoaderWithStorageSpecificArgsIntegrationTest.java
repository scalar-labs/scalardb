package com.scalar.db.storage.cosmos;

import com.google.common.collect.ImmutableList;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.schemaloader.SchemaLoaderIntegrationTestBase;
import java.io.File;
import java.io.IOException;
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
      String configFile, String schemaFile) throws IOException {
    CosmosConfig config = new CosmosConfig(new DatabaseConfig(new File(configFile)));
    ImmutableList.Builder<String> builder =
        ImmutableList.<String>builder()
            .add("--cosmos")
            .add("-h")
            .add(config.getEndpoint())
            .add("--schema-file")
            .add(schemaFile)
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

  @Disabled
  @Override
  public void createTablesThenDeleteTables_ShouldExecuteProperly() {}
}
