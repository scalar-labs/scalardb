package com.scalar.db.schemaloader.cosmos;

import com.google.common.collect.ImmutableList;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.schemaloader.SchemaLoaderIntegrationTestBase;
import com.scalar.db.storage.cosmos.CosmosEnv;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import org.junit.Ignore;

public class CosmosSchemaLoaderWithStorageSpecificArgsIntegrationTest
    extends SchemaLoaderIntegrationTestBase {

  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return CosmosEnv.getCosmosConfig();
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
  protected List<String> getCommandArgsForCreationWithCoordinatorTable(
      String configFile, String schemaFile) throws IOException {
    DatabaseConfig config = new DatabaseConfig(new File(configFile));
    ImmutableList.Builder<String> builder =
        ImmutableList.<String>builder()
            .add("--cosmos")
            .add("-h")
            .add(config.getContactPoints().get(0))
            .add("--schema-file")
            .add(schemaFile)
            .add("-p")
            .add(config.getPassword().get());
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

  @Ignore
  @Override
  public void createTablesThenDeleteTables_ShouldExecuteProperly() {}
}
