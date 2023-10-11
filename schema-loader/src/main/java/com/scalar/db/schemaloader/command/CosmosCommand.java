package com.scalar.db.schemaloader.command;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.schemaloader.SchemaLoaderException;
import com.scalar.db.storage.cosmos.CosmosAdmin;
import com.scalar.db.storage.cosmos.CosmosConfig;
import com.scalar.db.transaction.consensuscommit.ConsensusCommitConfig;
import com.scalar.db.transaction.consensuscommit.Coordinator;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
    name = "java -jar scalardb-schema-loader-<version>.jar --cosmos",
    description = "Create/Delete Cosmos DB schemas")
public class CosmosCommand extends StorageSpecificCommand implements Callable<Integer> {

  @Option(
      names = {"-h", "--host"},
      description = "Cosmos DB account URI",
      required = true)
  private String uri;

  // For backward compatibility
  @SuppressWarnings("UnusedVariable")
  @Option(
      names = {"-u", "--user"},
      description = "DB username",
      hidden = true)
  private String username;

  @Option(
      names = {"-p", "--password"},
      description = "Cosmos DB key",
      required = true)
  private String key;

  @Option(
      names = {"-r", "--ru"},
      description = "Base resource unit")
  private String ru;

  @Option(names = "--no-scaling", description = "Disable auto-scaling for Cosmos DB")
  private Boolean noScaling;

  // For test
  @Option(
      names = {"--table-metadata-database-prefix"},
      description = "Table metadata database prefix",
      hidden = true)
  private String tableMetadataDatabasePrefix;

  // For test
  @Option(
      names = {"--coordinator-namespace-prefix"},
      description = "Coordinator namespace prefix",
      hidden = true)
  private String coordinatorNamespacePrefix;

  @ArgGroup private CosmosMode cosmosMode;

  /**
   * To be able to have a "--repair-all" option description that is different only for the
   * CosmosCommand, a cosmos specific version of {@link Mode} had to be created. Eventually, the
   * only difference with {@link Mode} is the {@link CosmosMode#repairTables} description value
   */
  private static class CosmosMode {

    @Option(
        names = {"-D", "--delete-all"},
        description = "Delete tables",
        defaultValue = "false")
    boolean deleteTables;

    @Option(
        names = {"--repair-all"},
        description =
            "Repair tables : it repairs the table metadata of existing tables and repairs stored procedure attached to each table",
        defaultValue = "false")
    boolean repairTables;

    @Option(
        names = {"-A", "--alter"},
        description =
            "Alter tables : it will add new columns and create/delete secondary index for existing tables. "
                + "It compares the provided table schema to the existing schema to decide which columns need to be added and which indexes need to be created or deleted",
        defaultValue = "false")
    boolean alterTables;
  }

  @Override
  public Integer call() throws SchemaLoaderException {
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, uri);
    props.setProperty(DatabaseConfig.PASSWORD, key);
    props.setProperty(DatabaseConfig.STORAGE, "cosmos");

    // For test
    if (tableMetadataDatabasePrefix != null) {
      props.setProperty(
          CosmosConfig.METADATA_DATABASE,
          tableMetadataDatabasePrefix + CosmosAdmin.METADATA_DATABASE);
    }
    if (coordinatorNamespacePrefix != null) {
      props.setProperty(
          ConsensusCommitConfig.COORDINATOR_NAMESPACE,
          coordinatorNamespacePrefix + Coordinator.NAMESPACE);
    }

    Map<String, String> options = new HashMap<>();
    if (ru != null) {
      options.put(CosmosAdmin.REQUEST_UNIT, ru);
    }
    if (noScaling != null) {
      options.put(CosmosAdmin.NO_SCALING, noScaling.toString());
    }

    execute(props, options);
    return 0;
  }

  @Override
  Mode getMode() {
    if (cosmosMode == null) {
      return null;
    }

    Mode mode = new Mode();
    if (cosmosMode.deleteTables) {
      mode.deleteTables = true;
    } else if (cosmosMode.repairTables) {
      mode.repairTables = true;
    } else if (cosmosMode.alterTables) {
      mode.alterTables = true;
    }
    return mode;
  }
}
