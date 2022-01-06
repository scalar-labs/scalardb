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

  @Override
  public Integer call() throws SchemaLoaderException {
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, uri);
    props.setProperty(DatabaseConfig.PASSWORD, key);
    props.setProperty(DatabaseConfig.STORAGE, "cosmos");

    // For test
    if (tableMetadataDatabasePrefix != null) {
      props.setProperty(
          CosmosConfig.TABLE_METADATA_DATABASE,
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
}
