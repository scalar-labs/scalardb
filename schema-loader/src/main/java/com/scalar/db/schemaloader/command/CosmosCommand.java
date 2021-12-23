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
public class CosmosCommand extends StorageSpecificCommandBase implements Callable<Integer> {

  @Option(
      names = {"-h", "--host"},
      description = "Cosmos DB account URI",
      required = true)
  private String uri;

  // For backward compatibility
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
      description = "Base resource unit",
      defaultValue = "400")
  private String ru;

  @Option(names = "--no-scaling", description = "Disable auto-scaling for Cosmos DB")
  private Boolean noScaling;

  @Option(
      names = {"--prefix"},
      description = "Namespace prefix",
      hidden = true)
  private String prefix;

  @Override
  public Integer call() throws SchemaLoaderException {

    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, uri);
    props.setProperty(DatabaseConfig.PASSWORD, key);
    props.setProperty(DatabaseConfig.STORAGE, "cosmos");

    Map<String, String> metaOptions = new HashMap<>();
    if (prefix != null) {
      props.setProperty(
          CosmosConfig.TABLE_METADATA_DATABASE, prefix + CosmosAdmin.METADATA_DATABASE);
      props.setProperty(
          ConsensusCommitConfig.COORDINATOR_NAMESPACE, prefix + Coordinator.NAMESPACE);
    }
    if (ru != null) {
      metaOptions.put(CosmosAdmin.REQUEST_UNIT, ru);
    }
    if (noScaling != null) {
      metaOptions.put(CosmosAdmin.NO_SCALING, noScaling.toString());
    }

    execute(props, metaOptions);

    return 0;
  }
}
