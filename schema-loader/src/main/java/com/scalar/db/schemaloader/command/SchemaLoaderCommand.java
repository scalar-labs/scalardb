package com.scalar.db.schemaloader.command;

import com.scalar.db.schemaloader.SchemaLoader;
import com.scalar.db.storage.cassandra.CassandraAdmin;
import com.scalar.db.storage.cassandra.CassandraAdmin.CompactionStrategy;
import com.scalar.db.storage.cassandra.CassandraAdmin.ReplicationStrategy;
import com.scalar.db.storage.dynamo.DynamoAdmin;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
    name = "java -jar scalardb-schema-loader-<version>.jar",
    description = "Create/Delete schemas in the storage defined in the config file")
public class SchemaLoaderCommand implements Callable<Integer> {
  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaLoaderCommand.class);

  @Option(
      names = "--replication-strategy",
      description =
          "The replication strategy, must be SimpleStrategy or NetworkTopologyStrategy (supported in Cassandra)")
  private ReplicationStrategy replicationStrategy;

  @Option(
      names = "--compaction-strategy",
      description = "The compaction strategy, must be LCS, STCS or TWCS (supported in Cassandra)")
  private CompactionStrategy compactionStrategy;

  @Option(
      names = "--replication-factor",
      description = "The replication factor (supported in Cassandra)")
  private String replicaFactor;

  @Option(names = "--ru", description = "Base resource unit (supported in DynamoDB, Cosmos DB)")
  private String ru;

  @Option(
      names = "--no-scaling",
      description = "Disable auto-scaling (supported in DynamoDB, Cosmos DB)")
  private Boolean noScaling;

  @Option(names = "--no-backup", description = "Disable continuous backup (supported in DynamoDB)")
  private Boolean noBackup;

  @Option(
      names = {"-c", "--config"},
      description = "Path to the config file of Scalar DB",
      required = true)
  private Path configPath;

  @Option(
      names = "--coordinator",
      description = "Create/delete coordinator table",
      defaultValue = "false")
  private boolean coordinator;

  @Option(
      names = {"-f", "--schema-file"},
      description = "Path to the schema json file")
  private Path schemaFile;

  @Option(
      names = {"-D", "--delete-all"},
      description = "Delete tables",
      defaultValue = "false")
  private boolean deleteTables;

  @Override
  public Integer call() throws Exception {
    LOGGER.info("Config path: {}", configPath);
    LOGGER.info("Schema path: {}", schemaFile);

    if (!deleteTables) {
      Map<String, String> options = new HashMap<>();
      if (replicationStrategy != null) {
        options.put(CassandraAdmin.REPLICATION_STRATEGY, replicationStrategy.toString());
      }
      if (compactionStrategy != null) {
        options.put(CassandraAdmin.COMPACTION_STRATEGY, compactionStrategy.toString());
      }
      if (replicaFactor != null) {
        options.put(CassandraAdmin.REPLICATION_FACTOR, replicaFactor);
      }
      if (ru != null) {
        options.put(DynamoAdmin.REQUEST_UNIT, ru);
      }
      if (noScaling != null) {
        options.put(DynamoAdmin.NO_SCALING, noScaling.toString());
      }
      if (noBackup != null) {
        options.put(DynamoAdmin.NO_BACKUP, noBackup.toString());
      }

      SchemaLoader.load(configPath, schemaFile, options, coordinator);
    } else {
      SchemaLoader.unload(configPath, schemaFile, coordinator);
    }
    return 0;
  }
}
