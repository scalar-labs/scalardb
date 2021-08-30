package com.scalar.db.schemaloader.command;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.schemaloader.command.CassandraCommand.CompactStrategy;
import com.scalar.db.schemaloader.command.CassandraCommand.ReplicationStrategy;
import com.scalar.db.schemaloader.core.SchemaOperator;
import com.scalar.db.schemaloader.schema.SchemaParser;
import com.scalar.db.storage.cassandra.CassandraAdmin;
import java.io.FileInputStream;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "--config", description = "Using config file for Scalar DB")
public class ConfigFileBasedCommand implements Callable<Integer> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigFileBasedCommand.class);

  @Parameters(index = "0", description = "Path to config file of Scalar DB")
  private Path configPath;

  @Option(
      names = {"-n", "--network-strategy"},
      description =
          "Cassandra network strategy, should be SimpleStrategy or NetworkTopologyStrategy")
  private ReplicationStrategy replicationStrategy;

  @Option(
      names = {"-c", "--compaction-strategy"},
      description = "Cassandra compaction strategy, should be LCS, STCS or TWCS")
  private CompactStrategy compactStrategy;

  @Option(
      names = {"-R", "--replication-factor"},
      description = "Cassandra replication factor")
  private String replicaFactor;

  @Option(
      names = {"-r", "--ru"},
      description = "Base resource unit (supported in Dynamo DB, Cosmos DB)")
  private String ru;

  @Option(
      names = "--no-scaling",
      description = "Disable auto-scaling (supported in Dynamo DB, Cosmos DB)")
  private Boolean noScaling;

  @Option(names = "--no-backup", description = "Disable continuous backup for Dynamo DB")
  private Boolean dynamoNoBackup;

  @Option(
      names = {"-f", "--schema-file"},
      description = "Path to schema json file",
      required = true)
  private Path schemaFile;

  @Option(
      names = {"-D", "--delete-all"},
      description = "Delete tables",
      defaultValue = "false")
  private Boolean deleteTables;

  @Override
  public Integer call() throws Exception {

    LOGGER.info("Config path: " + configPath);
    LOGGER.info("Schema path: " + schemaFile);

    Map<String, String> metaOptions = new HashMap<>();
    if (replicationStrategy != null) {
      metaOptions.put(CassandraAdmin.NETWORK_STRATEGY, replicationStrategy.name());
    }
    if (compactStrategy != null) {
      metaOptions.put(CassandraAdmin.COMPACTION_STRATEGY, compactStrategy.name());
    }
    if (replicaFactor != null) {
      metaOptions.put(CassandraAdmin.REPLICATION_FACTOR, replicaFactor);
    }
    if (ru != null) {
      metaOptions.put("ru", ru);
    }
    if (noScaling != null) {
      metaOptions.put("no-scaling", noScaling.toString());
    }
    if (dynamoNoBackup != null) {
      metaOptions.put("no-backup", dynamoNoBackup.toString());
    }

    DatabaseConfig dbConfig = new DatabaseConfig(new FileInputStream(configPath.toString()));
    SchemaOperator operator = new SchemaOperator(dbConfig);
    SchemaParser schemaParser = new SchemaParser(schemaFile.toString(), metaOptions);

    if (deleteTables) {
      operator.deleteTables(schemaParser.getTables());
    } else {
      operator.createTables(schemaParser.getTables());
    }
    return 0;
  }
}
