package com.scalar.db.schemaloader.command;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.schemaloader.core.SchemaOperator;
import com.scalar.db.schemaloader.schema.SchemaParser;
import com.scalar.db.storage.cassandra.CassandraAdmin;
import com.scalar.db.storage.cassandra.CassandraAdmin.CompactionStrategy;
import com.scalar.db.storage.cassandra.CassandraAdmin.ReplicationStrategy;
import com.scalar.db.storage.dynamo.DynamoAdmin;
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
      names = {"-n", "--replication-strategy"},
      description =
          "Cassandra network strategy, should be SimpleStrategy or NetworkTopologyStrategy")
  private ReplicationStrategy replicationStrategy;

  @Option(
      names = {"-c", "--compaction-strategy"},
      description = "Cassandra compaction strategy, should be LCS, STCS or TWCS")
  private CompactionStrategy compactionStrategy;

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
  private Boolean noBackup;

  @Option(names = "--coordinator", description = "Create coordinator table", defaultValue = "false")
  private Boolean coordinator;

  @Option(
      names = {"-f", "--schema-file"},
      description = "Path to schema json file")
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
      metaOptions.put(CassandraAdmin.REPLICATION_STRATEGY, replicationStrategy.name());
    }
    if (compactionStrategy != null) {
      metaOptions.put(CassandraAdmin.COMPACTION_STRATEGY, compactionStrategy.name());
    }
    if (replicaFactor != null) {
      metaOptions.put(CassandraAdmin.REPLICATION_FACTOR, replicaFactor);
    }
    if (ru != null) {
      metaOptions.put(DynamoAdmin.REQUEST_UNIT, ru);
    }
    if (noScaling != null) {
      metaOptions.put(DynamoAdmin.NO_SCALING, noScaling.toString());
    }
    if (noBackup != null) {
      metaOptions.put(DynamoAdmin.NO_BACKUP, noBackup.toString());
    }

    DatabaseConfig dbConfig = new DatabaseConfig(new FileInputStream(configPath.toString()));
    SchemaOperator operator = new SchemaOperator(dbConfig);

    if (coordinator) {
      operator.createCoordinatorTable();
    }

    if (schemaFile != null) {
      SchemaParser schemaParser = new SchemaParser(schemaFile.toString(), metaOptions);
      if (deleteTables) {
        operator.deleteTables(schemaParser.getTables());
      } else {
        operator.createTables(schemaParser.getTables());
      }
    }

    return 0;
  }
}
