package command;

import com.scalar.db.config.DatabaseConfig;
import command.CassandraCommand.CassandraCompactStrategy;
import command.CassandraCommand.CassandraNetStrategy;
import core.SchemaOperator;
import java.io.FileInputStream;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.logging.Logger;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import utils.SchemaParser;

@Command(name = "--config", description = "Using config file for Scalar DB")
public class ConfigFileBasedCommand implements Callable<Integer> {

  @Parameters(index = "0", description = "Path to config file of Scalar DB")
  Path configPath;

  @Option(
      names = {"-n", "--network-strategy"},
      description =
          "Cassandra network strategy, should be SimpleStrategy or NetworkTopologyStrategy")
  CassandraNetStrategy cassandraNetStrategy;

  @Option(names = {"-c",
      "--compaction-strategy"}, description = "Cassandra compaction strategy, should be LCS, STCS or TWCS")
  CassandraCompactStrategy cassandraCompactStrategy;

  @Option(names = {"-R", "--replication-factor"}, description = "Cassandra replication factor")
  String cassandraReplicaFactor;

  @Option(names = {"-r",
      "--ru"}, description = "Base resource unit (supported in Dynamo DB, Cosmos DB)")
  String ru;

  @Option(names = "--no-scaling", description = "Disable auto-scaling (supported in Dynamo DB, Cosmos DB)")
  Boolean noScaling;

  @Option(names = "--no-backup", description = "Disable continuous backup for Dynamo DB")
  Boolean dynamoNoBackup;

  @Option(
      names = {"-f", "--schema-file"},
      description = "Path to schema json file",
      required = true)
  Path schemaFile;

  @Option(
      names = {"-D", "--delete-all"},
      description = "Delete tables",
      defaultValue = "false")
  Boolean deleteTables;

  @Override
  public Integer call() throws Exception {

    Logger.getGlobal().info("Config path: " + configPath);
    Logger.getGlobal().info("Schema path: " + schemaFile);

    Map<String, String> metaOptions = new HashMap<String, String>();
    if (cassandraNetStrategy != null) {
      metaOptions.put("network-strategy", cassandraNetStrategy.name());
    }
    if (cassandraCompactStrategy != null) {
      metaOptions.put("compaction-strategy", cassandraCompactStrategy.name());
    }
    if (cassandraReplicaFactor != null) {
      metaOptions.put("replication-factor", cassandraReplicaFactor);
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
    SchemaParser schemaMap = new SchemaParser(schemaFile.toString(), metaOptions);

    if (deleteTables) {
      operator.deleteTables(schemaMap.getTables());
    } else {
      operator.createTables(schemaMap.hasTransactionTable(), schemaMap.getTables());
    }
    return 0;
  }
}
