package command;

import com.scalar.db.config.DatabaseConfig;
import core.SchemaOperator;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.logging.Logger;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import utils.SchemaParser;

@Command(name = "--cassandra", description = "Using Cassandra DB")
public class CassandraCommand implements Callable<Integer> {

  @Option(names = {"-h", "--host"}, description = "Cassandra host IP", required = true)
  String cassandraIP;

  @Option(names = {"-P", "--port"}, description = "Cassandra Port", defaultValue = "9042")
  String cassandraPort;

  @Option(names = {"-u", "--user"}, description = "Cassandra user", defaultValue = "cassandra")
  String cassandraUser;

  @Option(names = {"-p",
      "--password"}, description = "Cassandra password", defaultValue = "cassandra")
  String cassandraPw;

  @Option(
      names = {"-n", "--network-strategy"},
      description =
          "Cassandra network strategy, should be SimpleStrategy or NetworkTopologyStrategy")
  String cassandraNetStrategy;

  @Option(names = {"-c",
      "--compaction-strategy"}, description = "Cassandra compaction strategy, should be LCS, STCS or TWCS")
  CassandraCompactStrategy cassandraCompactStrategy;

  @Option(names = {"-R", "--replication-factor"}, description = "Cassandra replication factor")
  String cassandraReplicaFactor;

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

    Logger.getGlobal().info("Schema path: " + schemaFile);

    Properties props = new Properties();
    props.setProperty("scalar.db.contact_points", cassandraIP);
    props.setProperty("scalar.db.contact_port", cassandraPort);
    props.setProperty("scalar.db.username", cassandraUser);
    props.setProperty("scalar.db.password", cassandraPw);
    props.setProperty("scalar.db.storage", "cassandra");

    Map<String, String> metaOptions = new HashMap<String, String>();
    if (cassandraNetStrategy != null) {
      metaOptions.put("network-strategy", cassandraNetStrategy);
    }
    if (cassandraCompactStrategy != null) {
      metaOptions.put("compaction-strategy", cassandraCompactStrategy.name());
    }
    if (cassandraReplicaFactor != null) {
      metaOptions.put("replication-factor", cassandraReplicaFactor);
    }

    DatabaseConfig dbConfig = new DatabaseConfig(props);
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

enum CassandraCompactStrategy {
  STCS,
  LCS,
  TWCS
}