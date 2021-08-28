package com.scalar.db.schemaloader.command;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.schemaloader.core.SchemaOperator;
import com.scalar.db.schemaloader.schema.SchemaParser;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "--cassandra", description = "Using Cassandra DB")
public class CassandraCommand implements Callable<Integer> {

  private static final Logger LOGGER = LoggerFactory.getLogger(CassandraCommand.class);

  @Option(
      names = {"-h", "--host"},
      description = "Cassandra host IP",
      required = true)
  String hostIP;

  @Option(
      names = {"-P", "--port"},
      description = "Cassandra Port",
      defaultValue = "9042")
  String port;

  @Option(
      names = {"-u", "--user"},
      description = "Cassandra user",
      defaultValue = "cassandra")
  String user;

  @Option(
      names = {"-p", "--password"},
      description = "Cassandra password",
      defaultValue = "cassandra")
  String password;

  @Option(
      names = {"-n", "--network-strategy"},
      description =
          "Cassandra network strategy, should be SimpleStrategy or NetworkTopologyStrategy")
  ReplicationStrategy replicationStrategy;

  @Option(
      names = {"-c", "--compaction-strategy"},
      description = "Cassandra compaction strategy, should be LCS, STCS or TWCS")
  CompactStrategy compactStrategy;

  @Option(
      names = {"-R", "--replication-factor"},
      description = "Cassandra replication factor")
  String replicaFactor;

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
    LOGGER.info("Schema path: " + schemaFile);

    Properties props = new Properties();
    props.setProperty("scalar.db.contact_points", hostIP);
    props.setProperty("scalar.db.contact_port", port);
    props.setProperty("scalar.db.username", user);
    props.setProperty("scalar.db.password", password);
    props.setProperty("scalar.db.storage", "cassandra");

    Map<String, String> metaOptions = new HashMap<>();
    if (replicationStrategy != null) {
      metaOptions.put("network-strategy", replicationStrategy.name());
    }
    if (compactStrategy != null) {
      metaOptions.put("compaction-strategy", compactStrategy.name());
    }
    if (replicaFactor != null) {
      metaOptions.put("replication-factor", replicaFactor);
    }

    DatabaseConfig dbConfig = new DatabaseConfig(props);
    SchemaOperator operator = new SchemaOperator(dbConfig);
    SchemaParser schemaParser = new SchemaParser(schemaFile.toString(), metaOptions);

    if (deleteTables) {
      operator.deleteTables(schemaParser.getTables());
    } else {
      operator.createTables(schemaParser.getTables());
    }
    return 0;
  }

  enum CompactStrategy {
    STCS,
    LCS,
    TWCS
  }

  enum ReplicationStrategy {
    SimpleStrategy,
    NetworkTopologyStrategy
  }
}
