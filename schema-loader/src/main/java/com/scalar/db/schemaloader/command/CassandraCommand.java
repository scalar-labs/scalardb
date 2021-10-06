package com.scalar.db.schemaloader.command;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.schemaloader.core.SchemaOperator;
import com.scalar.db.schemaloader.schema.SchemaParser;
import com.scalar.db.storage.cassandra.CassandraAdmin;
import com.scalar.db.storage.cassandra.CassandraAdmin.CompactionStrategy;
import com.scalar.db.storage.cassandra.CassandraAdmin.ReplicationStrategy;
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
  private String hostIP;

  @Option(
      names = {"-P", "--port"},
      description = "Cassandra Port",
      defaultValue = "9042")
  private String port;

  @Option(
      names = {"-u", "--user"},
      description = "Cassandra user",
      defaultValue = "cassandra")
  private String user;

  @Option(
      names = {"-p", "--password"},
      description = "Cassandra password",
      defaultValue = "cassandra")
  private String password;

  @Option(
      names = {"-n", "--network-strategy"},
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
    LOGGER.info("Schema path: " + schemaFile);

    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, hostIP);
    props.setProperty(DatabaseConfig.CONTACT_PORT, port);
    props.setProperty(DatabaseConfig.USERNAME, user);
    props.setProperty(DatabaseConfig.PASSWORD, password);
    props.setProperty(DatabaseConfig.STORAGE, "cassandra");

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

    DatabaseConfig dbConfig = new DatabaseConfig(props);
    SchemaOperator operator = new SchemaOperator(dbConfig);
    SchemaParser schemaParser = new SchemaParser(schemaFile.toString(), metaOptions);

    if (deleteTables) {
      operator.deleteTables(schemaParser.getTables());
    } else {
      operator.createTables(schemaParser.getTables());
    }

    operator.close();
    return 0;
  }
}
