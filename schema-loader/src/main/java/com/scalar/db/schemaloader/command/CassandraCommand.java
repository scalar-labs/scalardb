package com.scalar.db.schemaloader.command;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.schemaloader.core.SchemaOperator;
import com.scalar.db.schemaloader.core.SchemaOperatorFactory;
import com.scalar.db.schemaloader.schema.SchemaParser;
import com.scalar.db.schemaloader.schema.Table;
import com.scalar.db.storage.cassandra.CassandraAdmin;
import com.scalar.db.storage.cassandra.CassandraAdmin.CompactionStrategy;
import com.scalar.db.storage.cassandra.CassandraAdmin.ReplicationStrategy;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
    name = "java -jar scalardb-schema-loader-<version>.jar --cassandra",
    description = "Create/Delete Cassandra schemas")
public class CassandraCommand implements Callable<Integer> {

  private static final Logger LOGGER = LoggerFactory.getLogger(CassandraCommand.class);

  @Option(
      names = {"-h", "--host"},
      description = "Cassandra host IP",
      required = true)
  private String hostIp;

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
      description = "Cassandra network strategy, must be SimpleStrategy or NetworkTopologyStrategy")
  private ReplicationStrategy replicationStrategy;

  @Option(
      names = {"-c", "--compaction-strategy"},
      description = "Cassandra compaction strategy, must be LCS, STCS or TWCS")
  private CompactionStrategy compactionStrategy;

  @Option(
      names = {"-R", "--replication-factor"},
      description = "Cassandra replication factor")
  private String replicationFactor;

  @Option(
      names = {"-f", "--schema-file"},
      description = "Path to the schema json file",
      required = true)
  private Path schemaFile;

  @Option(
      names = {"-D", "--delete-all"},
      description = "Delete tables",
      defaultValue = "false")
  private boolean deleteTables;

  @Override
  public Integer call() throws Exception {
    LOGGER.info("Schema path: " + schemaFile);

    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, hostIp);
    props.setProperty(DatabaseConfig.CONTACT_PORT, port);
    props.setProperty(DatabaseConfig.USERNAME, user);
    props.setProperty(DatabaseConfig.PASSWORD, password);
    props.setProperty(DatabaseConfig.STORAGE, "cassandra");

    Map<String, String> metaOptions = new HashMap<>();
    if (replicationStrategy != null) {
      metaOptions.put(CassandraAdmin.REPLICATION_STRATEGY, replicationStrategy.toString());
    }
    if (compactionStrategy != null) {
      metaOptions.put(CassandraAdmin.COMPACTION_STRATEGY, compactionStrategy.toString());
    }
    if (replicationFactor != null) {
      metaOptions.put(CassandraAdmin.REPLICATION_FACTOR, replicationFactor);
    }

    SchemaOperator operator = SchemaOperatorFactory.getSchemaOperator(props);
    List<Table> tableList = SchemaParser.parse(schemaFile.toString(), metaOptions);

    if (deleteTables) {
      operator.deleteTables(tableList);
    } else {
      operator.createTables(tableList, metaOptions);
    }

    operator.close();
    return 0;
  }
}
