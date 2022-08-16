package com.scalar.db.schemaloader.command;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.schemaloader.SchemaLoaderException;
import com.scalar.db.storage.cassandra.CassandraAdmin;
import com.scalar.db.storage.cassandra.CassandraAdmin.CompactionStrategy;
import com.scalar.db.storage.cassandra.CassandraAdmin.ReplicationStrategy;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
    name = "java -jar scalardb-schema-loader-<version>.jar --cassandra",
    description = "Create/Delete Cassandra schemas")
public class CassandraCommand extends StorageSpecificCommand implements Callable<Integer> {

  @Option(
      names = {"-h", "--host"},
      description = "Cassandra host IP",
      required = true)
  private String hostIp;

  @Option(
      names = {"-P", "--port"},
      description = "Cassandra Port")
  private String port;

  @Option(
      names = {"-u", "--user"},
      description = "Cassandra user",
      required = true)
  private String user;

  @Option(
      names = {"-p", "--password"},
      description = "Cassandra password",
      required = true)
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

  @ArgGroup private Mode mode;

  @Override
  public Integer call() throws SchemaLoaderException {
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, hostIp);
    if (port != null) {
      props.setProperty(DatabaseConfig.CONTACT_PORT, port);
    }
    props.setProperty(DatabaseConfig.USERNAME, user);
    props.setProperty(DatabaseConfig.PASSWORD, password);
    props.setProperty(DatabaseConfig.STORAGE, "cassandra");

    Map<String, String> options = new HashMap<>();
    if (replicationStrategy != null) {
      options.put(CassandraAdmin.REPLICATION_STRATEGY, replicationStrategy.toString());
    }
    if (compactionStrategy != null) {
      options.put(CassandraAdmin.COMPACTION_STRATEGY, compactionStrategy.toString());
    }
    if (replicationFactor != null) {
      options.put(CassandraAdmin.REPLICATION_FACTOR, replicationFactor);
    }

    execute(props, options);
    return 0;
  }

  @Override
  Mode getMode() {
    return mode;
  }
}
