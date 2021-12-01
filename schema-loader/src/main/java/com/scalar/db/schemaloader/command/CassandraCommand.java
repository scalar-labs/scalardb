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
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
    name = "java -jar scalardb-schema-loader-<version>.jar --cassandra",
    description = "Create/Delete Cassandra schemas")
public class CassandraCommand extends StorageSpecificCommandBase implements Callable<Integer> {

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

  @Override
  public Integer call() throws SchemaLoaderException {

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

    execute(props, metaOptions);

    return 0;
  }
}
