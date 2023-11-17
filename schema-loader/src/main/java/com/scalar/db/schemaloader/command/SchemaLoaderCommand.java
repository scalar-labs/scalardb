package com.scalar.db.schemaloader.command;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.schemaloader.SchemaLoader;
import com.scalar.db.schemaloader.SchemaLoaderException;
import com.scalar.db.storage.cassandra.CassandraAdmin;
import com.scalar.db.storage.cassandra.CassandraAdmin.CompactionStrategy;
import com.scalar.db.storage.cassandra.CassandraAdmin.ReplicationStrategy;
import com.scalar.db.storage.dynamo.DynamoAdmin;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
    name = "scalardb-schema-loader",
    description = "Create/Delete schemas in the storage defined in the config file")
public class SchemaLoaderCommand implements Callable<Integer> {
  private static final Logger logger = LoggerFactory.getLogger(SchemaLoaderCommand.class);

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
  private String replicationFactor;

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
      description = "Path to the config file of ScalarDB",
      required = true)
  private Path configPath;

  @Option(
      names = "--coordinator",
      description = "Create/delete/repair coordinator tables",
      defaultValue = "false")
  private boolean coordinator;

  @Option(
      names = {"-f", "--schema-file"},
      description = "Path to the schema json file")
  private Path schemaFile;

  @ArgGroup private Mode mode;

  private static class Mode {

    @Option(
        names = {"-D", "--delete-all"},
        description = "Delete tables",
        defaultValue = "false")
    boolean deleteTables;

    @Option(
        names = {"--repair-all"},
        description =
            "Repair namespaces and tables that are in an unknown state: it recreates namespaces, tables, secondary indexes and their metadata if necessary.",
        defaultValue = "false")
    boolean repairAll;

    @Option(
        names = {"-A", "--alter"},
        description =
            "Alter tables : it will add new columns and create/delete secondary index for existing tables. "
                + "It compares the provided table schema to the existing schema to decide which columns need to be added and which indexes need to be created or deleted",
        defaultValue = "false")
    boolean alterTables;

    @Option(
        names = {"-I", "--import"},
        description = "Import tables : it will import existing non-ScalarDB tables to ScalarDB.",
        defaultValue = "false")
    boolean importTables;

    @Option(
        names = {"--upgrade"},
        description =
            "Upgrades the ScalarDB environment to support the latest version of the ScalarDB API. Typically, you will be requested, as indicated on the release notes, to run this command after"
                + " updating the ScalarDB version of your application environment.",
        defaultValue = "false")
    boolean upgrade;
  }

  @Override
  public Integer call() throws Exception {
    logger.info("Config path: {}", configPath);
    logger.info("Schema path: {}", schemaFile);

    if (mode == null) {
      createTables();
    } else if (mode.deleteTables) {
      SchemaLoader.unload(configPath, schemaFile, coordinator);
    } else if (mode.repairAll) {
      repairAll();
    } else if (mode.alterTables) {
      alterTables();
    } else if (mode.importTables) {
      importTables();
    } else if (mode.upgrade) {
      upgrade();
    }
    return 0;
  }

  private void createTables() throws SchemaLoaderException {
    Map<String, String> options = prepareAllOptions();
    SchemaLoader.load(configPath, schemaFile, options, coordinator);
  }

  private void repairAll() throws SchemaLoaderException {
    if (schemaFile == null) {
      throw new IllegalArgumentException(
          "Specifying the '--schema-file' option is required when using the '--repair-all' option");
    }
    Map<String, String> options = prepareAllOptions();
    SchemaLoader.repairAll(configPath, schemaFile, options, coordinator);
  }

  private void alterTables() throws SchemaLoaderException {
    if (schemaFile == null) {
      throw new IllegalArgumentException(
          "Specifying the '--schema-file' option is required when using the '--alter' option");
    }
    Map<String, String> options = prepareOptions(DynamoAdmin.NO_SCALING);
    SchemaLoader.alterTables(configPath, schemaFile, options);
  }

  private void importTables() throws SchemaLoaderException {
    if (schemaFile == null) {
      throw new IllegalArgumentException(
          "Specifying the '--schema-file' option is required when using the '--import' option");
    }

    if (coordinator) {
      throw new IllegalArgumentException(
          "Specifying the '--coordinator' option with the '--import' option is not allowed."
              + " Create coordinator tables separately");
    }
    Map<String, String> options = prepareAllOptions();
    SchemaLoader.importTables(configPath, schemaFile, options);
  }

  private void upgrade() throws SchemaLoaderException {
    Map<String, String> options = prepareAllOptions();
    SchemaLoader.upgrade(configPath, options);
  }

  private Map<String, String> prepareAllOptions() {
    return prepareOptions(
        CassandraAdmin.REPLICATION_STRATEGY,
        CassandraAdmin.COMPACTION_STRATEGY,
        CassandraAdmin.REPLICATION_FACTOR,
        DynamoAdmin.REQUEST_UNIT,
        DynamoAdmin.NO_SCALING,
        DynamoAdmin.NO_BACKUP);
  }

  private Map<String, String> prepareOptions(String... options) {
    ImmutableMap.Builder<String, String> optionToValue = ImmutableMap.builder();
    for (String option : options) {
      switch (option) {
        case CassandraAdmin.REPLICATION_STRATEGY:
          if (replicationStrategy != null) {
            optionToValue.put(CassandraAdmin.REPLICATION_STRATEGY, replicationStrategy.toString());
          }
          break;
        case CassandraAdmin.COMPACTION_STRATEGY:
          if (compactionStrategy != null) {
            optionToValue.put(CassandraAdmin.COMPACTION_STRATEGY, compactionStrategy.toString());
          }
          break;
        case CassandraAdmin.REPLICATION_FACTOR:
          if (replicationFactor != null) {
            optionToValue.put(CassandraAdmin.REPLICATION_FACTOR, replicationFactor);
          }
          break;
        case DynamoAdmin.REQUEST_UNIT:
          if (ru != null) {
            optionToValue.put(DynamoAdmin.REQUEST_UNIT, ru);
          }
          break;
        case DynamoAdmin.NO_SCALING:
          if (noScaling != null) {
            optionToValue.put(DynamoAdmin.NO_SCALING, noScaling.toString());
          }
          break;
        case DynamoAdmin.NO_BACKUP:
          if (noBackup != null) {
            optionToValue.put(DynamoAdmin.NO_BACKUP, noBackup.toString());
          }
          break;
        default:
          throw new AssertionError("Unknown option " + option);
      }
    }
    return optionToValue.build();
  }
}
