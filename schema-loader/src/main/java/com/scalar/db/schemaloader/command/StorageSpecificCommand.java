package com.scalar.db.schemaloader.command;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.schemaloader.SchemaLoaderException;
import com.scalar.db.schemaloader.SchemaOperator;
import com.scalar.db.schemaloader.SchemaParser;
import com.scalar.db.schemaloader.TableSchema;
import com.scalar.db.schemaloader.command.SchemaLoaderCommand.DeleteOrRepairTablesTables;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

public abstract class StorageSpecificCommand {
  private static final Logger logger = LoggerFactory.getLogger(StorageSpecificCommand.class);

  @Option(
      names = {"-f", "--schema-file"},
      description = "Path to the schema json file",
      required = true)
  private Path schemaFile;

  @ArgGroup(exclusive = true)
  DeleteOrRepairTablesTables deleteOrRepairTables;

  static class DeleteOrRepair {

    @Option(
        names = {"-D", "--delete-all"},
        description = "Delete tables",
        defaultValue = "false")
    boolean deleteTables;

    @Option(
        names = {"--repair-all"},
        description =
            "Repair tables : it repairs the table metadata of existing tables. When using Cosmos DB, it additionally repairs stored procedure attached to each table",
        defaultValue = "false")
    boolean repairTables;
  }

  protected void execute(Properties props, Map<String, String> options)
      throws SchemaLoaderException {
    logger.info("Schema path: {}", schemaFile);

    // Parse the schema file
    SchemaParser parser = getSchemaParser(options);
    List<TableSchema> tableSchemaList = parser.parse();

    // Create or delete tables
    SchemaOperator operator = getSchemaOperator(props);
    try {
      boolean hasTransactionalTable =
          tableSchemaList.stream().anyMatch(TableSchema::isTransactionalTable);

      if (deleteOrRepairTables == null) {
        operator.createTables(tableSchemaList);
        if (hasTransactionalTable) {
          operator.createCoordinatorTables(options);
        }
      } else if (deleteOrRepairTables.deleteTables) {
        operator.deleteTables(tableSchemaList);
        if (hasTransactionalTable) {
          operator.dropCoordinatorTables();
        }
      } else {
        operator.repairTables(tableSchemaList);
        if (hasTransactionalTable) {
          operator.repairCoordinatorTables(options);
        }
      }
    } finally {
      operator.close();
    }
  }

  @VisibleForTesting
  SchemaParser getSchemaParser(Map<String, String> options) throws SchemaLoaderException {
    return new SchemaParser(schemaFile, options);
  }

  @VisibleForTesting
  SchemaOperator getSchemaOperator(Properties props) {
    return new SchemaOperator(new DatabaseConfig(props));
  }
}
