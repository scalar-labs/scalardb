package com.scalar.db.schemaloader.command;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.schemaloader.SchemaLoaderException;
import com.scalar.db.schemaloader.SchemaOperator;
import com.scalar.db.schemaloader.SchemaParser;
import com.scalar.db.schemaloader.TableSchema;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Option;

public abstract class StorageSpecificCommand {
  private static final Logger logger = LoggerFactory.getLogger(StorageSpecificCommand.class);

  @Option(
      names = {"-f", "--schema-file"},
      description = "Path to the schema json file",
      required = true)
  private Path schemaFile;

  static class DeleteOrRepairTables {
    @Option(
        names = {"-D", "--delete-all"},
        description = "Delete tables",
        defaultValue = "false")
    boolean deleteTables;

    @SuppressFBWarnings("URF_UNREAD_FIELD")
    @Option(
        names = {"--repair-all"},
        description = "Repair tables : it repairs the table metadata of existing tables",
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

      if (getDeleteOrRepairTables() == null) {
        operator.createTables(tableSchemaList);
        if (hasTransactionalTable) {
          operator.createCoordinatorTables(options);
        }
      } else if (getDeleteOrRepairTables().deleteTables) {
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
    return new SchemaOperator(props);
  }

  abstract DeleteOrRepairTables getDeleteOrRepairTables();
}
