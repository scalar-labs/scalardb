package com.scalar.db.schemaloader.command;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.schemaloader.SchemaLoaderException;
import com.scalar.db.schemaloader.SchemaOperator;
import com.scalar.db.schemaloader.SchemaParser;
import com.scalar.db.schemaloader.TableSchema;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Option;

public abstract class StorageSpecificCommand {
  private static final Logger LOGGER = LoggerFactory.getLogger(StorageSpecificCommand.class);

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

  protected void execute(Properties props, Map<String, String> options)
      throws SchemaLoaderException {
    LOGGER.info("Schema path: {}", schemaFile);

    // Parse the schema file
    SchemaParser parser = getSchemaParser(options);
    List<TableSchema> tableSchemaList = parser.parse();

    // Create or delete tables
    SchemaOperator operator = getSchemaOperator(props);
    try {
      boolean hasTransactionalTable =
          tableSchemaList.stream().anyMatch(TableSchema::isTransactionalTable);

      if (!deleteTables) {
        operator.createTables(tableSchemaList);
        if (hasTransactionalTable) {
          operator.createCoordinatorTable(options);
        }
      } else {
        operator.deleteTables(tableSchemaList);
        if (hasTransactionalTable) {
          operator.dropCoordinatorTable();
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
