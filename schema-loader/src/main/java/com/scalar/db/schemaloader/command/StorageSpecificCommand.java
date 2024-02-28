package com.scalar.db.schemaloader.command;

import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.schemaloader.SchemaLoaderException;
import com.scalar.db.schemaloader.SchemaOperator;
import com.scalar.db.schemaloader.SchemaParser;
import com.scalar.db.schemaloader.TableSchema;
import java.io.IOException;
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

  static class Mode {

    @Option(
        names = {"-D", "--delete-all"},
        description = "Delete tables",
        defaultValue = "false")
    boolean deleteTables;

    @Option(
        names = {"--repair-all"},
        description = "Repair tables : it repairs the table metadata of existing tables",
        defaultValue = "false")
    boolean repairTables;

    @Option(
        names = {"-A", "--alter"},
        description =
            "Alter tables : it will add new columns and create/delete secondary index for existing tables. "
                + "It compares the provided table schema to the existing schema to decide which columns need to be added and which indexes need to be created or deleted",
        defaultValue = "false")
    boolean alterTables;
  }

  protected void execute(Properties props, Map<String, String> options)
      throws SchemaLoaderException {
    logger.info("Schema path: {}", schemaFile);

    // Parse the schema file
    SchemaParser parser = getSchemaParser(options);
    List<TableSchema> tableSchemaList = parser.parse();

    // Create or delete tables
    try (SchemaOperator operator = getSchemaOperator(props)) {
      boolean hasTransactionTable =
          tableSchemaList.stream().anyMatch(TableSchema::isTransactionTable);

      if (getMode() == null) {
        createTables(options, tableSchemaList, operator, hasTransactionTable);
      } else if (getMode().deleteTables) {
        deleteTables(tableSchemaList, operator, hasTransactionTable);
      } else if (getMode().repairTables) {
        repairTables(options, tableSchemaList, operator, hasTransactionTable);
      } else if (getMode().alterTables) {
        operator.alterTables(tableSchemaList, options);
      }
    }
  }

  private void createTables(
      Map<String, String> options,
      List<TableSchema> tableSchemaList,
      SchemaOperator operator,
      boolean hasTransactionTable)
      throws SchemaLoaderException {
    operator.createTables(tableSchemaList);
    if (hasTransactionTable) {
      operator.createCoordinatorTables(options);
    }
  }

  private void deleteTables(
      List<TableSchema> tableSchemaList, SchemaOperator operator, boolean hasTransactionTable)
      throws SchemaLoaderException {
    operator.deleteTables(tableSchemaList);
    if (hasTransactionTable) {
      operator.dropCoordinatorTables();
    }
  }

  private void repairTables(
      Map<String, String> options,
      List<TableSchema> tableSchemaList,
      SchemaOperator operator,
      boolean hasTransactionTable)
      throws SchemaLoaderException {
    operator.repairTables(tableSchemaList);
    if (hasTransactionTable) {
      operator.repairCoordinatorTables(options);
    }
  }

  @VisibleForTesting
  SchemaParser getSchemaParser(Map<String, String> options) throws SchemaLoaderException {
    try {
      return new SchemaParser(schemaFile, options);
    } catch (IOException e) {
      throw new SchemaLoaderException(
          "Reading the schema file failed. File: " + schemaFile.toAbsolutePath(), e);
    }
  }

  @VisibleForTesting
  SchemaOperator getSchemaOperator(Properties props) {
    return new SchemaOperator(props);
  }

  abstract Mode getMode();
}
