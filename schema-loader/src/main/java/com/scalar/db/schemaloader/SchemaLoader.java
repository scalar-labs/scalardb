package com.scalar.db.schemaloader;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.scalar.db.schemaloader.command.CassandraCommand;
import com.scalar.db.schemaloader.command.CosmosCommand;
import com.scalar.db.schemaloader.command.DynamoCommand;
import com.scalar.db.schemaloader.command.JdbcCommand;
import com.scalar.db.schemaloader.command.SchemaLoaderCommand;
import com.scalar.db.schemaloader.core.SchemaOperator;
import com.scalar.db.schemaloader.core.SchemaOperatorException;
import com.scalar.db.schemaloader.core.SchemaOperatorFactory;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

public class SchemaLoader {
  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaLoader.class);
  private static final Object SCHEMA_LOADER_COMMAND = new SchemaLoaderCommand();
  private static final ImmutableMap<String, Object> COMMAND_MAP =
      ImmutableMap.<String, Object>builder()
          .put("--config", SCHEMA_LOADER_COMMAND)
          .put("-c", SCHEMA_LOADER_COMMAND)
          .put("--cassandra", new CassandraCommand())
          .put("--cosmos", new CosmosCommand())
          .put("--dynamo", new DynamoCommand())
          .put("--jdbc", new JdbcCommand())
          .build();
  private static final ImmutableList<String> STORAGE_SPECIFIC_OPTION_LIST =
      ImmutableList.<String>builder()
          .add("--cassandra")
          .add("--cosmos")
          .add("--dynamo")
          .add("--jdbc")
          .build();

  public static void main(String... args) {
    Object command = null;
    String[] commandArgs = args;
    for (String arg : args) {
      if (COMMAND_MAP.containsKey(arg)) {
        LOGGER.warn(
            "Storage-specific options (--cassandra, --cosmos, --dynamo, --jdbc) "
                + "are deprecated and will be removed in the future. Please use "
                + "the --config option along with your config file instead.");
        command = COMMAND_MAP.get(arg);
        if (STORAGE_SPECIFIC_OPTION_LIST.contains(arg)) {
          // Remove the storage specific option from args
          commandArgs = Arrays.stream(args).filter(a -> !a.equals(arg)).toArray(String[]::new);
        }
        break;
      }
    }

    int status;
    if (command != null) {
      status = new CommandLine(command).execute(commandArgs);
    } else {
      LOGGER.error(
          "Need to specify either --config <configPath> or --cassandra or --cosmos or --dynamo or --jdbc");
      status = 1;
    }
    System.exit(status);
  }

  /**
   * Creates tables defined in the schema file.
   *
   * @param configFilePath path to Scalar DB config file.
   * @param schemaFilePath path to schema file.
   * @param options specific options for creating tables.
   * @param createCoordinatorTable create coordinator table or not.\
   */
  public static void load(
      String configFilePath,
      String schemaFilePath,
      Map<String, String> options,
      boolean createCoordinatorTable)
      throws IOException, SchemaOperatorException {
    load(
        configFilePath, schemaFilePath, options, createCoordinatorTable, InputSchemaType.FILE_PATH);
  }

  /**
   * Creates tables defined in the schema.
   *
   * @param configFilePath path to Scalar DB config file.
   * @param schema schema definition, can be path to schema file or serialized json string schema.
   * @param options specific options for creating tables.
   * @param createCoordinatorTable create coordinator table or not.
   * @param inputSchemaType type of input schema {@link InputSchemaType}
   */
  public static void load(
      String configFilePath,
      String schema,
      Map<String, String> options,
      boolean createCoordinatorTable,
      InputSchemaType inputSchemaType)
      throws IOException, SchemaOperatorException {
    SchemaOperator operator = SchemaOperatorFactory.getSchemaOperator(configFilePath, true);

    // Create tables
    if (inputSchemaType == InputSchemaType.FILE_PATH) {
      operator.createTables(Paths.get(schema), options);
    } else {
      operator.createTables(schema, options);
    }

    if (createCoordinatorTable) {
      operator.createCoordinatorTable(options);
    }

    operator.close();
  }

  /**
   * Delete tables defined in the schema file.
   *
   * @param configFilePath path to Scalar DB config file.
   * @param schemaFilePath path to schema json file.
   * @param options specific options for deleting tables.
   * @param deleteCoordinatorTable delete coordinator table or not.
   */
  public static void unload(
      String configFilePath,
      String schemaFilePath,
      Map<String, String> options,
      boolean deleteCoordinatorTable)
      throws IOException, SchemaOperatorException {
    unload(
        configFilePath, schemaFilePath, options, deleteCoordinatorTable, InputSchemaType.FILE_PATH);
  }

  /**
   * Delete tables defined in the schema.
   *
   * @param configFilePath path to Scalar DB config file.
   * @param schema schema definition, can be path to schema file or serialized json string schema.
   * @param options specific options for deleting tables.
   * @param deleteCoordinatorTable delete coordinator table or not.
   * @param inputSchemaType type of input schema {@link InputSchemaType}
   */
  public static void unload(
      String configFilePath,
      String schema,
      Map<String, String> options,
      boolean deleteCoordinatorTable,
      InputSchemaType inputSchemaType)
      throws IOException, SchemaOperatorException {
    SchemaOperator operator = SchemaOperatorFactory.getSchemaOperator(configFilePath, true);

    // Delete tables
    if (inputSchemaType == InputSchemaType.FILE_PATH) {
      operator.deleteTables(Paths.get(schema), options);
    } else {
      operator.deleteTables(schema, options);
    }

    if (deleteCoordinatorTable) {
      operator.createCoordinatorTable(options);
    }

    operator.close();
  }

  public enum InputSchemaType {
    FILE_PATH,
    SERIALIZED_JSON_STRING
  }
}
