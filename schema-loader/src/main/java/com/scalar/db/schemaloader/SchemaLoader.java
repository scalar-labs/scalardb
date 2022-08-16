package com.scalar.db.schemaloader;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.scalar.db.schemaloader.command.CassandraCommand;
import com.scalar.db.schemaloader.command.CosmosCommand;
import com.scalar.db.schemaloader.command.DynamoCommand;
import com.scalar.db.schemaloader.command.JdbcCommand;
import com.scalar.db.schemaloader.command.SchemaLoaderCommand;
import com.scalar.db.schemaloader.util.either.Either;
import com.scalar.db.schemaloader.util.either.Left;
import com.scalar.db.schemaloader.util.either.Right;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

public class SchemaLoader {
  private static final Logger logger = LoggerFactory.getLogger(SchemaLoader.class);
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
        command = COMMAND_MAP.get(arg);
        if (STORAGE_SPECIFIC_OPTION_LIST.contains(arg)) {
          logger.warn(
              "Storage-specific options (--cassandra, --cosmos, --dynamo, --jdbc) "
                  + "are deprecated and will be removed in 5.0.0. Please use "
                  + "the --config option along with your config file instead.");
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
      logger.error(
          "Need to specify either --config <configPath> or --cassandra or --cosmos or --dynamo or --jdbc");
      status = 1;
    }
    System.exit(status);
  }

  /**
   * Creates tables defined in the schema file.
   *
   * @param configProperties Scalar DB config properties.
   * @param schemaFilePath path to schema file.
   * @param options specific options for creating tables.
   * @param createCoordinatorTables create coordinator tables or not.
   * @throws SchemaLoaderException thrown when creating tables failed.
   */
  public static void load(
      Properties configProperties,
      @Nullable Path schemaFilePath,
      Map<String, String> options,
      boolean createCoordinatorTables)
      throws SchemaLoaderException {
    Either<Path, Properties> config = new Right<>(configProperties);
    Either<Path, String> schema = new Left<>(schemaFilePath);
    load(config, schema, options, createCoordinatorTables);
  }

  /**
   * Creates tables defined in the schema file.
   *
   * @param configFilePath path to Scalar DB config file.
   * @param schemaFilePath path to schema file.
   * @param options specific options for creating tables.
   * @param createCoordinatorTables create coordinator tables or not.
   * @throws SchemaLoaderException thrown when creating tables failed.
   */
  public static void load(
      Path configFilePath,
      @Nullable Path schemaFilePath,
      Map<String, String> options,
      boolean createCoordinatorTables)
      throws SchemaLoaderException {
    Either<Path, Properties> config = new Left<>(configFilePath);
    Either<Path, String> schema = new Left<>(schemaFilePath);
    load(config, schema, options, createCoordinatorTables);
  }

  /**
   * Creates tables defined in the schema.
   *
   * @param configProperties Scalar DB config properties.
   * @param serializedSchemaJson serialized json string schema.
   * @param options specific options for creating tables.
   * @param createCoordinatorTables create coordinator tables or not.
   * @throws SchemaLoaderException thrown when creating tables failed.
   */
  public static void load(
      Properties configProperties,
      @Nullable String serializedSchemaJson,
      Map<String, String> options,
      boolean createCoordinatorTables)
      throws SchemaLoaderException {
    Either<Path, Properties> config = new Right<>(configProperties);
    Either<Path, String> schema = new Right<>(serializedSchemaJson);
    load(config, schema, options, createCoordinatorTables);
  }

  /**
   * Creates tables defined in the schema.
   *
   * @param configFilePath path to Scalar DB config file.
   * @param serializedSchemaJson serialized json string schema.
   * @param options specific options for creating tables.
   * @param createCoordinatorTables create coordinator tables or not.
   * @throws SchemaLoaderException thrown when creating tables failed.
   */
  public static void load(
      Path configFilePath,
      @Nullable String serializedSchemaJson,
      Map<String, String> options,
      boolean createCoordinatorTables)
      throws SchemaLoaderException {
    Either<Path, Properties> config = new Left<>(configFilePath);
    Either<Path, String> schema = new Right<>(serializedSchemaJson);
    load(config, schema, options, createCoordinatorTables);
  }

  /**
   * Creates tables defined in the schema file.
   *
   * @param config Scalar DB config.
   * @param schema schema definition.
   * @param options specific options for creating tables.
   * @param createCoordinatorTables create coordinator tables or not.
   * @throws SchemaLoaderException thrown when creating tables failed.
   */
  private static void load(
      Either<Path, Properties> config,
      Either<Path, String> schema,
      Map<String, String> options,
      boolean createCoordinatorTables)
      throws SchemaLoaderException {
    // Parse the schema
    List<TableSchema> tableSchemaList = getTableSchemaList(schema, options);

    // Create tables
    SchemaOperator operator = getSchemaOperator(config);
    try {
      operator.createTables(tableSchemaList);
      if (createCoordinatorTables) {
        operator.createCoordinatorTables(options);
      }
    } finally {
      operator.close();
    }
  }

  /**
   * Delete tables defined in the schema file.
   *
   * @param configProperties Scalar DB config properties.
   * @param schemaFilePath path to schema json file.
   * @param deleteCoordinatorTables delete coordinator tables or not.
   * @throws SchemaLoaderException thrown when deleting tables failed.
   */
  public static void unload(
      Properties configProperties, @Nullable Path schemaFilePath, boolean deleteCoordinatorTables)
      throws SchemaLoaderException {
    Either<Path, Properties> config = new Right<>(configProperties);
    Either<Path, String> schema = new Left<>(schemaFilePath);
    unload(config, schema, deleteCoordinatorTables);
  }

  /**
   * Delete tables defined in the schema file.
   *
   * @param configFilePath path to Scalar DB config file.
   * @param schemaFilePath path to schema json file.
   * @param deleteCoordinatorTables delete coordinator tables or not.
   * @throws SchemaLoaderException thrown when deleting tables failed.
   */
  public static void unload(
      Path configFilePath, @Nullable Path schemaFilePath, boolean deleteCoordinatorTables)
      throws SchemaLoaderException {
    Either<Path, Properties> config = new Left<>(configFilePath);
    Either<Path, String> schema = new Left<>(schemaFilePath);
    unload(config, schema, deleteCoordinatorTables);
  }

  /**
   * Delete tables defined in the schema.
   *
   * @param configProperties Scalar DB config properties.
   * @param serializedSchemaJson serialized json string schema.
   * @param deleteCoordinatorTables delete coordinator tables or not.
   * @throws SchemaLoaderException thrown when deleting tables failed.
   */
  public static void unload(
      Properties configProperties,
      @Nullable String serializedSchemaJson,
      boolean deleteCoordinatorTables)
      throws SchemaLoaderException {
    Either<Path, Properties> config = new Right<>(configProperties);
    Either<Path, String> schema = new Right<>(serializedSchemaJson);
    unload(config, schema, deleteCoordinatorTables);
  }

  /**
   * Delete tables defined in the schema.
   *
   * @param configFilePath path to Scalar DB config file.
   * @param serializedSchemaJson serialized json string schema.
   * @param deleteCoordinatorTables delete coordinator tables or not.
   * @throws SchemaLoaderException thrown when deleting tables failed.
   */
  public static void unload(
      Path configFilePath, @Nullable String serializedSchemaJson, boolean deleteCoordinatorTables)
      throws SchemaLoaderException {
    Either<Path, Properties> config = new Left<>(configFilePath);
    Either<Path, String> schema = new Right<>(serializedSchemaJson);
    unload(config, schema, deleteCoordinatorTables);
  }

  /**
   * Delete tables defined in the schema.
   *
   * @param config Scalar DB config.
   * @param schema schema definition.
   * @param deleteCoordinatorTables delete coordinator tables or not.
   * @throws SchemaLoaderException thrown when deleting tables failed.
   */
  private static void unload(
      Either<Path, Properties> config, Either<Path, String> schema, boolean deleteCoordinatorTables)
      throws SchemaLoaderException {
    // Parse the schema
    List<TableSchema> tableSchemaList = getTableSchemaList(schema, Collections.emptyMap());

    // Delete tables
    SchemaOperator operator = getSchemaOperator(config);
    try {
      operator.deleteTables(tableSchemaList);
      if (deleteCoordinatorTables) {
        operator.dropCoordinatorTables();
      }
    } finally {
      operator.close();
    }
  }

  /**
   * Repair tables defined in the schema.
   *
   * @param configProperties Scalar DB config properties
   * @param serializedSchemaJson serialized json string schema.
   * @param options specific options for repairing tables.
   * @param repairCoordinatorTable repair coordinator tables or not.
   * @throws SchemaLoaderException thrown when repairing tables failed.
   */
  public static void repairTables(
      Properties configProperties,
      String serializedSchemaJson,
      Map<String, String> options,
      boolean repairCoordinatorTable)
      throws SchemaLoaderException {
    Either<Path, Properties> config = new Right<>(configProperties);
    Either<Path, String> schema = new Right<>(serializedSchemaJson);
    repairTables(config, schema, options, repairCoordinatorTable);
  }

  /**
   * Repair tables defined in the schema file.
   *
   * @param configProperties Scalar DB properties.
   * @param schemaPath path to the schema file.
   * @param options specific options for repairing tables.
   * @param repairCoordinatorTable repair coordinator tables or not.
   * @throws SchemaLoaderException thrown when repairing tables failed.
   */
  public static void repairTables(
      Properties configProperties,
      Path schemaPath,
      Map<String, String> options,
      boolean repairCoordinatorTable)
      throws SchemaLoaderException {
    Either<Path, Properties> config = new Right<>(configProperties);
    Either<Path, String> schema = new Left<>(schemaPath);
    repairTables(config, schema, options, repairCoordinatorTable);
  }

  /**
   * Repair tables defined in the schema.
   *
   * @param configPath path to the Scalar DB config.
   * @param serializedSchemaJson serialized json string schema.
   * @param options specific options for repairing tables.
   * @param repairCoordinatorTable repair coordinator tables or not.
   * @throws SchemaLoaderException thrown when repairing tables failed.
   */
  public static void repairTables(
      Path configPath,
      String serializedSchemaJson,
      Map<String, String> options,
      boolean repairCoordinatorTable)
      throws SchemaLoaderException {
    Either<Path, Properties> config = new Left<>(configPath);
    Either<Path, String> schema = new Right<>(serializedSchemaJson);
    repairTables(config, schema, options, repairCoordinatorTable);
  }

  /**
   * Repair tables defined in the schema file.
   *
   * @param configPath path to the Scalar DB config.
   * @param schemaPath path to the schema file.
   * @param options specific options for repairing tables.
   * @param repairCoordinatorTable repair coordinator tables or not.
   * @throws SchemaLoaderException thrown when repairing tables failed.
   */
  public static void repairTables(
      Path configPath, Path schemaPath, Map<String, String> options, boolean repairCoordinatorTable)
      throws SchemaLoaderException {
    Either<Path, Properties> config = new Left<>(configPath);
    Either<Path, String> schema = new Left<>(schemaPath);
    repairTables(config, schema, options, repairCoordinatorTable);
  }

  /**
   * Repair tables defined in the schema file.
   *
   * @param config Scalar DB config
   * @param schema schema.
   * @param options specific options for repairing tables.
   * @param repairCoordinatorTable repair coordinator tables or not.
   * @throws SchemaLoaderException thrown when repairing tables failed.
   */
  private static void repairTables(
      Either<Path, Properties> config,
      Either<Path, String> schema,
      Map<String, String> options,
      boolean repairCoordinatorTable)
      throws SchemaLoaderException {
    // Parse the schema
    List<TableSchema> tableSchemaList = getTableSchemaList(schema, options);

    // Repair tables
    SchemaOperator operator = getSchemaOperator(config);
    try {
      operator.repairTables(tableSchemaList);
      if (repairCoordinatorTable) {
        operator.repairCoordinatorTables(options);
      }
    } finally {
      operator.close();
    }
  }

  /**
   * Alter the tables defined in the schema. Supported alter operations are:
   *
   * <ul>
   *   <li>add non primary-key columns
   *   <li>create secondary indexes
   *   <li>delete secondary indexes
   * </ul>
   *
   * By comparing the differences between the provided schema and the current schema, it will know
   * which columns should be added and which secondary indexes should be created or deleted. Only
   * the tables with differences will be altered.
   *
   * @param configProperties Scalar DB config properties
   * @param serializedSchemaJson serialized json string schema.
   * @param indexCreationOptions specific options for index creation.
   * @throws SchemaLoaderException thrown when altering tables failed.
   */
  public static void alterTables(
      Properties configProperties,
      String serializedSchemaJson,
      Map<String, String> indexCreationOptions)
      throws SchemaLoaderException {
    Either<Path, Properties> config = new Right<>(configProperties);
    Either<Path, String> schema = new Right<>(serializedSchemaJson);
    alterTables(config, schema, indexCreationOptions);
  }

  /**
   * Alter the tables defined in the schema. Supported alter operations are:
   *
   * <ul>
   *   <li>add non primary-key columns
   *   <li>create secondary indexes
   *   <li>delete secondary indexes
   * </ul>
   *
   * By comparing the differences between the provided schema and the current schema, it will know
   * which columns should be added and which secondary indexes should be created or deleted. Only
   * the tables with differences will be altered.
   *
   * @param configProperties Scalar DB properties.
   * @param schemaPath path to the schema file.
   * @param indexCreationOptions specific options for index creation.
   * @throws SchemaLoaderException thrown when altering tables failed.
   */
  public static void alterTables(
      Properties configProperties, Path schemaPath, Map<String, String> indexCreationOptions)
      throws SchemaLoaderException {
    Either<Path, Properties> config = new Right<>(configProperties);
    Either<Path, String> schema = new Left<>(schemaPath);
    alterTables(config, schema, indexCreationOptions);
  }

  /**
   * Alter the tables defined in the schema. Supported alter operations are:
   *
   * <ul>
   *   <li>add non primary-key columns
   *   <li>create secondary indexes
   *   <li>delete secondary indexes
   * </ul>
   *
   * By comparing the differences between the provided schema and the current schema, it will know
   * which columns should be added and which secondary indexes should be created or deleted. Only
   * the tables with differences will be altered.
   *
   * @param configPath path to the Scalar DB config.
   * @param serializedSchemaJson serialized json string schema.
   * @param indexCreationOptions specific options for index creation.
   * @throws SchemaLoaderException thrown when altering tables failed.
   */
  public static void alterTables(
      Path configPath, String serializedSchemaJson, Map<String, String> indexCreationOptions)
      throws SchemaLoaderException {
    Either<Path, Properties> config = new Left<>(configPath);
    Either<Path, String> schema = new Right<>(serializedSchemaJson);
    alterTables(config, schema, indexCreationOptions);
  }

  /**
   * Alter the tables defined in the schema. Supported alter operations are:
   *
   * <ul>
   *   <li>add non primary-key columns
   *   <li>create secondary indexes
   *   <li>delete secondary indexes
   * </ul>
   *
   * By comparing the differences between the provided schema and the current schema, it will know
   * which columns should be added and which secondary indexes should be created or deleted. Only
   * the tables with differences will be altered.
   *
   * @param configPath path to the Scalar DB config.
   * @param schemaPath path to the schema file.
   * @param indexCreationOptions specific options for index creation.
   * @throws SchemaLoaderException thrown when altering tables failed.
   */
  public static void alterTables(
      Path configPath, Path schemaPath, Map<String, String> indexCreationOptions)
      throws SchemaLoaderException {
    Either<Path, Properties> config = new Left<>(configPath);
    Either<Path, String> schema = new Left<>(schemaPath);
    alterTables(config, schema, indexCreationOptions);
  }

  private static void alterTables(
      Either<Path, Properties> config,
      Either<Path, String> schema,
      Map<String, String> indexCreationOptions)
      throws SchemaLoaderException {
    // Parse the schema
    List<TableSchema> tableSchemaList = getTableSchemaList(schema, indexCreationOptions);

    // Alter tables
    SchemaOperator operator = getSchemaOperator(config);
    try {
      operator.alterTables(tableSchemaList, indexCreationOptions);
    } finally {
      operator.close();
    }
  }

  @VisibleForTesting
  static SchemaOperator getSchemaOperator(Either<Path, Properties> config)
      throws SchemaLoaderException {
    if (config.isLeft()) {
      try {
        assert config.getLeft() != null;
        return new SchemaOperator(config.getLeft());
      } catch (IOException e) {
        throw new SchemaLoaderException("Initializing schema operator failed.", e);
      }
    } else {
      assert config.getRight() != null;
      return new SchemaOperator(config.getRight());
    }
  }

  private static List<TableSchema> getTableSchemaList(
      Either<Path, String> schema, Map<String, String> options) throws SchemaLoaderException {
    if ((schema.isLeft() && schema.getLeft() != null)
        || (schema.isRight() && schema.getRight() != null)) {
      SchemaParser schemaParser = getSchemaParser(schema, options);
      return schemaParser.parse();
    }
    return Collections.emptyList();
  }

  @VisibleForTesting
  static SchemaParser getSchemaParser(Either<Path, String> schema, Map<String, String> options)
      throws SchemaLoaderException {
    assert (schema.isLeft() && schema.getLeft() != null)
        || (schema.isRight() && schema.getRight() != null);
    if (schema.isLeft()) {
      return new SchemaParser(schema.getLeft(), options);
    } else {
      return new SchemaParser(schema.getRight(), options);
    }
  }
}
