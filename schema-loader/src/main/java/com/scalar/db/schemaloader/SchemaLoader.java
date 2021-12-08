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
import com.scalar.db.schemaloader.util.either.Either;
import com.scalar.db.schemaloader.util.either.Left;
import com.scalar.db.schemaloader.util.either.Right;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import javax.annotation.Nullable;
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
        command = COMMAND_MAP.get(arg);
        if (STORAGE_SPECIFIC_OPTION_LIST.contains(arg)) {
          LOGGER.warn(
              "Storage-specific options (--cassandra, --cosmos, --dynamo, --jdbc) "
                  + "are deprecated and will be removed in the future. Please use "
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
      LOGGER.error(
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
   * @param createCoordinatorTable create coordinator table or not.
   * @throws SchemaLoaderException thrown when creating tables failed.
   */
  public static void load(
      Properties configProperties,
      @Nullable Path schemaFilePath,
      Map<String, String> options,
      boolean createCoordinatorTable)
      throws SchemaLoaderException {
    Either<Path, Properties> config = new Right<>(configProperties);
    Either<Path, String> schema = new Left<>(schemaFilePath);
    load(config, schema, options, createCoordinatorTable);
  }

  /**
   * Creates tables defined in the schema file.
   *
   * @param configFilePath path to Scalar DB config file.
   * @param schemaFilePath path to schema file.
   * @param options specific options for creating tables.
   * @param createCoordinatorTable create coordinator table or not.
   * @throws SchemaLoaderException thrown when creating tables failed.
   */
  public static void load(
      Path configFilePath,
      @Nullable Path schemaFilePath,
      Map<String, String> options,
      boolean createCoordinatorTable)
      throws SchemaLoaderException {
    Either<Path, Properties> config = new Left<>(configFilePath);
    Either<Path, String> schema = new Left<>(schemaFilePath);
    load(config, schema, options, createCoordinatorTable);
  }

  /**
   * Creates tables defined in the schema.
   *
   * @param configProperties Scalar DB config properties.
   * @param serializedSchemaJson serialized json string schema.
   * @param options specific options for creating tables.
   * @param createCoordinatorTable create coordinator table or not.
   * @throws SchemaLoaderException thrown when creating tables failed.
   */
  public static void load(
      Properties configProperties,
      @Nullable String serializedSchemaJson,
      Map<String, String> options,
      boolean createCoordinatorTable)
      throws SchemaLoaderException {
    Either<Path, Properties> config = new Right<>(configProperties);
    Either<Path, String> schema = new Right<>(serializedSchemaJson);
    load(config, schema, options, createCoordinatorTable);
  }

  /**
   * Creates tables defined in the schema.
   *
   * @param configFilePath path to Scalar DB config file.
   * @param serializedSchemaJson serialized json string schema.
   * @param options specific options for creating tables.
   * @param createCoordinatorTable create coordinator table or not.
   * @throws SchemaLoaderException thrown when creating tables failed.
   */
  public static void load(
      Path configFilePath,
      @Nullable String serializedSchemaJson,
      Map<String, String> options,
      boolean createCoordinatorTable)
      throws SchemaLoaderException {
    Either<Path, Properties> config = new Left<>(configFilePath);
    Either<Path, String> schema = new Right<>(serializedSchemaJson);
    load(config, schema, options, createCoordinatorTable);
  }

  /**
   * Creates tables defined in the schema file.
   *
   * @param config Scalar DB config.
   * @param schema schema definition.
   * @param options specific options for creating tables.
   * @param createCoordinatorTable create coordinator table or not.
   * @throws SchemaLoaderException thrown when creating tables failed.
   */
  private static void load(
      Either<Path, Properties> config,
      Either<Path, String> schema,
      Map<String, String> options,
      boolean createCoordinatorTable)
      throws SchemaLoaderException {
    SchemaOperator operator = getSchemaOperator(config);

    // Create tables
    try {
      if (schema.isLeft()) {
        if (schema.getLeft() != null) {
          operator.createTables(schema.getLeft(), options);
        }
      } else {
        if (schema.getRight() != null) {
          operator.createTables(schema.getRight(), options);
        }
      }

      if (createCoordinatorTable) {
        operator.createCoordinatorTable(options);
      }

    } catch (SchemaOperatorException e) {
      throw new SchemaLoaderException("Creating tables failed.", e);
    } finally {
      operator.close();
    }
  }

  /**
   * Delete tables defined in the schema file.
   *
   * @param configProperties Scalar DB config properties.
   * @param schemaFilePath path to schema json file.
   * @param options specific options for deleting tables.
   * @param deleteCoordinatorTable delete coordinator table or not.
   * @throws SchemaLoaderException thrown when deleting tables failed.
   */
  public static void unload(
      Properties configProperties,
      @Nullable Path schemaFilePath,
      Map<String, String> options,
      boolean deleteCoordinatorTable)
      throws SchemaLoaderException {
    Either<Path, Properties> config = new Right<>(configProperties);
    Either<Path, String> schema = new Left<>(schemaFilePath);
    unload(config, schema, options, deleteCoordinatorTable);
  }

  /**
   * Delete tables defined in the schema file.
   *
   * @param configFilePath path to Scalar DB config file.
   * @param schemaFilePath path to schema json file.
   * @param options specific options for deleting tables.
   * @param deleteCoordinatorTable delete coordinator table or not.
   * @throws SchemaLoaderException thrown when deleting tables failed.
   */
  public static void unload(
      Path configFilePath,
      @Nullable Path schemaFilePath,
      Map<String, String> options,
      boolean deleteCoordinatorTable)
      throws SchemaLoaderException {
    Either<Path, Properties> config = new Left<>(configFilePath);
    Either<Path, String> schema = new Left<>(schemaFilePath);
    unload(config, schema, options, deleteCoordinatorTable);
  }

  /**
   * Delete tables defined in the schema.
   *
   * @param configProperties Scalar DB config properties.
   * @param serializedSchemaJson serialized json string schema.
   * @param options specific options for deleting tables.
   * @param deleteCoordinatorTable delete coordinator table or not.
   * @throws SchemaLoaderException thrown when deleting tables failed.
   */
  public static void unload(
      Properties configProperties,
      @Nullable String serializedSchemaJson,
      Map<String, String> options,
      boolean deleteCoordinatorTable)
      throws SchemaLoaderException {
    Either<Path, Properties> config = new Right<>(configProperties);
    Either<Path, String> schema = new Right<>(serializedSchemaJson);
    unload(config, schema, options, deleteCoordinatorTable);
  }

  /**
   * Delete tables defined in the schema.
   *
   * @param configFilePath path to Scalar DB config file.
   * @param serializedSchemaJson serialized json string schema.
   * @param options specific options for deleting tables.
   * @param deleteCoordinatorTable delete coordinator table or not.
   * @throws SchemaLoaderException thrown when deleting tables failed.
   */
  public static void unload(
      Path configFilePath,
      @Nullable String serializedSchemaJson,
      Map<String, String> options,
      boolean deleteCoordinatorTable)
      throws SchemaLoaderException {
    Either<Path, Properties> config = new Left<>(configFilePath);
    Either<Path, String> schema = new Right<>(serializedSchemaJson);
    unload(config, schema, options, deleteCoordinatorTable);
  }

  /**
   * Delete tables defined in the schema.
   *
   * @param config Scalar DB config.
   * @param schema schema definition.
   * @param options specific options for deleting tables.
   * @param deleteCoordinatorTable delete coordinator table or not.
   * @throws SchemaLoaderException thrown when deleting tables failed.
   */
  private static void unload(
      Either<Path, Properties> config,
      Either<Path, String> schema,
      Map<String, String> options,
      boolean deleteCoordinatorTable)
      throws SchemaLoaderException {
    SchemaOperator operator = getSchemaOperator(config);

    // Delete tables
    try {
      if (schema.isLeft()) {
        if (schema.getLeft() != null) {
          operator.deleteTables(schema.getLeft(), options);
        }
      } else {
        if (schema.getRight() != null) {
          operator.deleteTables(schema.getRight(), options);
        }
      }

      if (deleteCoordinatorTable) {
        operator.dropCoordinatorTable();
      }
    } catch (SchemaOperatorException e) {
      throw new SchemaLoaderException("Deleting tables failed.", e);
    } finally {
      operator.close();
    }
  }

  private static SchemaOperator getSchemaOperator(Either<Path, Properties> config)
      throws SchemaLoaderException {
    SchemaOperator operator;
    if (config.isLeft()) {
      try {
        assert config.getLeft() != null;
        operator = SchemaOperatorFactory.getSchemaOperator(config.getLeft(), true);
      } catch (SchemaOperatorException e) {
        throw new SchemaLoaderException("Initializing schema operator failed.", e);
      }
    } else {
      assert config.getRight() != null;
      operator = SchemaOperatorFactory.getSchemaOperator(config.getRight(), true);
    }

    return operator;
  }
}
