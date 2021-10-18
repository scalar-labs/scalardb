package com.scalar.db.schemaloader;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.schemaloader.command.CassandraCommand;
import com.scalar.db.schemaloader.command.CosmosCommand;
import com.scalar.db.schemaloader.command.DynamoCommand;
import com.scalar.db.schemaloader.command.JdbcCommand;
import com.scalar.db.schemaloader.command.SchemaLoaderCommand;
import picocli.CommandLine;

public class SchemaLoader {

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

  public static void main(String... args) {
    Object command = null;
    for (String arg : args) {
      if (COMMAND_MAP.containsKey(arg)) {
        command = COMMAND_MAP.get(arg);
        break;
      }
    }

    int status;
    if (command != null) {
      status = new CommandLine(command).execute(args);
    } else {
      System.err.println(
          "Need to specify either --config <configPath> or --cassandra or --cosmos or --dynamo or --jdbc");
      status = 1;
    }
    System.exit(status);
  }
}
