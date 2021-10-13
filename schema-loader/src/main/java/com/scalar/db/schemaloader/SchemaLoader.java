package com.scalar.db.schemaloader;

import com.scalar.db.schemaloader.command.CassandraCommand;
import com.scalar.db.schemaloader.command.ConfigFileBasedCommand;
import com.scalar.db.schemaloader.command.CosmosCommand;
import com.scalar.db.schemaloader.command.DynamoCommand;
import com.scalar.db.schemaloader.command.JdbcCommand;
import picocli.CommandLine;

public class SchemaLoader {

  public static void main(String... args) {
    boolean config = false;
    boolean cassandra = false;
    boolean cosmos = false;
    boolean dynamo = false;
    boolean jdbc = false;

    for (String arg : args) {
      switch (arg) {
        case "--config":
        case "-c":
          config = true;
          break;
        case "--cassandra":
          cassandra = true;
          break;
        case "--cosmos":
          cosmos = true;
          break;
        case "--dynamo":
          dynamo = true;
          break;
        case "--jdbc":
          jdbc = true;
          break;
        default:
          break;
      }
    }

    int status;
    if (config) {
      status = new CommandLine(new ConfigFileBasedCommand()).execute(args);
    } else if (cassandra) {
      status = new CommandLine(new CassandraCommand()).execute(args);
    } else if (cosmos) {
      status = new CommandLine(new CosmosCommand()).execute(args);
    } else if (dynamo) {
      status = new CommandLine(new DynamoCommand()).execute(args);
    } else if (jdbc) {
      status = new CommandLine(new JdbcCommand()).execute(args);
    } else {
      System.err.println(
          "Need to specify either --config <configPath> or --cassandra or --cosmos or --dynamo or --jdbc");
      status = 1;
    }
    System.exit(status);
  }
}
