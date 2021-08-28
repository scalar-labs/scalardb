package com.scalar.db.schemaloader;

import com.scalar.db.schemaloader.command.CassandraCommand;
import com.scalar.db.schemaloader.command.ConfigFileBasedCommand;
import com.scalar.db.schemaloader.command.CosmosCommand;
import com.scalar.db.schemaloader.command.DynamoCommand;
import com.scalar.db.schemaloader.command.JdbcCommand;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
    name = "schema-loader",
    description = "Schema Loader for Scalar DB",
    subcommands = {
      ConfigFileBasedCommand.class,
      CosmosCommand.class,
      DynamoCommand.class,
      CassandraCommand.class,
      JdbcCommand.class
    })
public class SchemaLoader implements Runnable {

  @Option(
      names = {"-h", "--help"},
      usageHelp = true,
      description = "Displays this help message and quits.",
      defaultValue = "true")
  Boolean showHelp;

  public static void main(String... args) {
    new CommandLine(new SchemaLoader()).execute(args);
  }

  @Override
  public void run() {
    if (showHelp) {
      CommandLine.usage(this, System.out);
    }
  }
}
