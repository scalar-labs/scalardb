package com.scalar.db.dataloader.cli;

import com.scalar.db.dataloader.cli.command.dataexport.ExportCommand;
import com.scalar.db.dataloader.cli.command.dataimport.ImportCommand;
import picocli.CommandLine;

/** The main class to start the ScalarDB Data loader CLI tool */
@CommandLine.Command(
    description = "ScalarDB Data Loader CLI",
    mixinStandardHelpOptions = true,
    version = "1.0",
    subcommands = {ImportCommand.class, ExportCommand.class})
public class DataLoaderCli {

  /**
   * Main method to start the ScalarDB Data Loader CLI tool
   *
   * @param args the command line arguments
   */
  public static void main(String[] args) {
    int exitCode =
        new CommandLine(new DataLoaderCli())
            .setCaseInsensitiveEnumValuesAllowed(true)
            .execute(args);
    System.exit(exitCode);
  }
}
