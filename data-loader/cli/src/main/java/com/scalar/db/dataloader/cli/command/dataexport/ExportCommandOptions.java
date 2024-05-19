package com.scalar.db.dataloader.cli.command.dataexport;

import picocli.CommandLine;

/** A class to represent the command options for the export command. */
public class ExportCommandOptions {

  @CommandLine.Option(
      names = {"--output-file", "-o"},
      paramLabel = "<OUTPUT_FILE>",
      description =
          "Path and name of the output file for the exported data (default: <table_name>.<format>)")
  protected String outputFilePath;
}
