package com.scalar.db.dataloader.cli.command.dataimport;

import java.util.concurrent.Callable;
import picocli.CommandLine;

@CommandLine.Command(name = "import", description = "Import data into a ScalarDB table")
public class ImportCommand extends ImportCommandOptions implements Callable<Integer> {
  @CommandLine.Spec CommandLine.Model.CommandSpec spec;

  @Override
  public Integer call() throws Exception {
    return 0;
  }
}
