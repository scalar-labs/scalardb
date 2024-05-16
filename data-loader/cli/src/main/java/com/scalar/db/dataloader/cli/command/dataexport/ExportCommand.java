package com.scalar.db.dataloader.cli.command.dataexport;

import java.util.concurrent.Callable;
import picocli.CommandLine;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

@CommandLine.Command(name = "export", description = "Export data from a ScalarDB table")
public class ExportCommand extends ExportCommandOptions implements Callable<Integer> {

  @Spec CommandSpec spec;

  @Override
  public Integer call() throws Exception {
    return 0;
  }
}
