package command;

import core.SchemaOperator;
import java.util.concurrent.Callable;
import java.util.logging.Logger;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import utils.SchemaParser;

@Command(name = "schema-tool", description = "Schema tool for Scalar DB")
public class SchemaToolCommand implements Callable<Integer> {

  @Option(
      names = {"-cfg", "--config"},
      description = "Path to config file of Scalar DB")
  String configPath;

  @Option(
      names = {"-f", "--schema-file"},
      description = "Path to schema json file")
  String schemaFile;

  @Override
  public Integer call() throws Exception {

    Logger.getGlobal().info("Config path: " + configPath);
    Logger.getGlobal().info("Schema path: " + schemaFile);
    SchemaOperator operator = new SchemaOperator(configPath);
    SchemaParser schemaMap = new SchemaParser(schemaFile);

    operator.createTables(schemaMap.hasTransactionTable(), schemaMap.getTables());
    return null;
  }
}
