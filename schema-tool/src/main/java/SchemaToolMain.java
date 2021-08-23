import command.CassandraCommand;
import command.ConfigFileBasedCommand;
import command.CosmosCommand;
import command.DynamoCommand;
import command.JdbcCommand;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
    name = "schema-tool",
    description = "Schema tool for Scalar DB",
    subcommands = {
        ConfigFileBasedCommand.class,
        CosmosCommand.class,
        DynamoCommand.class,
        CassandraCommand.class,
        JdbcCommand.class
    })
public class SchemaToolMain implements Runnable {

  @Option(names = {"-h",
      "--help"}, usageHelp = true, description = "Displays this help message and quits.", defaultValue = "true")
  Boolean showHelp;

  public static void main(String... args) {
    new CommandLine(new SchemaToolMain()).execute(args);
  }

  @Override
  public void run() {
    if (showHelp) {
      CommandLine.usage(this, System.out);
    }
  }
}
