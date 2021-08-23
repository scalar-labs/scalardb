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
  @Option(names = {"-h", "--help"}, defaultValue = "true")
  Boolean showHelp;

  public static void main(String... args) {
    new CommandLine(new SchemaToolMain()).execute(args);
  }

  @Override
  public void run() {
    if (showHelp) {
      System.out.printf(
          "Usage: schema-tool [COMMAND] [OPTIONS]\n"
              + "Schema tool for Scalar DB\n"
              + "Commands:\n"
              + "  --config     Using config file for Scalar DB\n"
              + "  --cosmos     Using Cosmos DB\n"
              + "  --dynamo     Using Dynamo DB\n"
              + "  --cassandra  Using Cassandra DB\n"
              + "  --jdbc       Using JDBC type DB\n");
    }
  }
}
