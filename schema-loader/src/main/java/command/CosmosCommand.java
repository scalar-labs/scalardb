package command;

import com.scalar.db.config.DatabaseConfig;
import core.SchemaOperator;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import schema.SchemaParser;

@Command(name = "--cosmos", description = "Using Cosmos DB")
public class CosmosCommand implements Callable<Integer> {

  private static final Logger LOGGER = LoggerFactory.getLogger(CosmosCommand.class);
  @Option(names = {"-h", "--host"}, description = "Cosmos DB account URI", required = true)
  String uri;

  @Option(names = {"-p", "--password"}, description = "Cosmos DB key", required = true)
  String key;

  @Option(names = {"-r", "--ru"}, description = "Base resource unit", defaultValue = "400")
  String ru;

  @Option(names = "--no-scaling", description = "Disable auto-scaling for Cosmos DB")
  Boolean noScaling;

  @Option(
      names = {"-f", "--schema-file"},
      description = "Path to schema json file",
      required = true)
  Path schemaFile;

  @Option(
      names = {"-D", "--delete-all"},
      description = "Delete tables",
      defaultValue = "false")
  Boolean deleteTables;

  @Override
  public Integer call() throws Exception {

    LOGGER.info("Schema path: " + schemaFile);

    Properties props = new Properties();
    props.setProperty("scalar.db.contact_points", uri);
    props.setProperty("scalar.db.password", key);
    props.setProperty("scalar.db.storage", "cosmos");

    Map<String, String> metaOptions = new HashMap<>();
    if (ru != null) {
      metaOptions.put("ru", ru);
    }
    if (noScaling != null) {
      metaOptions.put("no-scaling", noScaling.toString());
    }

    DatabaseConfig dbConfig = new DatabaseConfig(props);
    SchemaOperator operator = new SchemaOperator(dbConfig);
    SchemaParser schemaParser = new SchemaParser(schemaFile.toString(), metaOptions);

    if (deleteTables) {
      operator.deleteTables(schemaParser.getTables());
    } else {
      operator.createTables(schemaParser.getTables());
    }
    return 0;
  }
}
