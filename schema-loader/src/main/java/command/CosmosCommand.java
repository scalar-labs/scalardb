package command;

import com.scalar.db.config.DatabaseConfig;
import core.SchemaOperator;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.logging.Logger;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import utils.SchemaParser;

@Command(name = "--cosmos", description = "Using Cosmos DB")
public class CosmosCommand implements Callable<Integer> {

  @Option(names = {"-h", "--host"}, description = "Cosmos DB account URI", required = true)
  String cosmosURI;

  @Option(names = {"-p", "--password"}, description = "Cosmos DB key", required = true)
  String cosmosKey;

  @Option(names = {"-r", "--ru"}, description = "Base resource unit", defaultValue = "400")
  String cosmosRU;

  @Option(names = "--no-scaling", description = "Disable auto-scaling for Cosmos DB")
  Boolean cosmosNoScaling;

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

    Logger.getGlobal().info("Schema path: " + schemaFile);

    Properties props = new Properties();
    props.setProperty("scalar.db.contact_points", cosmosURI);
    props.setProperty("scalar.db.password", cosmosKey);
    props.setProperty("scalar.db.storage", "cosmos");

    Map<String, String> metaOptions = new HashMap<String, String>();
    if (cosmosRU != null) {
      metaOptions.put("ru", cosmosRU);
    }
    if (cosmosNoScaling != null) {
      metaOptions.put("no-scaling", cosmosNoScaling.toString());
    }

    DatabaseConfig dbConfig = new DatabaseConfig(props);
    SchemaOperator operator = new SchemaOperator(dbConfig);
    SchemaParser schemaMap = new SchemaParser(schemaFile.toString(), metaOptions);

    if (deleteTables) {
      operator.deleteTables(schemaMap.getTables());
    } else {
      operator.createTables(schemaMap.hasTransactionTable(), schemaMap.getTables());
    }
    return 0;
  }
}
