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

@Command(name = "--dynamo", description = "Using Dynamo DB")
public class DynamoCommand implements Callable<Integer> {

  @Option(names = {"-u", "--user"}, description = "AWS access key ID", required = true)
  String awsKeyId;

  @Option(names = {"-p", "--password"}, description = "AWS access secret key", required = true)
  String awsSecKey;

  @Option(names = "--region", description = "AWS region", required = true)
  String awsRegion;

  @Option(names = {"-r", "--ru"}, description = "Base resource unit")
  String dynamoRU;

  @Option(names = "--no-scaling", description = "Disable auto-scaling for Dynamo DB")
  Boolean dynamoNoScaling;

  @Option(names = "no-backup", description = "Disable continuous backup for Dynamo DB")
  Boolean dynamoNoBackup;

  @Option(names = "--endpoint-override", description = "Endpoint with which the DynamoDB SDK should communicate")
  String dynamoEndpointOverride;

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

    Logger.getGlobal().info("Schema path: " + schemaFile.toString());

    Properties props = new Properties();
    props.setProperty("scalar.db.contact_points", awsRegion);
    props.setProperty("scalar.db.username", awsKeyId);
    props.setProperty("scalar.db.password", awsSecKey);
    props.setProperty("scalar.db.storage", "dynamo");

    Map<String, String> metaOptions = new HashMap<String, String>();
    if (dynamoRU != null) {
      metaOptions.put("ru", dynamoRU);
    }
    if (dynamoNoScaling != null) {
      metaOptions.put("no-scaling", dynamoNoScaling.toString());
    }
    if (dynamoNoBackup != null) {
      metaOptions.put("no-backup", dynamoNoBackup.toString());
    }
    if (dynamoEndpointOverride != null) {
      props.setProperty("scalar.db.dynamo.endpoint-override", dynamoEndpointOverride);
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
