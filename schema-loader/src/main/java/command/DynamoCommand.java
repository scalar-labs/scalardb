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

@Command(name = "--dynamo", description = "Using Dynamo DB")
public class DynamoCommand implements Callable<Integer> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DynamoCommand.class);
  @Option(names = {"-u", "--user"}, description = "AWS access key ID", required = true)
  String awsKeyId;

  @Option(names = {"-p", "--password"}, description = "AWS access secret key", required = true)
  String awsSecKey;

  @Option(names = "--region", description = "AWS region", required = true)
  String awsRegion;

  @Option(names = {"-r", "--ru"}, description = "Base resource unit")
  String ru;

  @Option(names = "--no-scaling", description = "Disable auto-scaling for Dynamo DB")
  Boolean noScaling;

  @Option(names = "--no-backup", description = "Disable continuous backup for Dynamo DB")
  Boolean noBackup;

  @Option(names = "--endpoint-override", description = "Endpoint with which the Dynamo DB SDK should communicate")
  String endpointOverride;

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

    LOGGER.info("Schema path: " + schemaFile.toString());

    Properties props = new Properties();
    props.setProperty("scalar.db.contact_points", awsRegion);
    props.setProperty("scalar.db.username", awsKeyId);
    props.setProperty("scalar.db.password", awsSecKey);
    props.setProperty("scalar.db.storage", "dynamo");

    Map<String, String> metaOptions = new HashMap<>();
    if (ru != null) {
      metaOptions.put("ru", ru);
    }
    if (noScaling != null) {
      metaOptions.put("no-scaling", noScaling.toString());
    }
    if (noBackup != null) {
      metaOptions.put("no-backup", noBackup.toString());
    }
    if (endpointOverride != null) {
      props.setProperty("scalar.db.dynamo.endpoint-override", endpointOverride);
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
