package com.scalar.db.schemaloader.command;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.schemaloader.core.SchemaOperator;
import com.scalar.db.schemaloader.schema.SchemaParser;
import com.scalar.db.storage.dynamo.DynamoAdmin;
import com.scalar.db.storage.dynamo.DynamoConfig;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "--dynamo", description = "Using Dynamo DB")
public class DynamoCommand implements Callable<Integer> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DynamoCommand.class);

  @Option(
      names = {"-u", "--user"},
      description = "AWS access key ID",
      required = true)
  private String awsKeyId;

  @Option(
      names = {"-p", "--password"},
      description = "AWS access secret key",
      required = true)
  private String awsSecKey;

  @Option(names = "--region", description = "AWS region", required = true)
  private String awsRegion;

  @Option(
      names = {"-r", "--ru"},
      description = "Base resource unit")
  private String ru;

  @Option(names = "--no-scaling", description = "Disable auto-scaling for Dynamo DB")
  private Boolean noScaling;

  @Option(names = "--no-backup", description = "Disable continuous backup for Dynamo DB")
  private Boolean noBackup;

  @Option(
      names = "--endpoint-override",
      description = "Endpoint with which the Dynamo DB SDK should communicate")
  private String endpointOverride;

  @Option(
      names = {"-f", "--schema-file"},
      description = "Path to schema json file",
      required = true)
  private Path schemaFile;

  @Option(
      names = {"-D", "--delete-all"},
      description = "Delete tables",
      defaultValue = "false")
  private Boolean deleteTables;

  @Override
  public Integer call() throws Exception {

    LOGGER.info("Schema path: " + schemaFile.toString());

    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, awsRegion);
    props.setProperty(DatabaseConfig.USERNAME, awsKeyId);
    props.setProperty(DatabaseConfig.PASSWORD, awsSecKey);
    props.setProperty(DatabaseConfig.STORAGE, "dynamo");

    Map<String, String> metaOptions = new HashMap<>();
    if (ru != null) {
      metaOptions.put(DynamoAdmin.REQUEST_UNIT, ru);
    }
    if (noScaling != null) {
      metaOptions.put(DynamoAdmin.NO_SCALING, noScaling.toString());
    }
    if (noBackup != null) {
      metaOptions.put(DynamoAdmin.NO_BACKUP, noBackup.toString());
    }
    if (endpointOverride != null) {
      props.setProperty(DynamoConfig.ENDPOINT_OVERRIDE, endpointOverride);
    }

    DatabaseConfig dbConfig = new DatabaseConfig(props);
    SchemaOperator operator = new SchemaOperator(dbConfig);
    SchemaParser schemaParser = new SchemaParser(schemaFile.toString(), metaOptions);

    if (deleteTables) {
      operator.deleteTables(schemaParser.getTables());
    } else {
      operator.createTables(schemaParser.getTables());
    }

    operator.close();
    return 0;
  }
}
