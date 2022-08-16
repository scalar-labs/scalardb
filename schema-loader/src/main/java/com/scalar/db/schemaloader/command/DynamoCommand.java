package com.scalar.db.schemaloader.command;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.schemaloader.SchemaLoaderException;
import com.scalar.db.storage.dynamo.DynamoAdmin;
import com.scalar.db.storage.dynamo.DynamoConfig;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
    name = "java -jar scalardb-schema-loader-<version>.jar --dynamo",
    description = "Create/Delete DynamoDB schemas")
public class DynamoCommand extends StorageSpecificCommand implements Callable<Integer> {

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

  @Option(names = "--no-scaling", description = "Disable auto-scaling for DynamoDB")
  private Boolean noScaling;

  @Option(names = "--no-backup", description = "Disable continuous backup for DynamoDB")
  private Boolean noBackup;

  @Option(
      names = "--endpoint-override",
      description = "Endpoint with which the DynamoDB SDK should communicate")
  private String endpointOverride;

  @ArgGroup private Mode mode;

  @Override
  public Integer call() throws SchemaLoaderException {
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, awsRegion);
    props.setProperty(DatabaseConfig.USERNAME, awsKeyId);
    props.setProperty(DatabaseConfig.PASSWORD, awsSecKey);
    props.setProperty(DatabaseConfig.STORAGE, "dynamo");

    if (endpointOverride != null) {
      props.setProperty(DynamoConfig.ENDPOINT_OVERRIDE, endpointOverride);
    }

    Map<String, String> options = new HashMap<>();
    if (ru != null) {
      options.put(DynamoAdmin.REQUEST_UNIT, ru);
    }
    if (noScaling != null) {
      options.put(DynamoAdmin.NO_SCALING, noScaling.toString());
    }
    if (noBackup != null) {
      options.put(DynamoAdmin.NO_BACKUP, noBackup.toString());
    }

    execute(props, options);
    return 0;
  }

  @Override
  Mode getMode() {
    return mode;
  }
}
