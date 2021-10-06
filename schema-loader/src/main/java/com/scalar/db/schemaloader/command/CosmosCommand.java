package com.scalar.db.schemaloader.command;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.schemaloader.core.SchemaOperator;
import com.scalar.db.schemaloader.schema.SchemaParser;
import com.scalar.db.storage.cosmos.CosmosAdmin;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "--cosmos", description = "Using Cosmos DB")
public class CosmosCommand implements Callable<Integer> {

  private static final Logger LOGGER = LoggerFactory.getLogger(CosmosCommand.class);

  @Option(
      names = {"-h", "--host"},
      description = "Cosmos DB account URI",
      required = true)
  private String uri;

  @Option(
      names = {"-p", "--password"},
      description = "Cosmos DB key",
      required = true)
  private String key;

  @Option(
      names = {"-r", "--ru"},
      description = "Base resource unit",
      defaultValue = "400")
  private String ru;

  @Option(names = "--no-scaling", description = "Disable auto-scaling for Cosmos DB")
  private Boolean noScaling;

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

    LOGGER.info("Schema path: " + schemaFile);

    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, uri);
    props.setProperty(DatabaseConfig.PASSWORD, key);
    props.setProperty(DatabaseConfig.STORAGE, "cosmos");

    Map<String, String> metaOptions = new HashMap<>();
    if (ru != null) {
      metaOptions.put(CosmosAdmin.REQUEST_UNIT, ru);
    }
    if (noScaling != null) {
      metaOptions.put(CosmosAdmin.NO_SCALING, noScaling.toString());
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
