package com.scalar.db.schemaloader.command;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.schemaloader.core.SchemaOperator;
import com.scalar.db.schemaloader.core.SchemaOperatorFactory;
import com.scalar.db.schemaloader.schema.Table;
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

@Command(
    name = "java -jar scalardb-schema-loader-<version>.jar --cosmos",
    description = "Create/Delete Cosmos DB schemas")
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
      description = "Path to the schema json file",
      required = true)
  private Path schemaFile;

  @Option(names = "--prefix", description = "Namespace prefix for all the tables")
  private String namespacePrefix;

  @Option(
      names = {"-D", "--delete-all"},
      description = "Delete tables",
      defaultValue = "false")
  private boolean deleteTables;

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
    if (namespacePrefix != null) {
      metaOptions.put(Table.NAMESPACE_PREFIX, namespacePrefix);
    }

    SchemaOperator operator = SchemaOperatorFactory.getSchemaOperator(props, false);

    if (deleteTables) {
      operator.deleteTables(schemaFile, metaOptions);
    } else {
      operator.createTables(schemaFile, metaOptions);
    }

    operator.close();
    return 0;
  }
}
