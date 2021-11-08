package com.scalar.db.schemaloader.command;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.schemaloader.core.SchemaOperator;
import com.scalar.db.schemaloader.core.SchemaOperatorFactory;
import com.scalar.db.schemaloader.schema.Table;
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
    name = "java -jar scalardb-schema-loader-<version>.jar --jdbc",
    description = "Create/Delete JDBC schemas")
public class JdbcCommand implements Callable<Integer> {

  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcCommand.class);

  @Option(
      names = {"-j", "--jdbc-url"},
      description = "JDBC URL",
      required = true)
  private String url;

  @Option(
      names = {"-u", "--user"},
      description = "JDBC user",
      required = true)
  private String user;

  @Option(
      names = {"-p", "--password"},
      description = "JDBC password",
      required = true)
  private String password;

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
    props.setProperty(DatabaseConfig.CONTACT_POINTS, url);
    props.setProperty(DatabaseConfig.USERNAME, user);
    props.setProperty(DatabaseConfig.PASSWORD, password);
    props.setProperty(DatabaseConfig.STORAGE, "jdbc");

    Map<String, String> metaOptions = new HashMap<>();
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
