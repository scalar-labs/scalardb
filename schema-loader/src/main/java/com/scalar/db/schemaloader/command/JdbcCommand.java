package com.scalar.db.schemaloader.command;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.schemaloader.core.SchemaOperator;
import com.scalar.db.schemaloader.schema.SchemaParser;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "--jdbc", description = "Using JDBC type DB")
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
      description = "Path to schema json file",
      required = true)
  private Path schemaFile;

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

    DatabaseConfig dbConfig = new DatabaseConfig(props);
    SchemaOperator operator = new SchemaOperator(dbConfig, true);
    SchemaParser schemaParser = new SchemaParser(schemaFile.toString(), Collections.emptyMap());

    if (deleteTables) {
      operator.deleteTables(schemaParser.getTables());
    } else {
      operator.createTables(schemaParser.getTables(), Collections.emptyMap());
    }

    operator.close();
    return 0;
  }
}
