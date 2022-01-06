package com.scalar.db.schemaloader.command;

import com.scalar.db.config.DatabaseConfig;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
    name = "java -jar scalardb-schema-loader-<version>.jar --jdbc",
    description = "Create/Delete JDBC schemas")
public class JdbcCommand extends StorageSpecificCommand implements Callable<Integer> {

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

  @Override
  public Integer call() throws Exception {
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, url);
    props.setProperty(DatabaseConfig.USERNAME, user);
    props.setProperty(DatabaseConfig.PASSWORD, password);
    props.setProperty(DatabaseConfig.STORAGE, "jdbc");

    Map<String, String> options = Collections.emptyMap();

    execute(props, options);
    return 0;
  }
}
