package command;

import com.scalar.db.config.DatabaseConfig;
import core.SchemaOperator;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.logging.Logger;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import utils.SchemaParser;

@Command(name = "--jdbc", description = "Using JDBC type DB")
public class JdbcCommand implements Callable<Integer> {

  @Option(names = "-j", description = "JDBC URL", required = true)
  String jdbcURL;

  @Option(names = "-u", description = "JDBC user", required = true)
  String jdbcUser;

  @Option(names = "-p", description = "JDBC password", required = true)
  String jdbcPw;

  @Option(
      names = {"-f", "--schema-file"},
      description = "Path to schema json file",
      required = true)
  String schemaFile;

  @Option(
      names = {"-D", "--delete"},
      description = "Delete tables")
  Boolean deleteTables;

  @Override
  public Integer call() throws Exception {

    Logger.getGlobal().info("Schema path: " + schemaFile);

    Properties props = new Properties();
    props.setProperty("scalar.db.contact_points", jdbcURL);
    props.setProperty("scalar.db.username", jdbcUser);
    props.setProperty("scalar.db.password", jdbcPw);
    props.setProperty("scalar.db.storage", "jdbc");

    DatabaseConfig dbConfig = new DatabaseConfig(props);
    SchemaOperator operator = new SchemaOperator(dbConfig);
    SchemaParser schemaMap = new SchemaParser(schemaFile, new HashMap<String, String>());

    if (deleteTables) {
      operator.deleteTables(schemaMap.getTables());
    } else {
      operator.createTables(schemaMap.hasTransactionTable(), schemaMap.getTables());
    }

    return 0;
  }
}
