package com.scalar.db.schemaloader.command;

import com.scalar.db.schemaloader.SchemaLoaderException;
import com.scalar.db.schemaloader.core.SchemaOperator;
import com.scalar.db.schemaloader.core.SchemaOperatorException;
import com.scalar.db.schemaloader.core.SchemaOperatorFactory;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Option;

public abstract class StorageSpecificCommandBase {
  private static final Logger LOGGER = LoggerFactory.getLogger(StorageSpecificCommandBase.class);

  @Option(
      names = {"-f", "--schema-file"},
      description = "Path to the schema json file",
      required = true)
  private Path schemaFile;

  @Option(
      names = {"-D", "--delete-all"},
      description = "Delete tables",
      defaultValue = "false")
  private boolean deleteTables;

  @Option(
      names = {"--prefix"},
      description = "Namespace prefix",
      hidden = true)
  protected String prefix;

  protected void execute(Properties props, Map<String, String> metaOptions)
      throws SchemaLoaderException {
    SchemaOperator operator = SchemaOperatorFactory.getSchemaOperator(props, false);
    LOGGER.info("Schema path: " + schemaFile);

    if (prefix != null) {
      metaOptions.put(SchemaOperator.NAMESPACE_PREFIX, prefix);
    }

    try {
      if (deleteTables) {
        operator.deleteTables(schemaFile, metaOptions);
      } else {
        operator.createTables(schemaFile, metaOptions);
      }
    } catch (SchemaOperatorException e) {
      throw new SchemaLoaderException(
          deleteTables ? "Deleting" : "Creating" + " tables failed.", e);
    } finally {
      operator.close();
    }
  }
}
