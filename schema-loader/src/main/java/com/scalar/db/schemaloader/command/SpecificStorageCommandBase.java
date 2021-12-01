package com.scalar.db.schemaloader.command;

import com.scalar.db.schemaloader.SchemaLoaderException;
import com.scalar.db.schemaloader.core.SchemaOperator;
import com.scalar.db.schemaloader.core.SchemaOperatorException;
import com.scalar.db.schemaloader.core.SchemaOperatorFactory;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;

public abstract class SpecificStorageCommandBase {
  protected void execute(
      Properties props, Path schemaFile, Map<String, String> metaOptions, boolean deleteTables)
      throws SchemaLoaderException {
    SchemaOperator operator = SchemaOperatorFactory.getSchemaOperator(props, false);

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
