package com.scalar.db.schemaloader.core;

import com.scalar.db.config.DatabaseConfig;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;

public class SchemaOperatorFactory {
  public static SchemaOperator getSchemaOperator(
      Properties properties, boolean isStorageSpecificCommand) {
    DatabaseConfig databaseConfig = new DatabaseConfig(properties);
    return new SchemaOperator(databaseConfig, isStorageSpecificCommand);
  }

  public static SchemaOperator getSchemaOperator(Path configPath, boolean isStorageSpecificCommand)
      throws IOException {
    DatabaseConfig dbConfig = new DatabaseConfig(new FileInputStream(configPath.toString()));
    return new SchemaOperator(dbConfig, false);
  }
}
