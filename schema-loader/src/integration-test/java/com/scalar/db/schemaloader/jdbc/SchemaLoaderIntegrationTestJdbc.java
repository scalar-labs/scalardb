package com.scalar.db.schemaloader.jdbc;

import com.google.common.collect.ImmutableList;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.schemaloader.SchemaLoaderIntegrationTestBase;
import com.scalar.db.storage.jdbc.JdbcConfig;
import com.scalar.db.storage.jdbc.JdbcEnv;
import java.io.FileOutputStream;
import java.util.List;
import java.util.Properties;
import org.junit.BeforeClass;

public class SchemaLoaderIntegrationTestJdbc extends SchemaLoaderIntegrationTestBase {
  private static final JdbcConfig config = JdbcEnv.getJdbcConfig();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Properties properties = config.getProperties();
    try (final FileOutputStream fileOutputStream = new FileOutputStream(CONFIG_FILE)) {
      properties.store(fileOutputStream, null);
    }
  }

  @Override
  protected DatabaseConfig getDatabaseConfig() {
    return config;
  }

  @Override
  protected List<String> getStorageSpecificCreationCommandArgs() {
    return ImmutableList.of(
        "java",
        "-jar",
        "scalardb-schema-loader.jar",
        "--jdbc",
        "-j",
        config.getContactPoints().get(0),
        "--schema-file",
        SCHEMA_FILE,
        "-u",
        config.getUsername().get(),
        "-p",
        config.getPassword().get());
  }
}
