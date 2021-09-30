package com.scalar.db.storage.dynamo;

import com.google.common.base.Strings;
import com.scalar.db.config.DatabaseConfig;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.Properties;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

@Immutable
@SuppressFBWarnings("JCIP_FIELD_ISNT_FINAL_IN_IMMUTABLE_CLASS")
public class DynamoConfig extends DatabaseConfig {

  public static final String PREFIX = DatabaseConfig.PREFIX + "dynamo.";
  public static final String ENDPOINT_OVERRIDE = PREFIX + "endpoint-override";
  public static final String TABLE_METADATA_NAMESPACE = PREFIX + "table_metadata.namespace";

  private Optional<String> endpointOverride;
  @Nullable private String tableMetadataNamespace;

  public DynamoConfig(File propertiesFile) throws IOException {
    super(propertiesFile);
  }

  public DynamoConfig(InputStream stream) throws IOException {
    super(stream);
  }

  public DynamoConfig(Properties properties) {
    super(properties);
  }

  @Override
  protected void load() {
    String storage = getProperties().getProperty(DatabaseConfig.STORAGE);
    if (storage == null || !storage.equals("dynamo")) {
      throw new IllegalArgumentException(DatabaseConfig.STORAGE + " should be 'dynamo'");
    }

    super.load();

    if (!Strings.isNullOrEmpty(getProperties().getProperty(ENDPOINT_OVERRIDE))) {
      endpointOverride = Optional.of(getProperties().getProperty(ENDPOINT_OVERRIDE));
    } else {
      endpointOverride = Optional.empty();
    }

    if (!Strings.isNullOrEmpty(getProperties().getProperty(TABLE_METADATA_NAMESPACE))) {
      tableMetadataNamespace = getProperties().getProperty(TABLE_METADATA_NAMESPACE);
    }
  }

  public Optional<String> getEndpointOverride() {
    return endpointOverride;
  }

  public Optional<String> getTableMetadataNamespace() {
    return Optional.ofNullable(tableMetadataNamespace);
  }
}
