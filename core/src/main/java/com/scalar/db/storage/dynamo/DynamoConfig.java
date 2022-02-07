package com.scalar.db.storage.dynamo;

import static com.scalar.db.config.ConfigUtils.getString;

import com.scalar.db.config.DatabaseConfig;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.Properties;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

@SuppressFBWarnings("JCIP_FIELD_ISNT_FINAL_IN_IMMUTABLE_CLASS")
@Immutable
public class DynamoConfig extends DatabaseConfig {

  public static final String PREFIX = DatabaseConfig.PREFIX + "dynamo.";
  public static final String ENDPOINT_OVERRIDE = PREFIX + "endpoint-override";
  public static final String TABLE_METADATA_NAMESPACE = PREFIX + "table_metadata.namespace";

  @Nullable private String endpointOverride;
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

    endpointOverride = getString(getProperties(), ENDPOINT_OVERRIDE, null);
    tableMetadataNamespace = getString(getProperties(), TABLE_METADATA_NAMESPACE, null);
  }

  public Optional<String> getEndpointOverride() {
    return Optional.ofNullable(endpointOverride);
  }

  public Optional<String> getTableMetadataNamespace() {
    return Optional.ofNullable(tableMetadataNamespace);
  }
}
