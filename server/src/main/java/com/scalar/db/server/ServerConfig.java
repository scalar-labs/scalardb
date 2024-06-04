package com.scalar.db.server;

import static com.scalar.db.config.ConfigUtils.getInt;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Properties;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

@SuppressFBWarnings("JCIP_FIELD_ISNT_FINAL_IN_IMMUTABLE_CLASS")
@Immutable
public class ServerConfig {

  public static final String PREFIX = "scalar.db.server.";
  public static final String PORT = PREFIX + "port";
  public static final String PROMETHEUS_EXPORTER_PORT = PREFIX + "prometheus_exporter_port";

  public static final String GRPC_MAX_INBOUND_MESSAGE_SIZE =
      PREFIX + "grpc.max_inbound_message_size";
  public static final String GRPC_MAX_INBOUND_METADATA_SIZE =
      PREFIX + "grpc.max_inbound_metadata_size";

  public static final int DEFAULT_PORT = 60051;
  public static final int DEFAULT_PROMETHEUS_EXPORTER_PORT = 8080;

  private final Properties props;
  private int port;
  private int prometheusExporterPort;

  @Nullable private Integer grpcMaxInboundMessageSize;
  @Nullable private Integer grpcMaxInboundMetadataSize;

  public ServerConfig(File propertiesFile) throws IOException {
    try (FileInputStream stream = new FileInputStream(propertiesFile)) {
      props = new Properties();
      props.load(stream);
    }
    load();
  }

  public ServerConfig(InputStream stream) throws IOException {
    props = new Properties();
    props.load(stream);
    load();
  }

  public ServerConfig(Properties properties) {
    props = new Properties();
    props.putAll(properties);
    load();
  }

  public ServerConfig(Path propertiesPath) throws IOException {
    this(propertiesPath.toFile());
  }

  // For the SpotBugs warning CT_CONSTRUCTOR_THROW
  @Override
  protected final void finalize() {}

  public Properties getProperties() {
    Properties ret = new Properties();
    ret.putAll(props);
    return ret;
  }

  private void load() {
    port = getInt(getProperties(), PORT, DEFAULT_PORT);
    prometheusExporterPort =
        getInt(getProperties(), PROMETHEUS_EXPORTER_PORT, DEFAULT_PROMETHEUS_EXPORTER_PORT);

    grpcMaxInboundMessageSize = getInt(getProperties(), GRPC_MAX_INBOUND_MESSAGE_SIZE, null);
    grpcMaxInboundMetadataSize = getInt(getProperties(), GRPC_MAX_INBOUND_METADATA_SIZE, null);
  }

  public int getPort() {
    return port;
  }

  public int getPrometheusExporterPort() {
    return prometheusExporterPort;
  }

  public Optional<Integer> getGrpcMaxInboundMessageSize() {
    return Optional.ofNullable(grpcMaxInboundMessageSize);
  }

  public Optional<Integer> getGrpcMaxInboundMetadataSize() {
    return Optional.ofNullable(grpcMaxInboundMetadataSize);
  }
}
