package com.scalar.db.server;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.server.config.ServerConfig;
import com.scalar.db.server.service.ServerModule;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "scalardb-server", description = "Starts Scalar DB server.")
public class ScalarDbServer implements Callable<Integer> {
  private static final Logger LOGGER = LoggerFactory.getLogger(ScalarDbServer.class);

  @CommandLine.Option(
      names = {"--properties", "--config"},
      required = true,
      paramLabel = "PROPERTIES_FILE",
      description = "A configuration file in properties format.")
  private String configFile;

  private Properties properties;
  private Server server;

  public ScalarDbServer() {}

  public ScalarDbServer(Properties properties) {
    this.properties = properties;
  }

  @Override
  public Integer call() throws Exception {
    start();
    addShutdownHook();
    blockUntilShutdown();
    return 0;
  }

  public void start() throws IOException {
    if (configFile != null) {
      properties = new Properties();
      properties.load(new FileInputStream(configFile));
    }

    ServerConfig config = new ServerConfig(properties);
    Injector injector = Guice.createInjector(new ServerModule(new DatabaseConfig(properties)));
    server =
        ServerBuilder.forPort(config.getPort())
            .addService(injector.getInstance(DistributedStorageService.class))
            .addService(injector.getInstance(DistributedStorageAdminService.class))
            .addService(injector.getInstance(DistributedTransactionService.class))
            .addService(new HealthService())
            .addService(ProtoReflectionService.newInstance())
            .build()
            .start();

    LOGGER.info("Scalar DB server started, listening on " + config.getPort());
  }

  public void addShutdownHook() {
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                  System.err.println("*** shutting down gRPC server since JVM is shutting down");
                  this.shutdown();
                  System.err.println("*** server shut down");
                }));
  }

  /** Await termination on the main thread since the grpc library uses daemon threads. */
  public void blockUntilShutdown() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
  }

  public void shutdown() {
    if (server != null) {
      server.shutdown();
    }
  }

  public static void main(String[] args) {
    int exitCode = new CommandLine(new ScalarDbServer()).execute(args);
    System.exit(exitCode);
  }
}
