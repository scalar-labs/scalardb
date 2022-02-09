package com.scalar.db.server;

import com.google.common.base.Strings;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.scalar.db.config.DatabaseConfig;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "scalardb-server", description = "Starts Scalar DB server.")
public class ScalarDbServer implements Callable<Integer> {
  private static final Logger LOGGER = LoggerFactory.getLogger(ScalarDbServer.class);
  private static final long MAX_WAIT_TIME_MILLIS = 60000; // 60 seconds

  @CommandLine.Option(
      names = {"--properties", "--config"},
      required = true,
      paramLabel = "PROPERTIES_FILE",
      description = "A configuration file in properties format.")
  private String configFile;

  private ServerConfig config;
  private Server server;

  public ScalarDbServer() {}

  public ScalarDbServer(ServerConfig config) {
    this.config = config;
  }

  @Override
  public Integer call() throws Exception {
    addShutdownHook();
    start();
    blockUntilShutdown();
    return 0;
  }

  public void start() throws IOException {
    if (configFile != null) {
      config = new ServerConfig(new File(configFile));
    }

    Injector injector =
        Guice.createInjector(new ServerModule(config, new DatabaseConfig(config.getProperties())));

    ServerBuilder<?> builder =
        ServerBuilder.forPort(config.getPort())
            .addService(injector.getInstance(DistributedStorageService.class))
            .addService(injector.getInstance(DistributedStorageAdminService.class))
            .addService(injector.getInstance(DistributedTransactionService.class))
            .addService(injector.getInstance(AdminService.class))
            .addService(new HealthService())
            .addService(ProtoReflectionService.newInstance());

    // Two-phase commit for JDBC is not supported for now
    String transactionManager =
        config.getProperties().getProperty(DatabaseConfig.TRANSACTION_MANAGER);
    if (Strings.isNullOrEmpty(transactionManager) || !transactionManager.equals("jdbc")) {
      builder.addService(injector.getInstance(TwoPhaseCommitTransactionService.class));
    } else {
      LOGGER.warn(
          "TwoPhaseCommitTransactionService doesn't start when setting \""
              + DatabaseConfig.TRANSACTION_MANAGER
              + "\" to 'jdbc'");
    }

    server = builder.build().start();

    LOGGER.info("Scalar DB server started, listening on {}", config.getPort());
  }

  public void addShutdownHook() {
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  LOGGER.info("Signal received. Shutting down the server ...");
                  shutdown();
                  blockUntilShutdown(MAX_WAIT_TIME_MILLIS, TimeUnit.MILLISECONDS);
                  LOGGER.info("The server shut down.");
                }));
  }

  public void blockUntilShutdown() {
    if (server != null) {
      try {
        server.awaitTermination();
      } catch (InterruptedException ignored) {
        // don't need to handle InterruptedException
      }
    }
  }

  public void blockUntilShutdown(long timeout, TimeUnit unit) {
    if (server != null) {
      try {
        server.awaitTermination(timeout, unit);
      } catch (InterruptedException ignored) {
        // don't need to handle InterruptedException
      }
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
