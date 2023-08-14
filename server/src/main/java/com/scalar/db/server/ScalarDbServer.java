package com.scalar.db.server;

import com.google.common.base.Strings;
import com.google.common.util.concurrent.Uninterruptibles;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.TwoPhaseCommitTransactionManager;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.service.StorageFactory;
import com.scalar.db.service.TransactionFactory;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;
import java.io.File;
import java.io.IOException;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "scalardb-server", description = "Starts ScalarDB Server.")
public class ScalarDbServer implements Callable<Integer> {
  private static final Logger logger = LoggerFactory.getLogger(ScalarDbServer.class);

  private static final long MAX_SHUTDOWN_WAIT_TIME_MILLIS = 60000;

  @CommandLine.Option(
      names = {"--properties", "--config"},
      required = true,
      paramLabel = "PROPERTIES_FILE",
      description = "A configuration file in properties format.")
  private String configFile;

  private ServerConfig config;
  private Server server;
  private final HealthService healthService = new HealthService();

  private DistributedStorage storage;
  private DistributedStorageAdmin storageAdmin;
  private DistributedTransactionManager transactionManager;
  private DistributedTransactionAdmin transactionAdmin;
  private TwoPhaseCommitTransactionManager twoPhaseCommitTransactionManager;

  public ScalarDbServer() {}

  public ScalarDbServer(Properties properties) {
    this.config = new ServerConfig(Objects.requireNonNull(properties));
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

    StorageFactory storageFactory = StorageFactory.create(config.getProperties());
    storage = storageFactory.getStorage();
    storageAdmin = storageFactory.getStorageAdmin();

    TransactionFactory transactionFactory = TransactionFactory.create(config.getProperties());
    transactionManager = transactionFactory.getTransactionManager();
    transactionAdmin = transactionFactory.getTransactionAdmin();

    DatabaseConfig databaseConfig = new DatabaseConfig(config.getProperties());
    TableMetadataManager tableMetadataManager =
        new TableMetadataManager(storageAdmin, databaseConfig.getMetadataCacheExpirationTimeSecs());

    GateKeeper gateKeeper = new SynchronizedGateKeeper();
    Metrics metrics = new Metrics(config);

    ServerBuilder<?> builder =
        ServerBuilder.forPort(config.getPort())
            .addService(
                new DistributedStorageService(storage, tableMetadataManager, gateKeeper, metrics))
            .addService(new DistributedStorageAdminService(storageAdmin, metrics))
            .addService(
                new DistributedTransactionService(
                    transactionManager, tableMetadataManager, gateKeeper, metrics))
            .addService(new DistributedTransactionAdminService(transactionAdmin, metrics))
            .addService(new AdminService(gateKeeper))
            .addService(healthService)
            .addService(ProtoReflectionService.newInstance());

    // Two-phase commit for JDBC is not supported for now
    String transactionManagerType =
        config.getProperties().getProperty(DatabaseConfig.TRANSACTION_MANAGER);
    if (Strings.isNullOrEmpty(transactionManagerType) || !transactionManagerType.equals("jdbc")) {
      twoPhaseCommitTransactionManager = transactionFactory.getTwoPhaseCommitTransactionManager();
      builder.addService(
          new TwoPhaseCommitTransactionService(
              twoPhaseCommitTransactionManager, tableMetadataManager, gateKeeper, metrics));
    } else {
      logger.warn(
          "TwoPhaseCommitTransactionService doesn't start when setting \""
              + DatabaseConfig.TRANSACTION_MANAGER
              + "\" to 'jdbc'");
    }

    config.getGrpcMaxInboundMessageSize().ifPresent(builder::maxInboundMessageSize);
    config.getGrpcMaxInboundMetadataSize().ifPresent(builder::maxInboundMetadataSize);

    server = builder.build().start();

    logger.info("ScalarDB Server started, listening on {}", config.getPort());
  }

  public void addShutdownHook() {
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  logger.info("Signal received. Decommissioning ...");
                  decommission();
                  logger.info("Decommissioned. Shutting down");
                  shutdown(MAX_SHUTDOWN_WAIT_TIME_MILLIS, TimeUnit.MILLISECONDS);
                  logger.info("ScalarDB Server shut down");
                }));
  }

  public void decommission() {
    healthService.decommission();
    Uninterruptibles.sleepUninterruptibly(
        config.getDecommissioningDurationSecs(), TimeUnit.SECONDS);
  }

  public void shutdown() {
    if (server != null) {
      server.shutdown();
      blockUntilShutdown();
    }
    close();
  }

  private void blockUntilShutdown() {
    try {
      server.awaitTermination();
    } catch (InterruptedException ignored) {
      // don't need to handle InterruptedException
    }
  }

  public void shutdown(long timeout, TimeUnit unit) {
    if (server != null) {
      server.shutdown();
      blockUntilShutdown(timeout, unit);
    }
    close();
  }

  private void blockUntilShutdown(long timeout, TimeUnit unit) {
    try {
      server.awaitTermination(timeout, unit);
    } catch (InterruptedException ignored) {
      // don't need to handle InterruptedException
    }
  }

  private void close() {
    if (storage != null) {
      storage.close();
    }
    if (storageAdmin != null) {
      storageAdmin.close();
    }
    if (transactionManager != null) {
      transactionManager.close();
    }
    if (transactionAdmin != null) {
      transactionAdmin.close();
    }
    if (twoPhaseCommitTransactionManager != null) {
      twoPhaseCommitTransactionManager.close();
    }
  }

  public static void main(String[] args) {
    int exitCode = new CommandLine(new ScalarDbServer()).execute(args);
    System.exit(exitCode);
  }
}
