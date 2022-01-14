package com.scalar.db.graphql.server;

import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.service.StorageFactory;
import java.io.File;
import java.util.concurrent.Callable;
import org.eclipse.jetty.server.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "scalardb-graphql", description = "Starts Scalar DB GraphQL server.")
public class GraphQlServer implements Callable<Integer> {
  private static final Logger LOGGER = LoggerFactory.getLogger(GraphQlServer.class);

  @CommandLine.Option(
      names = {"--properties", "--config"},
      required = true,
      paramLabel = "PROPERTIES_FILE",
      description = "A configuration file in properties format.")
  private String configFile;

  private ServerConfig config;

  public GraphQlServer() {}

  public GraphQlServer(ServerConfig config) {
    this.config = config;
  }

  public static void main(String[] args) {
    int exitCode = new CommandLine(new GraphQlServer()).execute(args);
    System.exit(exitCode);
  }

  @Override
  public Integer call() throws Exception {
    if (configFile != null) {
      config = new ServerConfig(new File(configFile));
    }

    DatabaseConfig databaseConfig = new DatabaseConfig(config.getProperties());
    GraphQlHandler.Builder handlerBuilder =
        GraphQlHandler.newBuilder()
            .path(config.getPath())
            .databaseConfig(databaseConfig)
            .setGraphiqlEnabled(config.getGraphiql());
    DistributedStorageAdmin storageAdmin = new StorageFactory(databaseConfig).getAdmin();
    for (String namespace : config.getNamespaces()) {
      for (String table : storageAdmin.getNamespaceTableNames(namespace)) {
        handlerBuilder.table(namespace, table);
      }
    }
    storageAdmin.close();

    Server server = new Server(config.getPort());
    server.setHandler(handlerBuilder.build());
    server.start();

    LOGGER.info("Scalar DB GraphQL server started, listening on " + config.getPort());

    server.join();

    return 0;
  }
}
