package com.scalar.db.graphql.server;

import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.service.StorageFactory;
import java.io.FileInputStream;
import java.util.Properties;
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

  public static void main(String[] args) {
    int exitCode = new CommandLine(new GraphQlServer()).execute(args);
    System.exit(exitCode);
  }

  @Override
  public Integer call() throws Exception {
    if (configFile != null) {
      Properties properties = new Properties();
      try (FileInputStream fis = new FileInputStream(configFile)) {
        properties.load(fis);
      }
      config = new ServerConfig(properties);
    }

    DatabaseConfig databaseConfig = new DatabaseConfig(config.getProperties());
    DistributedStorageAdmin storageAdmin = new StorageFactory(databaseConfig).getAdmin();
    GraphQlHandler.Builder handlerBuilder =
        GraphQlHandler.newBuilder()
            .path(config.getPath())
            .databaseConfig(databaseConfig)
            .setGraphiqlEnabled(config.getGraphiql());
    for (String namespace : config.getNamespaces()) {
      for (String table : storageAdmin.getNamespaceTableNames(namespace)) {
        handlerBuilder.table(namespace, table);
      }
    }

    Server server = new Server(config.getPort());
    server.setHandler(handlerBuilder.build());
    server.start();
    server.join();

    return 0;
  }
}
