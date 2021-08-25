package com.scalar.db.storage.dynamo;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.scalar.db.api.DistributedStorageAdmin;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.StorageRuntimeException;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.concurrent.ThreadSafe;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;

@ThreadSafe
public class DynamoAdmin implements DistributedStorageAdmin {

  private final DynamoDbClient client;
  private final Optional<String> namespacePrefix;
  private final DynamoTableMetadataManager metadataManager;

  @Inject
  public DynamoAdmin(DynamoConfig config) {
    DynamoDbClientBuilder builder = DynamoDbClient.builder();
    config.getEndpointOverride().ifPresent(e -> builder.endpointOverride(URI.create(e)));
    this.client =
        builder
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(
                        config.getUsername().orElse(null), config.getPassword().orElse(null))))
            .region(Region.of(config.getContactPoints().get(0)))
            .build();
    namespacePrefix = config.getNamespacePrefix();
    metadataManager = new DynamoTableMetadataManager(client, namespacePrefix);
  }

  @VisibleForTesting
  DynamoAdmin(DynamoTableMetadataManager metadataManager, Optional<String> namespacePrefix) {
    client = null;
    this.metadataManager = metadataManager;
    this.namespacePrefix = namespacePrefix.map(n -> n + "_");
  }

  @Override
  public void createNamespace(String namespace, Map<String, String> options)
      throws ExecutionException {
    // TODO To implement
    throw new UnsupportedOperationException("implement later");
  }

  @Override
  public void createTable(
      String namespace, String table, TableMetadata metadata, Map<String, String> options)
      throws ExecutionException {
    throw new UnsupportedOperationException("implement later");
  }

  @Override
  public void dropTable(String namespace, String table) throws ExecutionException {
    throw new UnsupportedOperationException("implement later");
  }

  @Override
  public void dropNamespace(String namespace) throws ExecutionException {
    // TODO To implement
    throw new UnsupportedOperationException("implement later");
  }

  @Override
  public void truncateTable(String namespace, String table) throws ExecutionException {
    throw new UnsupportedOperationException("implement later");
  }

  @Override
  public TableMetadata getTableMetadata(String namespace, String table) throws ExecutionException {
    try {
      return metadataManager.getTableMetadata(fullNamespace(namespace), table);
    } catch (StorageRuntimeException e) {
      throw new ExecutionException("getting a table metadata failed", e);
    }
  }

  @Override
  public Set<String> getNamespaceTableNames(String namespace) throws ExecutionException {
    // TODO To implement
    throw new UnsupportedOperationException("implement later");
  }

  @Override
  public boolean namespaceExists(String namespace) throws ExecutionException {
    // TODO To implement
    throw new UnsupportedOperationException("implement later");
  }

  private String fullNamespace(String namespace) {
    return namespacePrefix.map(s -> s + namespace).orElse(namespace);
  }

  @Override
  public void close() {
    client.close();
  }
}
