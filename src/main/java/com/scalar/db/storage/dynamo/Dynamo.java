package com.scalar.db.storage.dynamo;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.inject.Inject;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedStorage;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scanner;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.InvalidUsageException;
import com.scalar.db.storage.Utility;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * A storage implementation with DynamoDB for {@link DistributedStorage}
 *
 * @author Yuji Ito
 */
@ThreadSafe
public class Dynamo implements DistributedStorage {
  private static final Logger LOGGER = LoggerFactory.getLogger(Dynamo.class);
  private final DynamoDbClient client;
  private final TableMetadataManager metadataManager;
  private final SelectStatementHandler selectStatementHandler;
  private final PutStatementHandler putStatementHandler;
  private final DeleteStatementHandler deleteStatementHandler;
  private final BatchHandler batchHandler;
  private Optional<String> namespacePrefix;
  private Optional<String> namespace;
  private Optional<String> tableName;

  @Inject
  public Dynamo(DatabaseConfig config) {
    this.client =
        DynamoDbClient.builder()
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(config.getUsername(), config.getPassword())))
            .region(Region.of(config.getContactPoints().get(0)))
            .build();

    namespacePrefix = config.getNamespacePrefix();
    namespace = Optional.empty();
    tableName = Optional.empty();

    this.metadataManager = new TableMetadataManager(client, namespacePrefix);

    this.selectStatementHandler = new SelectStatementHandler(client, metadataManager);
    this.putStatementHandler = new PutStatementHandler(client, metadataManager);
    this.deleteStatementHandler = new DeleteStatementHandler(client, metadataManager);
    this.batchHandler = new BatchHandler(client, metadataManager);

    LOGGER.info("DynamoDB object is created properly.");
  }

  @Override
  public void with(String namespace, String tableName) {
    this.namespace = Optional.ofNullable(namespace);
    this.tableName = Optional.ofNullable(tableName);
  }

  @Override
  public void withNamespace(String namespace) {
    this.namespace = Optional.ofNullable(namespace);
  }

  @Override
  public Optional<String> getNamespace() {
    return namespace;
  }

  @Override
  public void withTable(String tableName) {
    this.tableName = Optional.ofNullable(tableName);
  }

  @Override
  public Optional<String> getTable() {
    return tableName;
  }

  @Override
  @Nonnull
  public Optional<Result> get(Get get) throws ExecutionException {
    Utility.setTargetToIfNot(get, namespacePrefix, namespace, tableName);
    DynamoTableMetadata metadata = metadataManager.getTableMetadata(get);
    Utility.checkGetOperation(get, metadata);

    List<Map<String, AttributeValue>> items = selectStatementHandler.handle(get);
    if (items.size() > 1) {
      throw new InvalidUsageException("please use scan() for non-exact match selection");
    }
    if (items.isEmpty() || items.get(0) == null) {
      return Optional.empty();
    }

    return Optional.of(new ResultImpl(items.get(0), get, metadata));
  }

  @Override
  public Scanner scan(Scan scan) throws ExecutionException {
    Utility.setTargetToIfNot(scan, namespacePrefix, namespace, tableName);
    DynamoTableMetadata metadata = metadataManager.getTableMetadata(scan);
    Utility.checkScanOperation(scan, metadata);

    List<Map<String, AttributeValue>> items = selectStatementHandler.handle(scan);

    return new ScannerImpl(items, scan, metadata);
  }

  @Override
  public void put(Put put) throws ExecutionException {
    Utility.setTargetToIfNot(put, namespacePrefix, namespace, tableName);
    checkIfPrimaryKeyExists(put);

    putStatementHandler.handle(put);
  }

  @Override
  public void put(List<Put> puts) throws ExecutionException {
    mutate(puts);
  }

  @Override
  public void delete(Delete delete) throws ExecutionException {
    Utility.setTargetToIfNot(delete, namespacePrefix, namespace, tableName);
    checkIfPrimaryKeyExists(delete);

    deleteStatementHandler.handle(delete);
  }

  @Override
  public void delete(List<Delete> deletes) throws ExecutionException {
    mutate(deletes);
  }

  @Override
  public void mutate(List<? extends Mutation> mutations) throws ExecutionException {
    checkArgument(mutations.size() != 0);
    if (mutations.size() > 1) {
      Utility.setTargetToIfNot(mutations, namespacePrefix, namespace, tableName);
      batchHandler.handle(mutations);
    } else if (mutations.size() == 1) {
      Mutation mutation = mutations.get(0);
      if (mutation instanceof Put) {
        put((Put) mutation);
      } else if (mutation instanceof Delete) {
        delete((Delete) mutation);
      }
    }
  }

  @Override
  public void close() {
    client.close();
  }

  private void checkIfPrimaryKeyExists(Mutation mutation) {
    DynamoTableMetadata metadata = metadataManager.getTableMetadata(mutation);

    Utility.checkIfPrimaryKeyExists(mutation, metadata);
  }
}
