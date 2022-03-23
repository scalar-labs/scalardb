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
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.storage.common.AbstractDistributedStorage;
import com.scalar.db.storage.common.checker.OperationChecker;
import com.scalar.db.util.ScalarDbUtils;
import java.net.URI;
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
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * A storage implementation with DynamoDB for {@link DistributedStorage}
 *
 * @author Yuji Ito
 */
@ThreadSafe
public class Dynamo extends AbstractDistributedStorage {
  private static final Logger LOGGER = LoggerFactory.getLogger(Dynamo.class);
  private final DynamoDbClient client;
  private final TableMetadataManager metadataManager;
  private final SelectStatementHandler selectStatementHandler;
  private final PutStatementHandler putStatementHandler;
  private final DeleteStatementHandler deleteStatementHandler;
  private final BatchHandler batchHandler;
  private final OperationChecker operationChecker;

  @Inject
  public Dynamo(DynamoConfig config) {
    DynamoDbClientBuilder builder = DynamoDbClient.builder();
    config.getEndpointOverride().ifPresent(e -> builder.endpointOverride(URI.create(e)));
    client =
        builder
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(
                        config.getUsername().orElse(null), config.getPassword().orElse(null))))
            .region(Region.of(config.getContactPoints().get(0)))
            .build();

    metadataManager =
        new TableMetadataManager(
            new DynamoAdmin(client, config), config.getTableMetadataCacheExpirationTimeSecs());
    operationChecker = new OperationChecker(metadataManager);

    selectStatementHandler = new SelectStatementHandler(client, metadataManager);
    putStatementHandler = new PutStatementHandler(client, metadataManager);
    deleteStatementHandler = new DeleteStatementHandler(client, metadataManager);
    batchHandler = new BatchHandler(client, metadataManager);

    LOGGER.info("DynamoDB object is created properly.");
  }

  @Override
  @Nonnull
  public Optional<Result> get(Get get) throws ExecutionException {
    get = copyAndSetTargetToIfNot(get);
    operationChecker.check(get);
    TableMetadata metadata = metadataManager.getTableMetadata(get);
    ScalarDbUtils.addProjectionsForKeys(get, metadata);

    List<Map<String, AttributeValue>> items = selectStatementHandler.handle(get);
    if (items.size() > 1) {
      throw new IllegalArgumentException("please use scan() for non-exact match selection");
    }
    if (items.isEmpty() || items.get(0) == null) {
      return Optional.empty();
    }

    return Optional.of(
        new ResultInterpreter(get.getProjections(), metadata).interpret(items.get(0)));
  }

  @Override
  public Scanner scan(Scan scan) throws ExecutionException {
    scan = copyAndSetTargetToIfNot(scan);
    operationChecker.check(scan);
    TableMetadata metadata = metadataManager.getTableMetadata(scan);
    ScalarDbUtils.addProjectionsForKeys(scan, metadata);

    List<Map<String, AttributeValue>> items = selectStatementHandler.handle(scan);

    return new ScannerImpl(items, new ResultInterpreter(scan.getProjections(), metadata));
  }

  @Override
  public void put(Put put) throws ExecutionException {
    put = copyAndSetTargetToIfNot(put);
    operationChecker.check(put);

    putStatementHandler.handle(put);
  }

  @Override
  public void put(List<Put> puts) throws ExecutionException {
    mutate(puts);
  }

  @Override
  public void delete(Delete delete) throws ExecutionException {
    delete = copyAndSetTargetToIfNot(delete);
    operationChecker.check(delete);

    deleteStatementHandler.handle(delete);
  }

  @Override
  public void delete(List<Delete> deletes) throws ExecutionException {
    mutate(deletes);
  }

  @Override
  public void mutate(List<? extends Mutation> mutations) throws ExecutionException {
    checkArgument(mutations.size() != 0);
    if (mutations.size() == 1) {
      Mutation mutation = mutations.get(0);
      if (mutation instanceof Put) {
        put((Put) mutation);
      } else if (mutation instanceof Delete) {
        delete((Delete) mutation);
      }
      return;
    }

    mutations = copyAndSetTargetToIfNot(mutations);
    operationChecker.check(mutations);
    for (Mutation mutation : mutations) {
      operationChecker.check(mutation);
    }
    batchHandler.handle(mutations);
  }

  @Override
  public void close() {
    client.close();
  }
}
