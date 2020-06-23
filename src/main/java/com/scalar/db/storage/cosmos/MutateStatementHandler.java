package com.scalar.db.storage.cosmos;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.CosmosStoredProcedure;
import com.azure.cosmos.models.CosmosStoredProcedureRequestOptions;
import com.azure.cosmos.models.CosmosStoredProcedureResponse;
import com.azure.cosmos.models.PartitionKey;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.io.Value;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import org.jooq.SQLDialect;
import org.jooq.SelectSelectStep;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An abstraction for handler classes for mutate statements */
@ThreadSafe
public abstract class MutateStatementHandler extends StatementHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(MutateStatementHandler.class);

  public MutateStatementHandler(CosmosClient client, TableMetadataHandler metadataHandler) {
    super(client, metadataHandler);
  }

  @Override
  @Nonnull
  public List<Record> handle(Operation operation) throws ExecutionException {
    try {
      List<Record> results = execute(operation);

      return results;
    } catch (NoMutationException e) {
      throw e;
    } catch (RuntimeException e) {
      LOGGER.error(e.getMessage());
      throw new ExecutionException(e.getMessage(), e);
    }
  }

  protected void executeStoredProcedure(String storedProcedureName, Mutation mutation)
      throws CosmosException, NoMutationException {
    Optional<Record> record = makeRecord(mutation);
    String query = makeConditionalQuery(mutation);
    List<Object> args =
        record.isPresent() ? Arrays.asList(record.get(), query) : Arrays.asList(query);

    CosmosStoredProcedureRequestOptions options =
        new CosmosStoredProcedureRequestOptions()
            .setPartitionKey(new PartitionKey(getConcatenatedPartitionKey(mutation)));

    CosmosStoredProcedure storedProcedure =
        getContainer(mutation).getScripts().getStoredProcedure(storedProcedureName);
    CosmosStoredProcedureResponse response = storedProcedure.execute(args, options);

    if (!response.getResponseAsString().equals("true")) {
      throw new NoMutationException("no mutation was applied.");
    }
  }

  protected Optional<Record> makeRecord(Mutation mutation) {
    if (mutation instanceof Delete) {
      return Optional.empty();
    }
    checkArgument(mutation, Put.class);
    Put put = (Put) mutation;

    Record record = new Record();
    record.setId(getId(put));
    record.setConcatenatedPartitionKey(getConcatenatedPartitionKey(put));

    MapVisitor partitionKeyVisitor = new MapVisitor();
    for (Value v : put.getPartitionKey()) {
      v.accept(partitionKeyVisitor);
    }
    record.setPartitionKey(toMap(put.getPartitionKey().get()));

    put.getClusteringKey()
        .ifPresent(
            k -> {
              record.setClusteringKey(toMap(k.get()));
            });

    record.setValues(toMap(put.getValues().values()));
    MapVisitor visitor = new MapVisitor();
    put.getValues()
        .values()
        .forEach(
            v -> {
              v.accept(visitor);
            });
    record.setValues(visitor.get());

    return Optional.of(record);
  }

  protected String makeConditionalQuery(Mutation mutation) {
    String id = getId(mutation);
    SelectSelectStep select =
        (SelectSelectStep)
            DSL.using(SQLDialect.DEFAULT).selectFrom("Record r").where(DSL.field("r.id").eq(id));

    ConditionalQueryBuilder builder = new ConditionalQueryBuilder(select);
    mutation
        .getCondition()
        .ifPresent(
            c -> {
              c.accept(builder);
            });

    return builder.getQuery();
  }

  private Map<String, Object> toMap(Collection<Value> values) {
    MapVisitor visitor = new MapVisitor();
    values.forEach(v -> v.accept(visitor));

    return visitor.get();
  }
}
