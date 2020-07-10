package com.scalar.db.storage.cosmos;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.CosmosStoredProcedure;
import com.azure.cosmos.models.CosmosStoredProcedureRequestOptions;
import com.azure.cosmos.models.CosmosStoredProcedureResponse;
import com.azure.cosmos.models.PartitionKey;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.MutationCondition;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.PutIfExists;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.exception.storage.NoMutationException;
import com.scalar.db.exception.storage.RetriableExecutionException;
import com.scalar.db.io.Value;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import org.jooq.SQLDialect;
import org.jooq.SelectConditionStep;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** An abstraction for handler classes for mutate statements */
@ThreadSafe
public abstract class MutateStatementHandler extends StatementHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(MutateStatementHandler.class);
  private final String MUTATION_STORED_PROCEDURE = "mutate.js";

  public MutateStatementHandler(CosmosClient client, TableMetadataManager metadataManager) {
    super(client, metadataManager);
  }

  @Override
  @Nonnull
  public List<Record> handle(Operation operation) throws ExecutionException {
    try {
      List<Record> results = execute(operation);

      return results;
    } catch (CosmosException e) {
      throwException(e);
    }

    return Collections.emptyList();
  }

  protected void executeStoredProcedure(Mutation mutation) throws CosmosException {
    executeStoredProcedure(Arrays.asList(mutation));
  }

  protected void executeStoredProcedure(List<? extends Mutation> mutations) throws CosmosException {
    List<Integer> types = new ArrayList<>();
    List<Record> records = new ArrayList<>();
    List<String> queries = new ArrayList<>();

    mutations.forEach(
        mutation -> {
          types.add(getMutationType(mutation).ordinal());
          records.add(makeRecord(mutation));
          queries.add(makeConditionalQuery(mutation));
        });
    List<Object> args = new ArrayList<>();
    args.add(mutations.size());
    args.addAll(types);
    args.addAll(records);
    args.addAll(queries);

    CosmosStoredProcedureRequestOptions options =
        new CosmosStoredProcedureRequestOptions()
            .setPartitionKey(new PartitionKey(getConcatenatedPartitionKey(mutations.get(0))));

    CosmosStoredProcedure storedProcedure =
        getContainer(mutations.get(0)).getScripts().getStoredProcedure(MUTATION_STORED_PROCEDURE);
    CosmosStoredProcedureResponse response = storedProcedure.execute(args, options);
  }

  private MutationType getMutationType(Mutation mutation) {
    if (!mutation.getCondition().isPresent()) {
      if (mutation instanceof Put) {
        return MutationType.PUT;
      } else {
        return MutationType.DELETE_IF;
      }
    }

    MutationCondition condition = mutation.getCondition().get();
    if (condition instanceof PutIfNotExists) {
      return MutationType.PUT_IF_NOT_EXISTS;
    } else if (condition instanceof PutIfExists || condition instanceof PutIf) {
      return MutationType.PUT_IF;
    } else {
      return MutationType.DELETE_IF;
    }
  }

  protected Record makeRecord(Mutation mutation) {
    Record record = new Record();
    if (mutation instanceof Delete) {
      return record;
    }

    checkArgument(mutation, Put.class);
    Put put = (Put) mutation;

    record.setId(getId(put));
    record.setConcatenatedPartitionKey(getConcatenatedPartitionKey(put));
    record.setPartitionKey(toMap(put.getPartitionKey().get()));
    put.getClusteringKey()
        .ifPresent(
            k -> {
              record.setClusteringKey(toMap(k.get()));
            });
    record.setValues(toMap(put.getValues().values()));

    return record;
  }

  protected String makeConditionalQuery(Mutation mutation) {
    String id = getId(mutation);
    SelectConditionStep<org.jooq.Record> select =
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

  protected void throwException(CosmosException exception) throws ExecutionException {
    LOGGER.error(exception.getMessage());
    int statusCode = exception.getSubStatusCode();

    if (statusCode == CosmosErrorCode.PRECONDITION_FAILED.get()) {
      throw new NoMutationException("no mutation was applied.");
    } else if (statusCode == CosmosErrorCode.RETRY_WITH.get()) {
      throw new RetriableExecutionException(exception.getMessage(), exception);
    }

    throw new ExecutionException(exception.getMessage(), exception);
  }

  private Map<String, Object> toMap(Collection<Value> values) {
    MapVisitor visitor = new MapVisitor();
    values.forEach(v -> v.accept(visitor));

    return visitor.get();
  }

  protected enum MutationType {
    PUT,
    PUT_IF_NOT_EXISTS,
    PUT_IF,
    DELETE_IF,
  }
}
