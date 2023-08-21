package com.scalar.db.storage.cosmos;

import static com.scalar.db.storage.cosmos.CosmosUtils.quoteKeyword;

import com.azure.cosmos.models.CosmosStoredProcedureRequestOptions;
import com.google.common.annotations.VisibleForTesting;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.MutationCondition;
import com.scalar.db.api.Put;
import com.scalar.db.api.PutIf;
import com.scalar.db.api.PutIfExists;
import com.scalar.db.api.PutIfNotExists;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.Column;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import org.jooq.Field;
import org.jooq.SQLDialect;
import org.jooq.SelectConditionStep;
import org.jooq.impl.DSL;

/** A class to treating utilities for a mutation */
@Immutable
public class CosmosMutation extends CosmosOperation {
  CosmosMutation(Mutation mutation, TableMetadata metadata) {
    super(mutation, metadata);
  }

  @Nonnull
  public MutationType getMutationType() {
    Mutation mutation = (Mutation) getOperation();
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

  @Nonnull
  public Record makeRecord() {
    Mutation mutation = (Mutation) getOperation();

    if (mutation instanceof Delete) {
      return new Record();
    }
    Put put = (Put) mutation;

    return new Record(
        getId(),
        getConcatenatedPartitionKey(),
        toMap(put.getPartitionKey().getColumns()),
        put.getClusteringKey().map(k -> toMap(k.getColumns())).orElse(Collections.emptyMap()),
        toMapForPut(put));
  }

  @Nonnull
  public String makeConditionalQuery() {
    Mutation mutation = (Mutation) getOperation();

    SelectConditionStep<org.jooq.Record> select;
    if (isPrimaryKeySpecified()) {
      String id = getId();
      select = DSL.using(SQLDialect.DEFAULT).selectFrom("Record r").where(DSL.field("r.id").eq(id));
    } else {
      String concatenatedPartitionKey = getConcatenatedPartitionKey();
      select =
          DSL.using(SQLDialect.DEFAULT)
              .selectFrom("Record r")
              .where(DSL.field("r.concatenatedPartitionKey").eq(concatenatedPartitionKey));

      mutation
          .getClusteringKey()
          .ifPresent(
              k -> {
                ValueBinder binder = new ValueBinder();
                k.getColumns()
                    .forEach(
                        c -> {
                          Field<Object> field =
                              DSL.field("r.clusteringKey" + quoteKeyword(c.getName()));
                          binder.set(s -> select.and(field.equal(s)));
                          c.accept(binder);
                        });
              });
    }

    ConditionalQueryBuilder builder = new ConditionalQueryBuilder(select);
    mutation.getCondition().ifPresent(c -> c.accept(builder));

    return builder.getQuery();
  }

  @Nonnull
  public CosmosStoredProcedureRequestOptions getStoredProcedureOptions() {
    return new CosmosStoredProcedureRequestOptions().setPartitionKey(getCosmosPartitionKey());
  }

  private Map<String, Object> toMap(Collection<Column<?>> columns) {
    MapVisitor visitor = new MapVisitor();
    columns.forEach(c -> c.accept(visitor));
    return visitor.get();
  }

  private Map<String, Object> toMapForPut(Put put) {
    MapVisitor visitor = new MapVisitor();
    put.getColumns().values().forEach(c -> c.accept(visitor));
    return visitor.get();
  }

  @VisibleForTesting
  enum MutationType {
    PUT,
    PUT_IF_NOT_EXISTS,
    PUT_IF,
    DELETE_IF,
  }
}
