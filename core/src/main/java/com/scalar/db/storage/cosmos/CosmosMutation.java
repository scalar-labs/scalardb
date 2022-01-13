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
import com.scalar.db.io.Value;
import java.util.Collection;
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
    Record record = new Record();

    if (mutation instanceof Delete) {
      return record;
    }
    Put put = (Put) mutation;

    record.setId(getId());
    record.setConcatenatedPartitionKey(getConcatenatedPartitionKey());
    record.setPartitionKey(toMap(put.getPartitionKey().get()));
    put.getClusteringKey().ifPresent(k -> record.setClusteringKey(toMap(k.get())));
    record.setValues(toMap(put.getValues().values()));

    return record;
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
                k.get()
                    .forEach(
                        v -> {
                          Field<Object> field =
                              DSL.field("r.clusteringKey" + quoteKeyword(v.getName()));
                          binder.set(s -> select.and(field.equal(s)));
                          v.accept(binder);
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

  private Map<String, Object> toMap(Collection<Value<?>> values) {
    MapVisitor visitor = new MapVisitor();
    values.forEach(v -> v.accept(visitor));

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
