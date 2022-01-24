package com.scalar.db.storage.cosmos;

import static com.scalar.db.storage.cosmos.CosmosUtils.quoteKeyword;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.util.CosmosPagedIterable;
import com.google.common.collect.Lists;
import com.scalar.db.api.Get;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Value;
import com.scalar.db.util.ScalarDbUtils;
import com.scalar.db.util.TableMetadataManager;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import org.jooq.Field;
import org.jooq.SQLDialect;
import org.jooq.SelectConditionStep;
import org.jooq.SelectWhereStep;
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;

/**
 * A handler class for select statements
 *
 * @author Yuji Ito
 */
@ThreadSafe
public class SelectStatementHandler extends StatementHandler {
  public SelectStatementHandler(CosmosClient client, TableMetadataManager metadataManager) {
    super(client, metadataManager);
  }

  @Override
  @Nonnull
  protected List<Record> execute(Operation operation) throws CosmosException, ExecutionException {
    TableMetadata tableMetadata = metadataManager.getTableMetadata(operation);
    try {
      if (operation instanceof Get) {
        return executeRead(operation, tableMetadata);
      } else {
        return executeQuery(operation, tableMetadata);
      }
    } catch (CosmosException e) {
      if (e.getStatusCode() == CosmosErrorCode.NOT_FOUND.get()) {
        return Collections.emptyList();
      }
      throw e;
    }
  }

  private List<Record> executeRead(Operation operation, TableMetadata tableMetadata)
      throws CosmosException {
    CosmosOperation cosmosOperation = new CosmosOperation(operation, tableMetadata);
    cosmosOperation.checkArgument(Get.class);

    if (ScalarDbUtils.isSecondaryIndexSpecified(operation, tableMetadata)) {
      return executeReadWithIndex(operation, tableMetadata);
    }

    String id = cosmosOperation.getId();
    PartitionKey partitionKey = cosmosOperation.getCosmosPartitionKey();

    Record record = getContainer(operation).readItem(id, partitionKey, Record.class).getItem();

    return Collections.singletonList(record);
  }

  private List<Record> executeReadWithIndex(Operation operation, TableMetadata tableMetadata)
      throws CosmosException {
    String query = makeQueryWithIndex(operation, tableMetadata);
    CosmosQueryRequestOptions options = new CosmosQueryRequestOptions();
    CosmosPagedIterable<Record> iterable =
        getContainer(operation).queryItems(query, options, Record.class);

    return Lists.newArrayList(iterable);
  }

  private List<Record> executeQuery(Operation operation, TableMetadata tableMetadata)
      throws CosmosException {
    CosmosOperation cosmosOperation = new CosmosOperation(operation, tableMetadata);
    cosmosOperation.checkArgument(Scan.class);
    Scan scan = (Scan) operation;

    String query;
    CosmosQueryRequestOptions options;
    if (ScalarDbUtils.isSecondaryIndexSpecified(scan, tableMetadata)) {
      query = makeQueryWithIndex(scan, tableMetadata);
      options = new CosmosQueryRequestOptions();
    } else {
      String concatenatedPartitionKey = cosmosOperation.getConcatenatedPartitionKey();
      SelectConditionStep<org.jooq.Record> select =
          DSL.using(SQLDialect.DEFAULT)
              .selectFrom("Record r")
              .where(DSL.field("r.concatenatedPartitionKey").eq(concatenatedPartitionKey));

      setStart(select, scan);
      setEnd(select, scan);

      setOrderings(select, scan.getOrderings(), tableMetadata);

      query = select.getSQL(ParamType.INLINED);
      options =
          new CosmosQueryRequestOptions().setPartitionKey(cosmosOperation.getCosmosPartitionKey());
    }

    if (scan.getLimit() > 0) {
      // Add limit as a string
      // because JOOQ doesn't support OFFSET LIMIT clause which Cosmos DB requires
      query += " offset 0 limit " + scan.getLimit();
    }

    CosmosPagedIterable<Record> iterable =
        getContainer(scan).queryItems(query, options, Record.class);

    return Lists.newArrayList(iterable);
  }

  private void setStart(SelectConditionStep<org.jooq.Record> select, Scan scan) {
    scan.getStartClusteringKey()
        .ifPresent(
            k -> {
              ValueBinder binder = new ValueBinder();
              List<Value<?>> start = k.get();
              IntStream.range(0, start.size())
                  .forEach(
                      i -> {
                        Value<?> value = start.get(i);
                        Field<Object> field =
                            DSL.field("r.clusteringKey" + quoteKeyword(value.getName()));
                        if (i == (start.size() - 1)) {
                          if (scan.getStartInclusive()) {
                            binder.set(v -> select.and(field.greaterOrEqual(v)));
                          } else {
                            binder.set(v -> select.and(field.greaterThan(v)));
                          }
                        } else {
                          binder.set(v -> select.and(field.equal(v)));
                        }
                        value.accept(binder);
                      });
            });
  }

  private void setEnd(SelectConditionStep<org.jooq.Record> select, Scan scan) {
    if (!scan.getEndClusteringKey().isPresent()) {
      return;
    }

    scan.getEndClusteringKey()
        .ifPresent(
            k -> {
              ValueBinder binder = new ValueBinder();
              List<Value<?>> end = k.get();
              IntStream.range(0, end.size())
                  .forEach(
                      i -> {
                        Value<?> value = end.get(i);
                        Field<Object> field =
                            DSL.field("r.clusteringKey" + quoteKeyword(value.getName()));
                        if (i == (end.size() - 1)) {
                          if (scan.getEndInclusive()) {
                            binder.set(v -> select.and(field.lessOrEqual(v)));
                          } else {
                            binder.set(v -> select.and(field.lessThan(v)));
                          }
                        } else {
                          binder.set(v -> select.and(field.equal(v)));
                        }
                        value.accept(binder);
                      });
            });
  }

  private void setOrderings(
      SelectConditionStep<org.jooq.Record> select,
      List<Scan.Ordering> scanOrderings,
      TableMetadata tableMetadata) {
    boolean reverse = false;
    if (!scanOrderings.isEmpty()) {
      reverse =
          tableMetadata.getClusteringOrder(scanOrderings.get(0).getName())
              != scanOrderings.get(0).getOrder();
    }

    // For partition key. To use the composite index, we always need to specify ordering for
    // partition key when orderings are set
    Field<Object> partitionKeyField = DSL.field("r.concatenatedPartitionKey");
    select.orderBy(reverse ? partitionKeyField.desc() : partitionKeyField.asc());

    // For clustering keys
    for (String clusteringKeyName : tableMetadata.getClusteringKeyNames()) {
      Field<Object> field = DSL.field("r.clusteringKey" + quoteKeyword(clusteringKeyName));
      select.orderBy(
          tableMetadata.getClusteringOrder(clusteringKeyName) == Order.ASC
              ? (!reverse ? field.asc() : field.desc())
              : (!reverse ? field.desc() : field.asc()));
    }
  }

  private String makeQueryWithIndex(Operation operation, TableMetadata tableMetadata) {
    SelectWhereStep<org.jooq.Record> select = DSL.using(SQLDialect.DEFAULT).selectFrom("Record r");
    Value<?> keyValue = operation.getPartitionKey().get().get(0);
    String fieldName;
    if (tableMetadata.getClusteringKeyNames().contains(keyValue.getName())) {
      fieldName = "r.clusteringKey";
    } else {
      fieldName = "r.values";
    }
    Field<Object> field = DSL.field(fieldName + quoteKeyword(keyValue.getName()));

    ValueBinder binder = new ValueBinder();
    binder.set(v -> select.where(field.eq(v)));
    keyValue.accept(binder);

    return select.getSQL(ParamType.INLINED);
  }
}
