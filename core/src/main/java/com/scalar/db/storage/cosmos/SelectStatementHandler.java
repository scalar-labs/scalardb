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
import com.scalar.db.api.ScanAll;
import com.scalar.db.api.Selection;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Column;
import com.scalar.db.util.ScalarDbUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import org.jooq.Field;
import org.jooq.SQLDialect;
import org.jooq.SelectConditionStep;
import org.jooq.SelectJoinStep;
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
    assert operation instanceof Selection;
    Selection selection = (Selection) operation;
    TableMetadata tableMetadata = metadataManager.getTableMetadata(operation);
    try {
      if (selection instanceof Get) {
        return executeRead((Get) selection, tableMetadata);
      } else {
        return executeQuery((Scan) selection, tableMetadata);
      }
    } catch (CosmosException e) {
      if (e.getStatusCode() == CosmosErrorCode.NOT_FOUND.get()) {
        return Collections.emptyList();
      }
      throw e;
    }
  }

  private List<Record> executeRead(Get get, TableMetadata tableMetadata) throws CosmosException {
    CosmosOperation cosmosOperation = new CosmosOperation(get, tableMetadata);
    cosmosOperation.checkArgument(Get.class);

    if (ScalarDbUtils.isSecondaryIndexSpecified(get, tableMetadata)) {
      return executeReadWithIndex(get, tableMetadata);
    }

    if (get.getProjections().isEmpty()) {
      String id = cosmosOperation.getId();
      PartitionKey partitionKey = cosmosOperation.getCosmosPartitionKey();
      return Collections.singletonList(
          getContainer(get).readItem(id, partitionKey, Record.class).getItem());
    }

    String query =
        makeQueryWithProjections(get, tableMetadata)
            .where(
                DSL.field("r.concatenatedPartitionKey")
                    .eq(cosmosOperation.getConcatenatedPartitionKey()),
                DSL.field("r.id").eq(cosmosOperation.getId()))
            .getSQL(ParamType.INLINED);

    return getContainer(get).queryItems(query, new CosmosQueryRequestOptions(), Record.class)
        .stream()
        .collect(Collectors.toList());
  }

  private List<Record> executeReadWithIndex(Selection selection, TableMetadata tableMetadata)
      throws CosmosException {
    String query = makeQueryWithIndex(selection, tableMetadata);
    CosmosQueryRequestOptions options = new CosmosQueryRequestOptions();
    CosmosPagedIterable<Record> iterable =
        getContainer(selection).queryItems(query, options, Record.class);

    return Lists.newArrayList(iterable);
  }

  private List<Record> executeQuery(Scan scan, TableMetadata tableMetadata) throws CosmosException {
    CosmosOperation cosmosOperation = new CosmosOperation(scan, tableMetadata);
    String query;
    CosmosQueryRequestOptions options;

    if (scan instanceof ScanAll) {
      query = makeQueryWithProjections(scan, tableMetadata).getSQL(ParamType.INLINED);
      options = new CosmosQueryRequestOptions();
    } else if (ScalarDbUtils.isSecondaryIndexSpecified(scan, tableMetadata)) {
      query = makeQueryWithIndex(scan, tableMetadata);
      options = new CosmosQueryRequestOptions();
    } else {
      query = makeQueryWithCondition(tableMetadata, cosmosOperation, scan);
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

  private String makeQueryWithCondition(
      TableMetadata tableMetadata, CosmosOperation cosmosOperation, Scan scan) {
    String concatenatedPartitionKey = cosmosOperation.getConcatenatedPartitionKey();
    SelectConditionStep<org.jooq.Record> select =
        makeQueryWithProjections(scan, tableMetadata)
            .where(DSL.field("r.concatenatedPartitionKey").eq(concatenatedPartitionKey));

    setStart(select, scan);
    setEnd(select, scan);

    setOrderings(select, scan.getOrderings(), tableMetadata);

    return select.getSQL(ParamType.INLINED);
  }

  private SelectJoinStep<org.jooq.Record> makeQueryWithProjections(
      Selection selection, TableMetadata tableMetadata) {
    if (selection.getProjections().isEmpty()) {
      return DSL.using(SQLDialect.DEFAULT).select().from("Record r");
    }

    List<String> projectedFields = new ArrayList<>();

    // To project the required columns, we build a JSON object with the same structure as the
    // `Record.class`so that each field can be deserialized properly into a `Record.class` object.
    // For example, the projected field "r.id" will be mapped to the `Record.id` attribute
    projectedFields.add("r.id");
    projectedFields.add("r.concatenatedPartitionKey");

    if (selection.getProjections().stream()
        .anyMatch(tableMetadata.getPartitionKeyNames()::contains)) {
      projectedFields.add("r.partitionKey");
    }

    if (selection.getProjections().stream()
        .anyMatch(tableMetadata.getClusteringKeyNames()::contains)) {
      projectedFields.add("r.clusteringKey");
    }

    List<String> projectedValues =
        selection.getProjections().stream()
            .filter(
                c ->
                    !tableMetadata.getPartitionKeyNames().contains(c)
                        && !tableMetadata.getClusteringKeyNames().contains(c))
            .map(c -> "\"" + c + "\":r.values" + CosmosUtils.quoteKeyword(c))
            .collect(Collectors.toList());

    if (!projectedValues.isEmpty()) {
      // The following will be mapped to the "Record.values" map attribute
      // For example, to project the columns c1 and c2, the values field will be
      // `{"c1": r["c1"], "c2":r["c2"]} as values`
      // Besides, since the Jooq parser consumes curly brace character as they are treated as
      // placeholder, each curly brace need to be doubled "{{" to have a single curly brace "{"
      // present in the generated sql query
      projectedFields.add("{{" + String.join(",", projectedValues) + "}} as values");
    }

    return DSL.using(SQLDialect.DEFAULT)
        .select(projectedFields.stream().map(DSL::field).collect(Collectors.toList()))
        .from("Record r");
  }

  private void setStart(SelectConditionStep<org.jooq.Record> select, Scan scan) {
    scan.getStartClusteringKey()
        .ifPresent(
            k -> {
              ValueBinder binder = new ValueBinder();
              List<Column<?>> start = k.getColumns();
              IntStream.range(0, start.size())
                  .forEach(
                      i -> {
                        Column<?> column = start.get(i);
                        Field<Object> field =
                            DSL.field("r.clusteringKey" + quoteKeyword(column.getName()));
                        if (i == (start.size() - 1)) {
                          if (scan.getStartInclusive()) {
                            binder.set(v -> select.and(field.greaterOrEqual(v)));
                          } else {
                            binder.set(v -> select.and(field.greaterThan(v)));
                          }
                        } else {
                          binder.set(v -> select.and(field.equal(v)));
                        }
                        column.accept(binder);
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
              List<Column<?>> end = k.getColumns();
              IntStream.range(0, end.size())
                  .forEach(
                      i -> {
                        Column<?> column = end.get(i);
                        Field<Object> field =
                            DSL.field("r.clusteringKey" + quoteKeyword(column.getName()));
                        if (i == (end.size() - 1)) {
                          if (scan.getEndInclusive()) {
                            binder.set(v -> select.and(field.lessOrEqual(v)));
                          } else {
                            binder.set(v -> select.and(field.lessThan(v)));
                          }
                        } else {
                          binder.set(v -> select.and(field.equal(v)));
                        }
                        column.accept(binder);
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
          tableMetadata.getClusteringOrder(scanOrderings.get(0).getColumnName())
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

  private String makeQueryWithIndex(Selection selection, TableMetadata tableMetadata) {
    SelectWhereStep<org.jooq.Record> select = makeQueryWithProjections(selection, tableMetadata);
    Column<?> column = selection.getPartitionKey().getColumns().get(0);
    String fieldName;
    if (tableMetadata.getClusteringKeyNames().contains(column.getName())) {
      fieldName = "r.clusteringKey";
    } else {
      fieldName = "r.values";
    }
    Field<Object> field = DSL.field(fieldName + quoteKeyword(column.getName()));

    ValueBinder binder = new ValueBinder();
    binder.set(v -> select.where(field.eq(v)));
    column.accept(binder);

    return select.getSQL(ParamType.INLINED);
  }
}
