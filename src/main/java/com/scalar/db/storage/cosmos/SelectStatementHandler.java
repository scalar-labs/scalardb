package com.scalar.db.storage.cosmos;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.util.CosmosPagedIterable;
import com.google.common.collect.Lists;
import com.scalar.db.api.Get;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Scan;
import com.scalar.db.io.Value;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;
import org.jooq.Field;
import org.jooq.OrderField;
import org.jooq.SQLDialect;
import org.jooq.SelectSelectStep;
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;

public class SelectStatementHandler extends StatementHandler {
  public SelectStatementHandler(CosmosClient client, TableMetadataHandler metadataHandler) {
    super(client, metadataHandler);
  }

  @Override
  @Nonnull
  protected List<Record> execute(Operation operation) {
    if (operation instanceof Get) {
      return executeRead(operation);
    } else {
      return executeQuery(operation);
    }
  }

  private List<Record> executeRead(Operation operation) {
    checkArgument(operation, Get.class);
    Get get = (Get) operation;

    String id = getId(get);
    String concatenatedPartitionKey = getConcatenatedPartitionKey(get);
    PartitionKey partitionKey = new PartitionKey(concatenatedPartitionKey);

    Record record = getContainer(get).readItem(id, partitionKey, Record.class).getItem();

    return Arrays.asList(record);
  }

  private List<Record> executeQuery(Operation operation) {
    checkArgument(operation, Scan.class);
    Scan scan = (Scan) operation;

    String concatenatedPartitionKey = getConcatenatedPartitionKey(scan);
    SelectSelectStep select =
        (SelectSelectStep)
            DSL.using(SQLDialect.DEFAULT)
                .selectFrom("Record r")
                .where(DSL.field("r.concatenatedPartitionKey").eq(concatenatedPartitionKey));

    setStart(select, scan);
    setEnd(select, scan);

    setOrderings(select, scan.getOrderings());

    String query = select.getSQL(ParamType.INLINED);
    if (scan.getLimit() > 0) {
      // Add limit as a string
      // because JOOQ doesn't support OFFSET LIMIT clause which Cosmos DB requires
      query += " offset 0 limit " + scan.getLimit();
    }

    CosmosQueryRequestOptions options =
        new CosmosQueryRequestOptions().setPartitionKey(new PartitionKey(concatenatedPartitionKey));

    CosmosPagedIterable<Record> iterable =
        getContainer(scan).queryItems(query, options, Record.class);

    return Lists.newArrayList(iterable);
  }

  private void setStart(SelectSelectStep select, Scan scan) {
    scan.getStartClusteringKey()
        .ifPresent(
            k -> {
              ValueBinder binder = new ValueBinder();
              List<Value> start = k.get();
              IntStream.range(0, start.size())
                  .forEach(
                      i -> {
                        Value value = start.get(i);
                        Field field = DSL.field("r.clusteringKey." + value.getName());
                        if (i == (start.size() - 1)) {
                          if (scan.getStartInclusive()) {
                            binder.set(v -> select.where(field.greaterOrEqual(v)));
                          } else {
                            binder.set(v -> select.where(field.greaterThan(v)));
                          }
                        } else {
                          binder.set(v -> select.where(field.equal(v)));
                        }
                        value.accept(binder);
                      });
            });
  }

  private void setEnd(SelectSelectStep select, Scan scan) {
    if (!scan.getEndClusteringKey().isPresent()) {
      return;
    }

    scan.getEndClusteringKey()
        .ifPresent(
            k -> {
              ValueBinder binder = new ValueBinder();
              List<Value> end = k.get();
              IntStream.range(0, end.size())
                  .forEach(
                      i -> {
                        Value value = end.get(i);
                        Field field = DSL.field("r.clusteringKey." + value.getName());
                        if (i == (end.size() - 1)) {
                          if (scan.getEndInclusive()) {
                            binder.set(v -> select.where(field.lessOrEqual(v)));
                          } else {
                            binder.set(v -> select.where(field.lessThan(v)));
                          }
                        } else {
                          binder.set(v -> select.where(field.equal(v)));
                        }
                        value.accept(binder);
                      });
            });
  }

  private void setOrderings(SelectSelectStep select, List<Scan.Ordering> scanOrderings) {
    if (scanOrderings.isEmpty()) {
      return;
    }

    scanOrderings.forEach(
        o -> {
          Field field = DSL.field("r.clusteringKey." + o.getName());
          OrderField orderField =
              (o.getOrder() == Scan.Ordering.Order.ASC) ? field.asc() : field.desc();
          select.orderBy(orderField);
        });
  }
}
