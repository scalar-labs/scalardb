package com.scalar.db.storage.cosmos;

import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.models.FeedOptions;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.util.CosmosPagedIterable;
import com.google.common.collect.Lists;
import com.scalar.db.api.Get;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Scan;
import com.scalar.db.io.Value;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;

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
    String concatPartitionKey = getConcatPartitionKey(get);
    PartitionKey partitionKey = new PartitionKey(concatPartitionKey);

    Record record = getContainer(get).readItem(id, partitionKey, Record.class).getItem();

    return Arrays.asList(record);
  }

  private List<Record> executeQuery(Operation operation) {
    checkArgument(operation, Scan.class);
    Scan scan = (Scan) operation;

    // TODO: replace StringBuilder with SQL builder
    StringBuilder builder = new StringBuilder();

    String concatPartitionKey = getConcatPartitionKey(scan);
    String where = "r.concatPartitionKey = '" + concatPartitionKey + "'";
    builder.append("SELECT * FROM Record r WHERE " + where);

    setStart(builder, scan);
    setEnd(builder, scan);

    setOrderings(builder, scan.getOrderings());

    if (scan.getLimit() > 0) {
      builder.append(" OFFSET 0 LIMIT " + scan.getLimit());
    }

    String query = new String(builder);
    // TODO: FeedOption will be replaced in the next SDK version
    FeedOptions options = new FeedOptions().setPartitionKey(new PartitionKey(concatPartitionKey));

    CosmosPagedIterable<Record> iterable =
        getContainer(scan).queryItems(query, options, Record.class);

    return Lists.newArrayList(iterable);
  }

  private void setStart(StringBuilder builder, Scan scan) {
    scan.getStartClusteringKey()
        .ifPresent(
            k -> {
              ValueBinder binder = new ValueBinder(builder);
              List<Value> start = k.get();
              IntStream.range(0, start.size())
                  .forEach(
                      i -> {
                        Value value = start.get(i);
                        builder.append(" AND r.clusteringKey." + value.getName());
                        if (i == (start.size() - 1)) {
                          if (scan.getStartInclusive()) {
                            builder.append(" >= ");
                            value.accept(binder);
                          } else {
                            builder.append(" > ");
                            value.accept(binder);
                          }
                        } else {
                          builder.append(" = ");
                          value.accept(binder);
                        }
                      });
            });
  }

  private void setEnd(StringBuilder builder, Scan scan) {
    if (!scan.getEndClusteringKey().isPresent()) {
      return;
    }

    scan.getEndClusteringKey()
        .ifPresent(
            k -> {
              ValueBinder binder = new ValueBinder(builder);
              List<Value> end = k.get();
              IntStream.range(0, end.size())
                  .forEach(
                      i -> {
                        Value value = end.get(i);
                        builder.append(" AND r.clusteringKey." + value.getName());
                        if (i == (end.size() - 1)) {
                          if (scan.getEndInclusive()) {
                            builder.append(" <= ");
                            value.accept(binder);
                          } else {
                            builder.append(" < ");
                            value.accept(binder);
                          }
                        } else {
                          builder.append(" = ");
                          value.accept(binder);
                        }
                      });
            });
  }

  private void setOrderings(StringBuilder builder, List<Scan.Ordering> scanOrderings) {
    if (scanOrderings.isEmpty()) {
      return;
    }
    builder.append(" ORDER BY");

    List<String> orderings = new ArrayList<>();
    scanOrderings.forEach(
        o -> {
          String order = (o.getOrder() == Scan.Ordering.Order.ASC) ? "ASC" : "DESC";
          orderings.add("r.clusteringKey." + o.getName() + " " + order);
        });
    builder.append(" " + String.join(",", orderings));
  }
}
