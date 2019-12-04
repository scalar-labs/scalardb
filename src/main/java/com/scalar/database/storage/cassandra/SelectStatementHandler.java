package com.scalar.database.storage.cassandra;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.select.SelectFrom;
import com.scalar.database.api.Get;
import com.scalar.database.api.Operation;
import com.scalar.database.api.Scan;
import com.scalar.database.api.Selection;
import com.scalar.database.io.Key;
import com.scalar.database.io.Value;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A handler class for select statement
 *
 * @author Hiroyuki Yamada, Yuji Ito
 */
@ThreadSafe
public class SelectStatementHandler extends StatementHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(SelectStatementHandler.class);

  /**
   * Constructs {@code SelectStatementHandler} with the specified {@code CqlSession}
   *
   * @param session session to be used with this statement
   */
  public SelectStatementHandler(CqlSession session) {
    super(session);
  }

  @Override
  @Nonnull
  protected PreparedStatement prepare(Operation operation) {
    checkArgument(operation, Get.class, Scan.class);
    Select select = null;

    if (operation instanceof Get) {
      select = prepare((Get) operation);
    } else {
      select = prepare((Scan) operation);
    }

    return prepare(select.asCql());
  }

  @Override
  @Nonnull
  protected BoundStatementBuilder bind(PreparedStatement prepared, Operation operation) {
    checkArgument(operation, Get.class, Scan.class);
    BoundStatementBuilder builder = prepared.boundStatementBuilder();

    if (operation instanceof Get) {
      bind(builder, (Get) operation);
    } else {
      bind(builder, (Scan) operation);
    }

    return builder;
  }

  @Override
  @Nonnull
  protected ResultSet execute(BoundStatement bound, Operation operation) {
    return session.execute(bound);
  }

  @Override
  protected void overwriteConsistency(BoundStatementBuilder builder, Operation operation) {
    // nothing to overwrite
  }

  private Select prepare(Get get) {
    Select select = getSelect(get);
    return setPredicates(select, get);
  }

  private Select prepare(Scan scan) {
    Select select = getSelect(scan);
    select = createStatement(select, scan);

    Map<String, ClusteringOrder> orderings = getOrderings(scan.getOrderings());
    if (!orderings.isEmpty()) {
      select = select.orderBy(orderings);
    }

    if (scan.getLimit() > 0) {
      select = select.limit(scan.getLimit());
    }

    return select;
  }

  private Select getSelect(Selection sel) {
    SelectFrom selectFrom = selectFrom(sel.forNamespace().get(), sel.forTable().get());
    return setProjections(selectFrom, sel.getProjections());
  }

  private Select setProjections(SelectFrom selectFrom, List<String> projections) {
    if (projections.isEmpty()) {
      return selectFrom.all();
    } else {
      return selectFrom.columns(projections);
    }
  }

  private Select setPredicates(Select select, Get get) {
    Select selectWithPartitionKeys = setKey(select, Optional.of(get.getPartitionKey()));
    return setKey(selectWithPartitionKeys, get.getClusteringKey());
  }

  private Select createStatement(Select select, Scan scan) {
    Select selectWithPartitionKeys = setKey(select, Optional.of(scan.getPartitionKey()));
    Select selectWithStart = setStart(selectWithPartitionKeys, scan);
    return setEnd(selectWithStart, scan);
  }

  private Select setKey(Select select, Optional<Key> key) {
    List<Relation> relations = new ArrayList<>();
    key.ifPresent(
        k -> {
          k.forEach(v -> relations.add(Relation.column(v.getName()).isEqualTo(bindMarker())));
        });

    return select.where(relations);
  }

  private Select setStart(Select select, Scan scan) {
    List<Relation> relations = new ArrayList<>();

    if (!scan.getStartClusteringKey().isPresent()) {
      return select;
    }

    scan.getStartClusteringKey()
        .ifPresent(
            k -> {
              List<Value> start = k.get();
              IntStream.range(0, start.size())
                  .forEach(
                      i -> {
                        if (i == (start.size() - 1)) {
                          if (scan.getStartInclusive()) {
                            relations.add(
                                Relation.column(start.get(i).getName())
                                    .isGreaterThanOrEqualTo(bindMarker()));
                          } else {
                            relations.add(
                                Relation.column(start.get(i).getName())
                                    .isGreaterThan(bindMarker()));
                          }
                        } else {
                          relations.add(
                              Relation.column(start.get(i).getName()).isEqualTo(bindMarker()));
                        }
                      });
            });

    return select.where(relations);
  }

  private Select setEnd(Select select, Scan scan) {
    List<Relation> relations = new ArrayList<Relation>();

    if (!scan.getEndClusteringKey().isPresent()) {
      return select;
    }

    scan.getEndClusteringKey()
        .ifPresent(
            k -> {
              List<Value> end = k.get();
              IntStream.range(0, end.size())
                  .forEach(
                      i -> {
                        if (i == (end.size() - 1)) {
                          if (scan.getEndInclusive()) {
                            relations.add(
                                Relation.column(end.get(i).getName())
                                    .isLessThanOrEqualTo(bindMarker()));
                          } else {
                            relations.add(
                                Relation.column(end.get(i).getName()).isLessThan(bindMarker()));
                          }
                        } else {
                          relations.add(
                              Relation.column(end.get(i).getName()).isEqualTo(bindMarker()));
                        }
                      });
            });

    return select.where(relations);
  }

  private void bind(BoundStatementBuilder builder, Get get) {
    ValueBinder binder = new ValueBinder(builder);

    // bind in the prepared order
    get.getPartitionKey().forEach(v -> v.accept(binder));
    get.getClusteringKey().ifPresent(k -> k.forEach(v -> v.accept(binder)));
  }

  private void bind(BoundStatementBuilder builder, Scan scan) {
    ValueBinder binder = new ValueBinder(builder);

    // bind in the prepared order
    scan.getPartitionKey().forEach(v -> v.accept(binder));
    scan.getStartClusteringKey().ifPresent(k -> k.forEach(v -> v.accept(binder)));
    scan.getEndClusteringKey().ifPresent(k -> k.forEach(v -> v.accept(binder)));
  }

  private ClusteringOrder getOrdering(Scan.Ordering ordering) {
    switch (ordering.getOrder()) {
      case ASC:
        return ClusteringOrder.ASC;
      case DESC:
        return ClusteringOrder.DESC;
      default:
        LOGGER.warn("Unsupported ordering specified. Using Order.ASC.");
        return ClusteringOrder.ASC;
    }
  }

  private Map<String, ClusteringOrder> getOrderings(List<Scan.Ordering> scanOrderings) {
    Map<String, ClusteringOrder> orderings = new LinkedHashMap<>(scanOrderings.size());
    scanOrderings.forEach(
        o -> {
          orderings.put(o.getName(), getOrdering(o));
        });

    return orderings;
  }
}
