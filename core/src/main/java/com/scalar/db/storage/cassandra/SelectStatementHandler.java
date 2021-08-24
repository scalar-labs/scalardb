package com.scalar.db.storage.cassandra;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Ordering;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.scalar.db.api.Get;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Selection;
import com.scalar.db.io.Key;
import com.scalar.db.io.Value;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A handler class for select statement
 *
 * @author Hiroyuki Yamada
 */
@ThreadSafe
public class SelectStatementHandler extends StatementHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(SelectStatementHandler.class);

  /**
   * Constructs {@code SelectStatementHandler} with the specified {@code Session}
   *
   * @param session session to be used with this statement
   */
  public SelectStatementHandler(Session session) {
    super(session);
  }

  @Override
  @Nonnull
  protected PreparedStatement prepare(Operation operation) {
    checkArgument(operation, Get.class, Scan.class);
    Select select;

    if (operation instanceof Get) {
      select = prepare((Get) operation);
    } else {
      select = prepare((Scan) operation);
    }

    return prepare(select.getQueryString());
  }

  @Override
  @Nonnull
  protected BoundStatement bind(PreparedStatement prepared, Operation operation) {
    checkArgument(operation, Get.class, Scan.class);
    if (operation instanceof Get) {
      return bind(prepared.bind(), (Get) operation);
    } else {
      return bind(prepared.bind(), (Scan) operation);
    }
  }

  @Override
  @Nonnull
  protected ResultSet execute(BoundStatement bound, Operation operation) {
    return session.execute(bound);
  }

  @Override
  protected void overwriteConsistency(BoundStatement bound, Operation operation) {
    // nothing to overwrite
  }

  private Select prepare(Get get) {
    Select select = getSelect(get);
    setPredicates(select.where(), get);

    return select;
  }

  private Select prepare(Scan scan) {
    Select select = getSelect(scan);
    createStatement(select.where(), scan);

    List<Ordering> orderings = getOrderings(scan.getOrderings());
    if (!orderings.isEmpty()) {
      select.orderBy(orderings.toArray(new Ordering[0]));
    }

    if (scan.getLimit() > 0) {
      select.limit(scan.getLimit());
    }

    return select;
  }

  private Select getSelect(Selection sel) {
    Select.Selection selection = select();

    setProjections(selection, sel.getProjections());
    return selection.from(sel.forFullNamespace().get(), sel.forTable().get());
  }

  private void setProjections(Select.Selection selection, List<String> projections) {
    if (projections.isEmpty()) {
      selection.all();
    } else {
      projections.forEach(selection::column);
    }
  }

  private void setPredicates(Select.Where statement, Get get) {
    setKey(statement, Optional.of(get.getPartitionKey()));
    setKey(statement, get.getClusteringKey());
  }

  private void createStatement(Select.Where statement, Scan scan) {
    setKey(statement, Optional.of(scan.getPartitionKey()));
    setStart(statement, scan);
    setEnd(statement, scan);
  }

  private void setKey(Select.Where statement, Optional<Key> key) {
    key.ifPresent(k -> k.forEach(v -> statement.and(eq(v.getName(), bindMarker()))));
  }

  private void setStart(Select.Where statement, Scan scan) {
    if (!scan.getStartClusteringKey().isPresent()) {
      return;
    }

    scan.getStartClusteringKey()
        .ifPresent(
            k -> {
              List<Value<?>> start = k.get();
              IntStream.range(0, start.size())
                  .forEach(
                      i -> {
                        if (i == (start.size() - 1)) {
                          if (scan.getStartInclusive()) {
                            statement.and(gte(start.get(i).getName(), bindMarker()));
                          } else {
                            statement.and(gt(start.get(i).getName(), bindMarker()));
                          }
                        } else {
                          statement.and(eq(start.get(i).getName(), bindMarker()));
                        }
                      });
            });
  }

  private void setEnd(Select.Where statement, Scan scan) {
    if (!scan.getEndClusteringKey().isPresent()) {
      return;
    }

    scan.getEndClusteringKey()
        .ifPresent(
            k -> {
              List<Value<?>> end = k.get();
              IntStream.range(0, end.size())
                  .forEach(
                      i -> {
                        if (i == (end.size() - 1)) {
                          if (scan.getEndInclusive()) {
                            statement.and(lte(end.get(i).getName(), bindMarker()));
                          } else {
                            statement.and(lt(end.get(i).getName(), bindMarker()));
                          }
                        } else {
                          statement.and(eq(end.get(i).getName(), bindMarker()));
                        }
                      });
            });
  }

  private BoundStatement bind(BoundStatement bound, Get get) {
    ValueBinder binder = new ValueBinder(bound);

    // bind in the prepared order
    get.getPartitionKey().forEach(v -> v.accept(binder));
    get.getClusteringKey().ifPresent(k -> k.forEach(v -> v.accept(binder)));

    return bound;
  }

  private BoundStatement bind(BoundStatement bound, Scan scan) {
    ValueBinder binder = new ValueBinder(bound);

    // bind in the prepared order
    scan.getPartitionKey().forEach(v -> v.accept(binder));
    scan.getStartClusteringKey().ifPresent(k -> k.forEach(v -> v.accept(binder)));
    scan.getEndClusteringKey().ifPresent(k -> k.forEach(v -> v.accept(binder)));

    return bound;
  }

  private Ordering getOrdering(Scan.Ordering ordering) {
    switch (ordering.getOrder()) {
      case ASC:
        return QueryBuilder.asc(ordering.getName());
      case DESC:
        return QueryBuilder.desc(ordering.getName());
      default:
        LOGGER.warn("Unsupported ordering specified. Using Order.ASC.");
        return QueryBuilder.asc(ordering.getName());
    }
  }

  private List<Ordering> getOrderings(List<Scan.Ordering> scanOrderings) {
    List<Ordering> orderings = new ArrayList<>(scanOrderings.size());
    scanOrderings.forEach(o -> orderings.add(getOrdering(o)));
    return orderings;
  }
}
