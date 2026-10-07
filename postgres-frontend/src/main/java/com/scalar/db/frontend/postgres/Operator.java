package com.scalar.db.frontend.postgres;

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * A Volcano-style operator. {@link #open} prepares it for one execution, {@link #next} pulls the
 * next row (null once exhausted), and {@link #close} releases its resources. Operators form a tree
 * that the planner builds once per statement and that can be opened repeatedly, for instance once
 * per outer row for a correlated subquery.
 */
public interface Operator extends QueryParser.Cursor {
  /** What an execution provides: the ScalarDB reader and the enclosing query's row, if any. */
  final class Execution {
    final QueryParser.Reader reader;
    @Nullable final Map<String, Object> outer;

    Execution(QueryParser.Reader reader, @Nullable Map<String, Object> outer) {
      this.reader = reader;
      this.outer = outer;
    }
  }

  void open(Execution execution);

  /** One line for EXPLAIN. */
  String describe();

  /** The input operators, for EXPLAIN; a lookup join's right side is read per row, not listed. */
  List<Operator> children();

  /** Rows returned so far over all executions, for EXPLAIN ANALYZE. */
  long rows();

  /** Times this operator was opened, for EXPLAIN ANALYZE. */
  long loops();

  /** ScalarDB reads this operator issued itself (not its inputs), for EXPLAIN ANALYZE. */
  long reads();
}
