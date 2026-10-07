package com.scalar.db.frontend.postgres;

import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.io.DataType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.Select;

/**
 * A SELECT after binding and before any physical decision: its FROM items in order, how each joins
 * the ones before it, and its clauses as bound expressions (every column qualified with its
 * source). The {@link Optimizer} rewrites it in place: it moves conjuncts out of {@link #where} and
 * {@link Join#on} into ScalarDB conditions, keyed lookups, and hash-join keys. The physical planner
 * then turns it into a {@link QueryParser.Plan}.
 */
final class LogicalPlan {
  /** A FROM item: a ScalarDB table, a catalog table, a table function, or a subquery. */
  static final class Source {
    final String qualifier;
    final List<String> columns;
    @Nullable final QueryParser.Target target; // a ScalarDB table
    @Nullable final QueryParser.Plan derived; // a subquery in FROM
    boolean lateral; // LATERAL: the subquery may reference the sources before it, runs per row
    @Nullable String catalogTable; // a catalog table or table function, with its rows
    @Nullable List<Map<String, Object>> catalogRows;
    // USING/NATURAL columns of this source, mapped to the qualifier of the earlier source they are
    // merged with: SELECT * outputs them once, and an unqualified reference binds to that source
    final Map<String, String> merged = new HashMap<>();
    boolean star; // SELECT * needs every column
    final Set<String> projections = new LinkedHashSet<>(); // the columns referenced
    // Set by the optimizer
    final List<ConditionalExpression> pushed = new ArrayList<>(); // ScalarDB conditions (AND)
    // Each entry is one OR pushed to ScalarDB: a list of alternatives, each a list of conditions
    // that must all hold. The read's condition is pushed AND every entry, expanded to OR-of-ANDs.
    final List<List<List<ConditionalExpression>>> pushedOr = new ArrayList<>();
    // Conditions with placeholders, pushed to ScalarDB when the plan is opened with their values
    final List<Expression> pushedLate = new ArrayList<>();
    @Nullable Map<String, Expression> lookupParams; // key columns bound to outer expressions

    Source(
        String qualifier,
        List<String> columns,
        @Nullable QueryParser.Target target,
        @Nullable QueryParser.Plan derived) {
      this.qualifier = qualifier;
      this.columns = columns;
      this.target = target;
      this.derived = derived;
    }

    boolean isTable() {
      return target != null;
    }
  }

  /** How a source joins the sources before it. */
  static final class Join {
    enum Kind {
      INNER,
      LEFT,
      RIGHT,
      FULL
    }

    final Kind kind;
    final List<Expression> on = new ArrayList<>(); // ON conjuncts not consumed by a rule
    // Set by the optimizer: equalities between the earlier sources (left) and this one (right)
    final List<Expression> hashLeft = new ArrayList<>();
    final List<Expression> hashRight = new ArrayList<>();

    Join(Kind kind) {
      this.kind = kind;
    }

    /** Unmatched rows of the earlier sources are kept, with this source's columns null. */
    boolean leftOuter() {
      return kind == Kind.LEFT || kind == Kind.FULL;
    }

    /** Unmatched rows of this source are kept, with the earlier sources' columns null. */
    boolean rightOuter() {
      return kind == Kind.RIGHT || kind == Kind.FULL;
    }
  }

  final List<Source> sources = new ArrayList<>();
  final List<Join> joins = new ArrayList<>(); // joins.get(i) joins sources.get(i + 1)
  final List<Expression> where = new ArrayList<>(); // WHERE conjuncts not consumed by a rule
  final List<String> outputNames = new ArrayList<>();
  final List<String> outputLabels = new ArrayList<>(); // the names clients see; may repeat
  final List<Expression> outputExpressions = new ArrayList<>();
  final List<DataType> outputTypes = new ArrayList<>(); // null for computed columns
  final List<Integer> outputOids = new ArrayList<>(); // inferred PostgreSQL type OIDs; 0 unknown
  final List<String> expanded = new ArrayList<>(); // outputs that are set-returning functions
  @Nullable List<Expression> groupBy; // every grouping expression
  @Nullable List<List<Expression>> groupingSets; // GROUPING SETS/ROLLUP/CUBE: subsets of groupBy
  @Nullable Expression having;
  boolean aggregated;
  final List<Windows.Spec> windows = new ArrayList<>(); // window calls, distinct by text
  boolean distinct;
  final List<Expression> distinctOn = new ArrayList<>(); // DISTINCT ON (...), bound
  List<OrderByElement> orderBy = Collections.emptyList();
  int limit = -1; // negative for none
  boolean withTies; // FETCH FIRST n ROWS WITH TIES: rows tying with the n-th under ORDER BY too
  int offset;
  final Map<Select, QueryParser.Plan> subplans = new HashMap<>();

  /**
   * True if an outer join may null-extend the rows of {@code s}: as the right side of a LEFT or
   * FULL JOIN, or as an earlier source of a RIGHT or FULL JOIN. WHERE conditions on such a source
   * apply after the join, so they cannot be pushed into its read.
   */
  boolean nullable(Source s) {
    int i = sources.indexOf(s);
    if (i > 0 && joins.get(i - 1).leftOuter()) {
      return true;
    }
    for (int j = i; j < joins.size(); j++) {
      if (joins.get(j).rightOuter()) {
        return true;
      }
    }
    return false;
  }
}
