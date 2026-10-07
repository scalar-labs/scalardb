package com.scalar.db.frontend.postgres;

import com.scalar.db.api.Operation;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;

/** The operators a {@link QueryParser.Plan} is built from. */
final class Operators {
  /** Prefix of the hidden columns an Aggregate adds so a Sort can order by an aggregate. */
  private static final String ORDER_KEY = "$order";

  private Operators() {}

  private static final Iterator<Map<String, Object>> NONE = Collections.emptyIterator();

  private static Map<String, Object> pull(Iterator<Map<String, Object>> rows) {
    return rows.hasNext() ? rows.next() : null;
  }

  /**
   * Counts opens, rows and ScalarDB reads for EXPLAIN ANALYZE around {@link #start}/{@link #fetch}.
   */
  abstract static class Base implements Operator {
    private long rows;
    private long loops;
    private long reads;

    @Override
    public final void open(Execution execution) {
      loops++;
      try {
        start(execution);
      } catch (RuntimeException e) {
        // No cursor reaches the caller, so close the scanners this subtree already opened
        close();
        throw e;
      }
    }

    @Override
    public final Map<String, Object> next() {
      Map<String, Object> row = fetch();
      if (row != null) {
        rows++;
      }
      return row;
    }

    abstract void start(Execution execution);

    @Nullable
    abstract Map<String, Object> fetch();

    /** Runs a read on behalf of this operator, attributing the ScalarDB calls it makes to it. */
    <T> T reading(Execution execution, java.util.function.Supplier<T> read) {
      long before = execution.reader.reads();
      try {
        return read.get();
      } finally {
        reads += execution.reader.reads() - before;
      }
    }

    @Override
    public long rows() {
      return rows;
    }

    @Override
    public long loops() {
      return loops;
    }

    @Override
    public long reads() {
      return reads;
    }
  }

  abstract static class Unary extends Base {
    final Operator input;

    Unary(Operator input) {
      this.input = input;
    }

    @Override
    void start(Execution execution) {
      input.open(execution);
    }

    @Override
    public void close() {
      input.close();
    }

    @Override
    public List<Operator> children() {
      return Collections.singletonList(input);
    }
  }

  /**
   * Reads a ScalarDB Get or Scan. A streamed scan pulls rows from the scanner; otherwise the rows
   * are read once and reused each time the operator is opened, such as for a subquery.
   */
  static final class Scan extends Base {
    private final Operation operation;
    private final String qualifier;
    private final List<String> columns;
    private final boolean stream;
    @Nullable private List<Map<String, Object>> cache;
    @Nullable private QueryParser.Cursor rows;
    private Iterator<Map<String, Object>> cached = NONE;

    Scan(Operation operation, String qualifier, List<String> columns, boolean stream) {
      this.operation = operation;
      this.qualifier = qualifier;
      this.columns = columns;
      this.stream = stream;
    }

    @Override
    void start(Execution execution) {
      close();
      if (cache == null && !stream) {
        cache =
            reading(
                execution,
                () -> QueryParser.readAll(execution.reader.open(operation, qualifier, columns)));
      }
      if (cache != null) {
        cached = cache.iterator();
      } else {
        rows = reading(execution, () -> execution.reader.open(operation, qualifier, columns));
      }
    }

    @Override
    Map<String, Object> fetch() {
      return rows != null ? rows.next() : pull(cached);
    }

    @Override
    public void close() {
      cached = NONE;
      if (rows != null) {
        QueryParser.Cursor open = rows;
        rows = null;
        open.close();
      }
    }

    @Override
    public String describe() {
      return "ScalarDB " + QueryParser.describe(operation) + " AS " + qualifier;
    }

    @Override
    public List<Operator> children() {
      return Collections.emptyList();
    }
  }

  /** A first table bound to the enclosing query's row by key: read once per open. */
  static final class Lookup extends Base {
    private final QueryParser.Source source;
    private final QueryParser.Plan plan;
    private Iterator<Map<String, Object>> rows = NONE;

    Lookup(QueryParser.Source source, QueryParser.Plan plan) {
      this.source = source;
      this.plan = plan;
    }

    @Override
    void start(Execution execution) {
      rows =
          reading(
                  execution,
                  () ->
                      plan.lookupRows(
                          source,
                          Collections.<String, Object>emptyMap(),
                          plan.context(execution.outer)))
              .iterator();
    }

    @Override
    Map<String, Object> fetch() {
      return pull(rows);
    }

    @Override
    public void close() {
      rows = NONE;
    }

    @Override
    public String describe() {
      return "Lookup (per outer row): ScalarDB "
          + QueryParser.describeLookup(source)
          + " AS "
          + source.qualifier;
    }

    @Override
    public List<Operator> children() {
      return Collections.emptyList();
    }
  }

  /** Rows known in advance: catalog tables, EXPLAIN output, the one row of a FROM-less query. */
  static final class Values extends Base {
    private final List<Map<String, Object>> rows;
    private final String description;
    private Iterator<Map<String, Object>> remaining = NONE;

    Values(List<Map<String, Object>> rows, String description) {
      this.rows = rows;
      this.description = description;
    }

    @Override
    void start(Execution execution) {
      remaining = rows.iterator();
    }

    @Override
    Map<String, Object> fetch() {
      return pull(remaining);
    }

    @Override
    public void close() {
      remaining = NONE;
    }

    @Override
    public String describe() {
      return description;
    }

    @Override
    public List<Operator> children() {
      return Collections.emptyList();
    }
  }

  static final class Filter extends Unary {
    private final List<Expression> conditions;
    private final QueryParser.Plan plan;
    private Evaluator.Context ctx = Evaluator.Context.EMPTY;

    Filter(Operator input, List<Expression> conditions, QueryParser.Plan plan) {
      super(input);
      this.conditions = conditions;
      this.plan = plan;
    }

    @Override
    void start(Execution execution) {
      super.start(execution);
      ctx = plan.context(execution.outer);
    }

    @Override
    Map<String, Object> fetch() {
      for (Map<String, Object> row = input.next(); row != null; row = input.next()) {
        if (Evaluator.allTrue(conditions, row, ctx)) {
          return row;
        }
      }
      return null;
    }

    @Override
    public String describe() {
      return "Filter " + QueryParser.text(conditions);
    }
  }

  /**
   * Computes the output columns. With {@code keepInput} the source columns stay in the row so a
   * later Sort can order by them; without expressions it only keeps the named columns.
   */
  static final class Project extends Unary {
    private final List<String> names;
    @Nullable private final List<Expression> expressions;
    private final boolean keepInput;
    private final QueryParser.Plan plan;
    private Evaluator.Context ctx = Evaluator.Context.EMPTY;

    Project(
        Operator input,
        List<String> names,
        @Nullable List<Expression> expressions,
        boolean keepInput,
        QueryParser.Plan plan) {
      super(input);
      this.names = names;
      this.expressions = expressions;
      this.keepInput = keepInput;
      this.plan = plan;
    }

    @Override
    void start(Execution execution) {
      super.start(execution);
      ctx = plan.context(execution.outer);
    }

    @Override
    Map<String, Object> fetch() {
      Map<String, Object> row = input.next();
      if (row == null) {
        return null;
      }
      Map<String, Object> out = keepInput ? new LinkedHashMap<>(row) : new LinkedHashMap<>();
      List<Map<String, Object>> group = Collections.singletonList(row);
      for (int i = 0; i < names.size(); i++) {
        out.put(
            names.get(i),
            expressions == null
                ? row.get(names.get(i))
                : Evaluator.eval(expressions.get(i), group, ctx));
      }
      return out;
    }

    @Override
    public String describe() {
      return "Project " + names;
    }
  }

  /**
   * Blocking: computes every window call over all input rows and adds each value to its row under
   * the call's hidden key, keeping the input order. The Project above reads the values back.
   */
  static final class Window extends Unary {
    private final List<Windows.Spec> windows;
    private final QueryParser.Plan plan;
    private Iterator<Map<String, Object>> out = NONE;

    Window(Operator input, List<Windows.Spec> windows, QueryParser.Plan plan) {
      super(input);
      this.windows = windows;
      this.plan = plan;
    }

    @Override
    void start(Execution execution) {
      super.start(execution);
      List<List<Map<String, Object>>> units = new ArrayList<>();
      for (Map<String, Object> row : QueryParser.drain(input)) {
        units.add(Collections.singletonList(row));
      }
      List<Map<String, Object>> rows = new ArrayList<>(units.size());
      for (List<Map<String, Object>> unit :
          Windows.apply(units, windows, plan.context(execution.outer))) {
        rows.add(unit.get(0));
      }
      out = rows.iterator();
    }

    @Override
    Map<String, Object> fetch() {
      return pull(out);
    }

    @Override
    public void close() {
      out = NONE;
      super.close();
    }

    @Override
    public String describe() {
      return "Window " + Windows.describe(windows);
    }
  }

  /**
   * Set-returning functions in the select list: a row whose expanded columns hold lists becomes one
   * row per element, the lists walked in step and the shorter ones padded with NULL.
   */
  static final class Expand extends Unary {
    private final List<String> columns;
    private Iterator<Map<String, Object>> pending = NONE;

    Expand(Operator input, List<String> columns) {
      super(input);
      this.columns = columns;
    }

    @Override
    Map<String, Object> fetch() {
      while (!pending.hasNext()) {
        Map<String, Object> row = input.next();
        if (row == null) {
          return null;
        }
        int n = 0;
        for (String c : columns) {
          Object v = row.get(c);
          n = Math.max(n, v instanceof List ? ((List<?>) v).size() : v == null ? 0 : 1);
        }
        List<Map<String, Object>> out = new ArrayList<>();
        for (int i = 0; i < n; i++) {
          Map<String, Object> copy = new LinkedHashMap<>(row);
          for (String c : columns) {
            Object v = row.get(c);
            copy.put(
                c,
                v instanceof List ? (i < ((List<?>) v).size() ? ((List<?>) v).get(i) : null) : v);
          }
          out.add(copy);
        }
        pending = out.iterator();
      }
      return pending.next();
    }

    @Override
    public void close() {
      pending = NONE;
      super.close();
    }

    @Override
    public String describe() {
      return "Expand " + columns;
    }
  }

  /** OFFSET and LIMIT; stops pulling from its input once the limit is reached. */
  /** FETCH FIRST n ROWS WITH TIES: after n rows, the rows that tie with the last under ORDER BY. */
  static final class LimitWithTies extends Unary {
    private final int offset;
    private final int limit;
    private final List<OrderByElement> orderBy;
    private final List<String> names;
    private final QueryParser.Plan plan;
    private Evaluator.Context ctx;
    private int skipped;
    private int emitted;
    @Nullable private Map<String, Object> last;

    LimitWithTies(
        Operator input,
        int offset,
        int limit,
        List<OrderByElement> orderBy,
        List<String> names,
        QueryParser.Plan plan) {
      super(input);
      this.offset = offset;
      this.limit = limit;
      this.orderBy = orderBy;
      this.names = names;
      this.plan = plan;
    }

    @Override
    void start(Execution execution) {
      super.start(execution);
      ctx = plan.context(execution.outer);
      skipped = 0;
      emitted = 0;
      last = null;
    }

    @Override
    Map<String, Object> fetch() {
      while (skipped < offset) {
        if (input.next() == null) {
          return null;
        }
        skipped++;
      }
      Map<String, Object> row = input.next();
      if (row == null) {
        return null;
      }
      if (limit >= 0
          && emitted >= limit
          && (last == null || Sort.compare(orderBy, names, last, row, ctx) != 0)) {
        return null;
      }
      emitted++;
      last = row;
      return row;
    }

    @Override
    public String describe() {
      return "Limit " + limit + " with ties" + (offset > 0 ? " offset " + offset : "");
    }
  }

  static final class Limit extends Unary {
    private final int offset;
    private final int limit;
    private int skipped;
    private int emitted;

    Limit(Operator input, int offset, int limit) {
      super(input);
      this.offset = offset;
      this.limit = limit;
    }

    @Override
    void start(Execution execution) {
      super.start(execution);
      skipped = 0;
      emitted = 0;
    }

    @Override
    Map<String, Object> fetch() {
      if (limit >= 0 && emitted >= limit) {
        return null;
      }
      while (skipped < offset) {
        if (input.next() == null) {
          return null;
        }
        skipped++;
      }
      Map<String, Object> row = input.next();
      if (row != null) {
        emitted++;
      }
      return row;
    }

    @Override
    public String describe() {
      return (offset > 0 ? "Offset " + offset + " " : "") + (limit >= 0 ? "Limit " + limit : "");
    }
  }

  /** Blocking: drains its input and emits it in ORDER BY order. */
  static final class Sort extends Unary {
    private final List<OrderByElement> orderBy;
    private final List<String> names;
    private final QueryParser.Plan plan;
    private Iterator<Map<String, Object>> sorted = NONE;

    Sort(Operator input, List<OrderByElement> orderBy, List<String> names, QueryParser.Plan plan) {
      super(input);
      this.orderBy = orderBy;
      this.names = names;
      this.plan = plan;
    }

    @Override
    void start(Execution execution) {
      super.start(execution);
      Evaluator.Context ctx = plan.context(execution.outer);
      List<Map<String, Object>> rows = QueryParser.drain(input);
      rows.sort((x, y) -> compare(orderBy, names, x, y, ctx));
      sorted = rows.iterator();
    }

    /** Two rows in ORDER BY order. */
    static int compare(
        List<OrderByElement> orderBy,
        List<String> names,
        Map<String, Object> x,
        Map<String, Object> y,
        Evaluator.Context ctx) {
      for (int i = 0; i < orderBy.size(); i++) {
        int c =
            Evaluator.orderCompare(
                orderBy.get(i), key(orderBy, names, x, i, ctx), key(orderBy, names, y, i, ctx));
        if (c != 0) {
          return c;
        }
      }
      return 0;
    }

    /** The sort key: a hidden aggregate value, an ordinal, an output column, or an expression. */
    @Nullable
    private static Object key(
        List<OrderByElement> orderBy,
        List<String> names,
        Map<String, Object> row,
        int i,
        Evaluator.Context ctx) {
      if (row.containsKey(ORDER_KEY + i)) {
        return row.get(ORDER_KEY + i);
      }
      Expression e = orderBy.get(i).getExpression();
      if (e instanceof LongValue) {
        int index = (int) ((LongValue) e).getValue() - 1;
        if (index < 0 || index >= names.size()) {
          throw new IllegalArgumentException("ORDER BY position out of range: " + orderBy.get(i));
        }
        return row.get(names.get(index));
      }
      if (e instanceof Column && ((Column) e).getTable() == null) {
        String name = QueryParser.unquote(((Column) e).getColumnName());
        if (row.containsKey(name)) {
          return row.get(name);
        }
      }
      return Evaluator.eval(e, Collections.singletonList(row), ctx);
    }

    @Override
    Map<String, Object> fetch() {
      return pull(sorted);
    }

    @Override
    public void close() {
      sorted = NONE;
      super.close();
    }

    @Override
    public String describe() {
      StringBuilder sb = new StringBuilder("Sort ");
      for (OrderByElement o : orderBy) {
        sb.append(sb.length() == 5 ? "" : ", ").append(o);
      }
      return sb.toString();
    }
  }

  /** Blocking: keeps the first row for each distinct combination of the output columns. */
  static final class Distinct extends Unary {
    private final List<String> names;
    private final List<Expression> on; // DISTINCT ON (...): keep the first row per key
    private final QueryParser.Plan plan;
    private Iterator<Map<String, Object>> distinct = NONE;

    Distinct(Operator input, List<String> names, List<Expression> on, QueryParser.Plan plan) {
      super(input);
      this.names = names;
      this.on = on;
      this.plan = plan;
    }

    @Override
    void start(Execution execution) {
      super.start(execution);
      Evaluator.Context ctx = plan.context(execution.outer);
      Set<List<Object>> seen = new LinkedHashSet<>();
      List<Map<String, Object>> rows = new ArrayList<>();
      for (Map<String, Object> row = input.next(); row != null; row = input.next()) {
        List<Object> key = new ArrayList<>();
        if (on.isEmpty()) {
          for (String name : names) {
            key.add(normalize(row.get(name)));
          }
        } else {
          for (Expression e : on) {
            key.add(normalize(Evaluator.eval(e, Collections.singletonList(row), ctx)));
          }
        }
        if (seen.add(key)) {
          rows.add(row);
        }
      }
      distinct = rows.iterator();
    }

    @Override
    Map<String, Object> fetch() {
      return pull(distinct);
    }

    @Override
    public void close() {
      distinct = NONE;
      super.close();
    }

    @Override
    public String describe() {
      return on.isEmpty() ? "Distinct" : "Distinct ON " + QueryParser.text(on);
    }
  }

  /**
   * Blocking: groups its input, applies HAVING, and emits one output row per group. ORDER BY
   * expressions over the group are computed here into hidden columns for the Sort above.
   */
  /** Row key under which a grouping-set row carries the grouping expressions it left out. */
  static final String GROUPING_KEY = "$grouping";

  static final class Aggregate extends Unary {
    @Nullable private final List<Expression> groupBy;
    // GROUPING SETS / ROLLUP / CUBE: the sets to group by, each a subset of groupBy
    @Nullable private final List<List<Expression>> groupingSets;
    @Nullable private final Expression having;
    private final List<String> names;
    private final List<Expression> expressions;
    private final List<OrderByElement> orderBy;
    private final List<Windows.Spec> windows;
    private final QueryParser.Plan plan;
    private Iterator<Map<String, Object>> groups = NONE;

    Aggregate(
        Operator input,
        @Nullable List<Expression> groupBy,
        @Nullable List<List<Expression>> groupingSets,
        @Nullable Expression having,
        List<String> names,
        List<Expression> expressions,
        List<OrderByElement> orderBy,
        List<Windows.Spec> windows,
        QueryParser.Plan plan) {
      super(input);
      this.groupBy = groupBy;
      this.groupingSets = groupingSets;
      this.having = having;
      this.names = names;
      this.expressions = expressions;
      this.orderBy = orderBy;
      this.windows = windows;
      this.plan = plan;
    }

    @Override
    void start(Execution execution) {
      super.start(execution);
      Evaluator.Context ctx = plan.context(execution.outer);
      List<Map<String, Object>> rows = QueryParser.drain(input);
      List<List<Map<String, Object>>> grouped = new ArrayList<>();
      if (groupingSets != null) {
        // One grouping per set; the grouping columns outside the set read as NULL in that set's
        // rows, so the output expressions and HAVING see them as PostgreSQL does
        for (List<Expression> set : groupingSets) {
          List<String> nulled =
              new ArrayList<>(); // row keys of the grouping columns not in the set
          Set<String> inSet = new HashSet<>();
          for (Expression e : set) {
            inSet.add(e.toString());
          }
          Set<String> outside = new LinkedHashSet<>(); // grouping expressions not in the set
          List<Expression> all = groupBy == null ? Collections.<Expression>emptyList() : groupBy;
          for (Expression g : all) {
            if (inSet.contains(g.toString())) {
              continue;
            }
            outside.add(g.toString());
            if (g instanceof Column) {
              Column c = (Column) g;
              nulled.add(
                  (c.getTable() == null ? "" : QueryParser.unquote(c.getTable().getName()) + ".")
                      + QueryParser.unquote(c.getColumnName()));
            }
          }
          grouped.addAll(groups(rows, set, nulled, outside, ctx));
        }
      } else if (groupBy == null) {
        grouped.add(rows);
      } else {
        grouped.addAll(
            groups(
                rows,
                groupBy,
                Collections.<String>emptyList(),
                Collections.<String>emptySet(),
                ctx));
      }
      List<List<Map<String, Object>>> kept = new ArrayList<>();
      for (List<Map<String, Object>> group : grouped) {
        if (having == null || Evaluator.isTrue(Evaluator.eval(having, group, ctx))) {
          kept.add(group);
        }
      }
      if (!windows.isEmpty()) {
        kept = Windows.apply(kept, windows, ctx);
      }
      List<Map<String, Object>> out = new ArrayList<>();
      for (List<Map<String, Object>> group : kept) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (int i = 0; i < names.size(); i++) {
          row.put(names.get(i), Evaluator.eval(expressions.get(i), group, ctx));
        }
        for (int i = 0; i < orderBy.size(); i++) {
          Expression e = orderBy.get(i).getExpression();
          boolean byOutput =
              e instanceof LongValue
                  || (e instanceof Column
                      && ((Column) e).getTable() == null
                      && names.contains(QueryParser.unquote(((Column) e).getColumnName())));
          if (!byOutput) {
            row.put(ORDER_KEY + i, Evaluator.eval(e, group, ctx));
          }
        }
        out.add(row);
      }
      groups = out.iterator();
    }

    /**
     * Groups the rows by the key expressions; {@code nulled} row keys are blanked first, and {@code
     * outside} (the grouping expressions this set leaves out) is kept in the rows for {@code
     * grouping()}.
     */
    private static List<List<Map<String, Object>>> groups(
        List<Map<String, Object>> rows,
        List<Expression> key,
        List<String> nulled,
        Set<String> outside,
        Evaluator.Context ctx) {
      Map<List<Object>, List<Map<String, Object>>> byKey = new LinkedHashMap<>();
      for (Map<String, Object> row : rows) {
        if (!nulled.isEmpty() || !outside.isEmpty()) {
          row = new LinkedHashMap<>(row);
          for (String k : nulled) {
            row.put(k, null);
          }
          row.put(GROUPING_KEY, outside);
        }
        List<Object> values = new ArrayList<>();
        for (Expression g : key) {
          values.add(normalize(Evaluator.eval(g, Collections.singletonList(row), ctx)));
        }
        byKey.computeIfAbsent(values, k -> new ArrayList<>()).add(row);
      }
      return new ArrayList<>(byKey.values());
    }

    @Override
    Map<String, Object> fetch() {
      return pull(groups);
    }

    @Override
    public void close() {
      groups = NONE;
      super.close();
    }

    @Override
    public String describe() {
      return "Aggregate "
          + names
          + (groupingSets != null
              ? " GROUPING SETS " + groupingSets
              : groupBy == null ? "" : " GROUP BY " + QueryParser.text(groupBy))
          + (having == null ? "" : " HAVING " + having)
          + (windows.isEmpty() ? "" : " WINDOW " + Windows.describe(windows));
    }
  }

  /**
   * WITH RECURSIVE: the anchor's rows, then the step's rows computed over the previous iteration's
   * rows (the working table) until an iteration adds none. UNION drops rows already produced, UNION
   * ALL keeps every row. Rows are renamed positionally to the CTE's column names.
   */
  static final class RecursiveUnion extends Base {
    // ponytail: a cycle under UNION ALL never ends on its own; cap the result instead
    static final int MAX_ROWS = 1_000_000;

    private final QueryParser.Plan anchor;
    private final QueryParser.Plan step;
    private final List<Map<String, Object>> working;
    private final List<String> names;
    private final boolean all;
    private Iterator<Map<String, Object>> out = NONE;

    RecursiveUnion(
        QueryParser.Plan anchor,
        QueryParser.Plan step,
        List<Map<String, Object>> working,
        List<String> names,
        boolean all) {
      this.anchor = anchor;
      this.step = step;
      this.working = working;
      this.names = names;
      this.all = all;
    }

    @Override
    void start(Execution execution) {
      List<Map<String, Object>> result = new ArrayList<>();
      Set<List<Object>> seen = new HashSet<>();
      List<Map<String, Object>> rows = run(anchor, execution);
      while (true) {
        List<Map<String, Object>> fresh = new ArrayList<>();
        for (Map<String, Object> row : rows) {
          if (all || seen.add(key(row))) {
            fresh.add(row);
          }
        }
        result.addAll(fresh);
        if (fresh.isEmpty()) {
          break;
        }
        if (result.size() > MAX_ROWS) {
          throw new IllegalArgumentException(
              "Recursive query produced more than " + MAX_ROWS + " rows");
        }
        working.clear();
        working.addAll(fresh);
        rows = run(step, execution);
      }
      working.clear();
      out = result.iterator();
    }

    /** Runs a member plan to completion and renames its rows to the CTE's columns. */
    private List<Map<String, Object>> run(QueryParser.Plan plan, Execution execution) {
      Operator root = plan.root();
      root.open(execution);
      List<String> from = plan.getOutputColumns();
      List<Map<String, Object>> renamed = new ArrayList<>();
      for (Map<String, Object> row : QueryParser.readAll(root)) {
        Map<String, Object> r = new LinkedHashMap<>();
        for (int i = 0; i < names.size(); i++) {
          r.put(names.get(i), row.get(from.get(i)));
        }
        renamed.add(r);
      }
      return renamed;
    }

    private List<Object> key(Map<String, Object> row) {
      List<Object> key = new ArrayList<>(names.size());
      for (String name : names) {
        key.add(normalize(row.get(name)));
      }
      return key;
    }

    @Override
    Map<String, Object> fetch() {
      return pull(out);
    }

    @Override
    public void close() {
      out = NONE;
      working.clear();
    }

    @Override
    public String describe() {
      return "Recursive Union" + (all ? " ALL " : " ") + names;
    }

    @Override
    public List<Operator> children() {
      return java.util.Arrays.asList(anchor.root(), step.root());
    }
  }

  /** A value normalized for set comparisons, so INT 1 and BIGINT 1 are the same member. */
  static Object normalize(@Nullable Object v) {
    return v instanceof Number && Evaluator.isFinite(v)
        ? new BigDecimal(v.toString()).stripTrailingZeros()
        : v instanceof Number ? ((Number) v).doubleValue() : v;
  }

  /**
   * UNION, INTERSECT or EXCEPT of two inputs. Rows of the right input are re-keyed to the left
   * input's column names. UNION streams: the left input, then the right, dropping repeats unless
   * ALL. INTERSECT and EXCEPT read the right input first, then stream the left against it.
   */
  static final class SetOperation extends Base {
    enum Kind {
      UNION,
      INTERSECT,
      EXCEPT
    }

    private final Kind kind;
    private final boolean all;
    private final Operator left;
    private final Operator right;
    private final List<String> names;
    final List<QueryParser.Plan> members;
    @Nullable private Execution execution;
    private boolean leftDone;
    private boolean rightOpened;
    @Nullable private Map<List<Object>, Integer> rightCounts;
    private Set<List<Object>> seen = new HashSet<>();

    SetOperation(
        Kind kind,
        boolean all,
        Operator left,
        Operator right,
        List<String> names,
        List<QueryParser.Plan> members) {
      this.kind = kind;
      this.all = all;
      this.left = left;
      this.right = right;
      this.names = names;
      this.members = members;
    }

    @Override
    void start(Execution execution) {
      close();
      this.execution = execution;
      leftDone = false;
      rightCounts = null;
      seen = new HashSet<>();
      left.open(execution);
    }

    @Override
    Map<String, Object> fetch() {
      if (kind == Kind.UNION) {
        while (!leftDone) {
          Map<String, Object> row = left.next();
          if (row == null) {
            leftDone = true;
            openRight();
          } else if (all || seen.add(key(row))) {
            return row;
          }
        }
        for (Map<String, Object> row = right.next(); row != null; row = right.next()) {
          Map<String, Object> rekeyed = rekey(row);
          if (all || seen.add(key(rekeyed))) {
            return rekeyed;
          }
        }
        return null;
      }
      if (rightCounts == null) {
        rightCounts = new HashMap<>();
        openRight();
        for (Map<String, Object> row = right.next(); row != null; row = right.next()) {
          rightCounts.merge(key(row), 1, Integer::sum);
        }
      }
      for (Map<String, Object> row = left.next(); row != null; row = left.next()) {
        List<Object> key = key(row);
        boolean inRight = rightCounts.containsKey(key);
        if (kind == Kind.INTERSECT ? inRight && seen.add(key) : !inRight && seen.add(key)) {
          return row;
        }
      }
      return null;
    }

    private void openRight() {
      right.open(execution);
      rightOpened = true;
    }

    private Map<String, Object> rekey(Map<String, Object> row) {
      Map<String, Object> out = new LinkedHashMap<>();
      Iterator<Object> values = row.values().iterator();
      for (String name : names) {
        out.put(name, values.hasNext() ? values.next() : null);
      }
      return out;
    }

    private List<Object> key(Map<String, Object> row) {
      List<Object> key = new ArrayList<>();
      for (Object v : row.values()) {
        key.add(normalize(v));
      }
      return key;
    }

    @Override
    public void close() {
      left.close();
      if (rightOpened) {
        right.close();
        rightOpened = false;
      }
    }

    @Override
    public String describe() {
      String name = kind.name().charAt(0) + kind.name().substring(1).toLowerCase(Locale.ROOT);
      return all ? name + " All" : name;
    }

    @Override
    public List<Operator> children() {
      List<Operator> children = new ArrayList<>();
      children.add(left);
      children.add(right);
      return children;
    }
  }

  /**
   * A subquery in FROM: its plan's tree, with the output columns re-keyed as {@code alias.column}.
   */
  static final class Rekey extends Base {
    final QueryParser.Plan derived;
    private final String qualifier;

    private final Map<String, String> rename = new HashMap<>();

    /** {@code columns} names the subquery's output columns positionally, as a column list does. */
    Rekey(QueryParser.Plan derived, String qualifier, List<String> columns) {
      this.derived = derived;
      this.qualifier = qualifier;
      List<String> outputs = derived.getOutputColumns();
      for (int i = 0; i < outputs.size() && i < columns.size(); i++) {
        rename.put(outputs.get(i), columns.get(i));
      }
    }

    @Override
    void start(Execution execution) {
      derived.root().open(execution);
    }

    @Override
    Map<String, Object> fetch() {
      Map<String, Object> out = derived.root().next();
      if (out == null) {
        return null;
      }
      Map<String, Object> row = new LinkedHashMap<>();
      for (Map.Entry<String, Object> e : out.entrySet()) {
        row.put(qualifier + "." + rename.getOrDefault(e.getKey(), e.getKey()), e.getValue());
      }
      return row;
    }

    @Override
    public void close() {
      derived.root().close();
    }

    @Override
    public String describe() {
      return "Subquery AS " + qualifier;
    }

    @Override
    public List<Operator> children() {
      return Collections.singletonList(derived.root());
    }
  }

  /** Pulls from the left; for each left row, joins it with the candidate rows of the right side. */
  abstract static class Join extends Base {
    final Operator left;
    final QueryParser.Source right;
    final List<Expression> on;
    final LogicalPlan.Join.Kind kind;
    final boolean leftOuter; // unmatched left rows come out with null right columns
    final boolean rightOuter; // unmatched right rows come out last, with null left columns
    private final List<String> leftColumns; // the left side's keys, for that null extension
    final QueryParser.Plan plan;
    Execution execution;
    Evaluator.Context ctx = Evaluator.Context.EMPTY;
    @Nullable private Map<String, Object> current;
    @Nullable private Iterator<Map<String, Object>> candidates;
    private boolean matched;
    private final Set<Map<String, Object>> matchedRight =
        Collections.newSetFromMap(new IdentityHashMap<Map<String, Object>, Boolean>());
    @Nullable private Iterator<Map<String, Object>> unmatchedRight; // once the left is exhausted

    Join(
        Operator left,
        QueryParser.Source right,
        List<Expression> on,
        LogicalPlan.Join.Kind kind,
        List<String> leftColumns,
        QueryParser.Plan plan) {
      this.left = left;
      this.right = right;
      this.on = on;
      this.kind = kind;
      this.leftOuter = kind == LogicalPlan.Join.Kind.LEFT || kind == LogicalPlan.Join.Kind.FULL;
      this.rightOuter = kind == LogicalPlan.Join.Kind.RIGHT || kind == LogicalPlan.Join.Kind.FULL;
      this.leftColumns = leftColumns;
      this.plan = plan;
    }

    @Override
    void start(Execution execution) {
      this.execution = execution;
      ctx = plan.context(execution.outer);
      current = null;
      candidates = null;
      matchedRight.clear();
      unmatchedRight = null;
      left.open(execution);
    }

    /** The next left row; a subclass may read ahead. */
    Map<String, Object> nextLeft() {
      return left.next();
    }

    /** The right rows that may join with {@code leftRow}. */
    abstract List<Map<String, Object>> candidatesFor(Map<String, Object> leftRow);

    /** Every right row (read now if not yet), to find the unmatched ones of a RIGHT/FULL join. */
    abstract List<Map<String, Object>> allRight();

    @Override
    Map<String, Object> fetch() {
      while (true) {
        if (unmatchedRight != null) {
          if (!unmatchedRight.hasNext()) {
            return null;
          }
          Map<String, Object> merged = new LinkedHashMap<>();
          for (String c : leftColumns) {
            merged.put(c, null);
          }
          merged.putAll(unmatchedRight.next());
          return merged;
        }
        if (candidates == null) {
          current = nextLeft();
          if (current == null) {
            if (!rightOuter) {
              return null;
            }
            List<Map<String, Object>> rest = new ArrayList<>();
            for (Map<String, Object> r : allRight()) {
              if (!matchedRight.contains(r)) {
                rest.add(r);
              }
            }
            unmatchedRight = rest.iterator();
            continue;
          }
          candidates = candidatesFor(current).iterator();
          matched = false;
        }
        while (candidates.hasNext()) {
          Map<String, Object> candidate = candidates.next();
          Map<String, Object> merged = new LinkedHashMap<>(current);
          merged.putAll(candidate);
          if (Evaluator.allTrue(on, merged, ctx)) {
            matched = true;
            if (rightOuter) {
              matchedRight.add(candidate);
            }
            return merged;
          }
        }
        candidates = null;
        if (!matched && leftOuter) {
          Map<String, Object> merged = new LinkedHashMap<>(current);
          for (String c : right.columns) {
            merged.put(right.qualifier + "." + c, null);
          }
          return merged;
        }
      }
    }

    @Override
    public void close() {
      candidates = null;
      left.close();
    }

    @Override
    public List<Operator> children() {
      return Collections.singletonList(left);
    }

    String kind() {
      switch (kind) {
        case LEFT:
          return "Left ";
        case RIGHT:
          return "Right ";
        case FULL:
          return "Full ";
        default:
          return "";
      }
    }
  }

  /** The right side is read by key for each left row. */
  static final class LookupJoin extends Join {
    /** Left rows read ahead on the first batch; doubles up to {@link #MAX_BATCH} while consumed. */
    static final int BATCH = 16;

    static final int MAX_BATCH = 128;

    /** Reads in flight at once when the reads may run concurrently. */
    static final int PARALLEL = 16;

    private static final java.util.concurrent.ExecutorService LOOKUPS =
        java.util.concurrent.Executors.newCachedThreadPool(
            runnable -> {
              Thread t = new Thread(runnable, "lookup");
              t.setDaemon(true);
              return t;
            });

    private final java.util.ArrayDeque<Map<String, Object>> pendingLeft =
        new java.util.ArrayDeque<>();
    private final java.util.ArrayDeque<List<Map<String, Object>>> pendingCandidates =
        new java.util.ArrayDeque<>();
    private boolean leftExhausted;
    private int batch;

    LookupJoin(
        Operator left,
        QueryParser.Source right,
        List<Expression> on,
        LogicalPlan.Join.Kind kind,
        List<String> leftColumns,
        QueryParser.Plan plan) {
      super(left, right, on, kind, leftColumns, plan);
    }

    @Override
    void start(Execution execution) {
      super.start(execution);
      pendingLeft.clear();
      pendingCandidates.clear();
      leftExhausted = false;
      batch = BATCH;
    }

    /**
     * Left rows are read ahead in batches and their lookups issued together: lookups into one
     * partition become a single scan, and independent reads overlap when the reader allows it, so a
     * fan-out of N rows costs a few round trips, not N. The batch starts small in case the consumer
     * stops early (LIMIT) and doubles while the join keeps going.
     */
    @Override
    Map<String, Object> nextLeft() {
      if (pendingLeft.isEmpty() && !leftExhausted) {
        List<Map<String, Object>> rows = new ArrayList<>();
        for (int i = 0; i < batch; i++) {
          Map<String, Object> row = left.next();
          if (row == null) {
            leftExhausted = true;
            break;
          }
          rows.add(row);
        }
        if (!rows.isEmpty()) {
          List<List<Map<String, Object>>> results =
              reading(execution, () -> plan.lookupRows(right, rows, ctx, this::run));
          pendingLeft.addAll(rows);
          pendingCandidates.addAll(results);
        }
        batch = Math.min(batch * 2, MAX_BATCH);
      }
      return pendingLeft.poll();
    }

    @Override
    List<Map<String, Object>> candidatesFor(Map<String, Object> leftRow) {
      return pendingCandidates.poll(); // in step with nextLeft()
    }

    /**
     * Runs independent reads: {@link #PARALLEL} at a time on the pool when allowed, else in order.
     */
    private void run(List<Runnable> tasks) {
      if (!execution.reader.concurrent() || tasks.size() == 1) {
        tasks.forEach(Runnable::run);
        return;
      }
      for (int from = 0; from < tasks.size(); from += PARALLEL) {
        List<java.util.concurrent.Future<?>> futures = new ArrayList<>();
        for (Runnable task : tasks.subList(from, Math.min(from + PARALLEL, tasks.size()))) {
          futures.add(LOOKUPS.submit(task));
        }
        for (java.util.concurrent.Future<?> future : futures) {
          try {
            future.get();
          } catch (java.util.concurrent.ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
              throw (RuntimeException) cause;
            }
            throw new IllegalStateException(cause);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
          }
        }
      }
    }

    @Override
    List<Map<String, Object>> allRight() {
      throw new IllegalStateException("A lookup join cannot be a RIGHT or FULL join");
    }

    @Override
    public String describe() {
      return kind()
          + "Lookup Join (per outer row): ScalarDB "
          + QueryParser.describeLookup(right)
          + " AS "
          + right.qualifier
          + (on.isEmpty() ? "" : " ON " + QueryParser.text(on));
    }
  }

  /** The right side is read once and hashed on the join equalities; left rows probe it. */
  static final class HashJoin extends Join {
    private final Operator rightOperator;
    private final List<Expression> hashLeft;
    private final List<Expression> hashRight;
    @Nullable private Map<List<Object>, List<Map<String, Object>>> table;
    private List<Map<String, Object>> rightRows = Collections.emptyList();

    HashJoin(
        Operator left,
        Operator rightOperator,
        QueryParser.Source right,
        List<Expression> hashLeft,
        List<Expression> hashRight,
        List<Expression> on,
        LogicalPlan.Join.Kind kind,
        List<String> leftColumns,
        QueryParser.Plan plan) {
      super(left, right, on, kind, leftColumns, plan);
      this.rightOperator = rightOperator;
      this.hashLeft = hashLeft;
      this.hashRight = hashRight;
    }

    @Override
    void start(Execution execution) {
      super.start(execution);
      table = null;
    }

    private void build() {
      table = new HashMap<>();
      rightOperator.open(execution);
      rightRows = QueryParser.readAll(rightOperator);
      for (Map<String, Object> r : rightRows) {
        List<Object> key = key(hashRight, r);
        if (key != null) {
          table.computeIfAbsent(key, k -> new ArrayList<>()).add(r);
        }
      }
    }

    @Override
    List<Map<String, Object>> allRight() {
      if (table == null) {
        build();
      }
      return rightRows;
    }

    @Override
    List<Map<String, Object>> candidatesFor(Map<String, Object> leftRow) {
      if (table == null) {
        build();
      }
      List<Object> key = key(hashLeft, leftRow);
      return key == null
          ? Collections.<Map<String, Object>>emptyList()
          : table.getOrDefault(key, Collections.<Map<String, Object>>emptyList());
    }

    /** Join key values, with numbers normalized so INT 1 and BIGINT 1 match; null on any NULL. */
    @Nullable
    private List<Object> key(List<Expression> expressions, Map<String, Object> row) {
      List<Object> key = new ArrayList<>();
      for (Expression e : expressions) {
        Object v = Evaluator.eval(e, Collections.singletonList(row), ctx);
        if (v == null) {
          return null;
        }
        key.add(normalize(v));
      }
      return key;
    }

    @Override
    public void close() {
      super.close();
      rightOperator.close();
    }

    @Override
    public List<Operator> children() {
      List<Operator> children = new ArrayList<>();
      children.add(left);
      children.add(rightOperator);
      return children;
    }

    @Override
    public String describe() {
      StringBuilder sb = new StringBuilder(kind()).append("Hash Join ON ");
      for (int k = 0; k < hashLeft.size(); k++) {
        sb.append(k == 0 ? "" : " AND ")
            .append(hashLeft.get(k))
            .append(" = ")
            .append(hashRight.get(k));
      }
      return sb.append(on.isEmpty() ? "" : " AND " + QueryParser.text(on)).toString();
    }
  }

  /** The right side is read once; every left row is checked against every right row. */
  /** LATERAL: the right subquery runs once per left row, which it may reference. */
  static final class LateralJoin extends Join {
    private final Map<String, String> rename = new HashMap<>(); // output name -> column name

    LateralJoin(
        Operator left,
        QueryParser.Source right,
        List<Expression> on,
        LogicalPlan.Join.Kind kind,
        List<String> leftColumns,
        QueryParser.Plan plan) {
      super(left, right, on, kind, leftColumns, plan);
      if (rightOuter) {
        throw new IllegalArgumentException("RIGHT and FULL JOIN LATERAL are not supported");
      }
      List<String> outputs = right.derived.getOutputColumns();
      for (int i = 0; i < outputs.size() && i < right.columns.size(); i++) {
        rename.put(outputs.get(i), right.columns.get(i));
      }
    }

    @Override
    List<Map<String, Object>> candidatesFor(Map<String, Object> leftRow) {
      Map<String, Object> outer = new LinkedHashMap<>();
      if (execution.outer != null) {
        outer.putAll(execution.outer);
      }
      outer.putAll(leftRow);
      List<Map<String, Object>> out = new ArrayList<>();
      for (Map<String, Object> row : right.derived.evaluate(outer)) {
        Map<String, Object> keyed = new LinkedHashMap<>();
        for (Map.Entry<String, Object> e : row.entrySet()) {
          keyed.put(
              right.qualifier + "." + rename.getOrDefault(e.getKey(), e.getKey()), e.getValue());
        }
        out.add(keyed);
      }
      return out;
    }

    @Override
    List<Map<String, Object>> allRight() {
      throw new IllegalStateException("A lateral subquery has no rows apart from a left row");
    }

    @Override
    public String describe() {
      return "Lateral "
          + kind
          + " join "
          + right.qualifier
          + " (subquery per left row)"
          + (on.isEmpty() ? "" : " ON " + QueryParser.text(on));
    }

    @Override
    public List<Operator> children() {
      List<Operator> children = new ArrayList<>();
      children.add(left);
      children.add(right.derived.root());
      return children;
    }
  }

  static final class NestedLoopJoin extends Join {
    private final Operator rightOperator;
    @Nullable private List<Map<String, Object>> rightRows;

    NestedLoopJoin(
        Operator left,
        Operator rightOperator,
        QueryParser.Source right,
        List<Expression> on,
        LogicalPlan.Join.Kind kind,
        List<String> leftColumns,
        QueryParser.Plan plan) {
      super(left, right, on, kind, leftColumns, plan);
      this.rightOperator = rightOperator;
    }

    @Override
    List<Map<String, Object>> allRight() {
      return candidatesFor(Collections.<String, Object>emptyMap());
    }

    @Override
    void start(Execution execution) {
      super.start(execution);
      rightRows = null;
    }

    @Override
    List<Map<String, Object>> candidatesFor(Map<String, Object> leftRow) {
      if (rightRows == null) {
        // ponytail: O(n*m); only for joins without an equality to hash on
        rightOperator.open(execution);
        rightRows = QueryParser.readAll(rightOperator);
      }
      return rightRows;
    }

    @Override
    public void close() {
      super.close();
      rightOperator.close();
    }

    @Override
    public List<Operator> children() {
      List<Operator> children = new ArrayList<>();
      children.add(left);
      children.add(rightOperator);
      return children;
    }

    @Override
    public String describe() {
      return kind()
          + "Nested Loop Join"
          + (on.isEmpty() ? " (no condition)" : " ON " + QueryParser.text(on));
    }
  }
}
