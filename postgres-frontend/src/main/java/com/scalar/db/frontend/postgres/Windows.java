package com.scalar.db.frontend.postgres;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import javax.annotation.Nullable;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.WindowDefinition;
import net.sf.jsqlparser.expression.WindowElement;
import net.sf.jsqlparser.expression.WindowOffset;
import net.sf.jsqlparser.statement.select.OrderByElement;

/**
 * Window functions: {@code f(...) OVER (PARTITION BY ... ORDER BY ... frame)}. They run after
 * WHERE, GROUP BY and HAVING and before ORDER BY, DISTINCT and LIMIT, over the rows (or, in an
 * aggregated query, the groups) the query produces. Each call's value is added to its row under a
 * hidden key, and {@link Evaluator} reads it back when it meets the call in an expression.
 */
final class Windows {
  /** Prefix of a window call's hidden row key: the prefix followed by the call's text. */
  static final String KEY = "$window:";

  private static final List<String> RANKING =
      Arrays.asList(
          "ROW_NUMBER",
          "RANK",
          "DENSE_RANK",
          "PERCENT_RANK",
          "CUME_DIST",
          "NTILE",
          "LAG",
          "LEAD",
          "FIRST_VALUE",
          "LAST_VALUE",
          "NTH_VALUE");

  private Windows() {}

  /** One window call with its window resolved: a named window is taken from the WINDOW clause. */
  static final class Spec {
    final AnalyticExpression call;
    final String name; // the upper-cased function name
    final List<Expression> partitionBy;
    final List<OrderByElement> orderBy;
    @Nullable final WindowElement frame;
    @Nullable private String key; // computed once the call's columns are bound

    private Spec(
        AnalyticExpression call,
        String name,
        List<Expression> partitionBy,
        List<OrderByElement> orderBy,
        @Nullable WindowElement frame) {
      this.call = call;
      this.name = name;
      this.partitionBy = partitionBy;
      this.orderBy = orderBy;
      this.frame = frame;
    }

    /** The hidden row key; the call's text, so it must not be taken before binding. */
    String key() {
      if (key == null) {
        key = KEY + call;
      }
      return key;
    }
  }

  static Spec spec(AnalyticExpression a, @Nullable List<WindowDefinition> named) {
    String name = a.getName().toUpperCase(Locale.ROOT);
    name = name.substring(name.lastIndexOf('.') + 1);
    if (!RANKING.contains(name) && !Evaluator.isAggregateName(name)) {
      throw new IllegalArgumentException("Unsupported window function: " + a);
    }
    if (a.isDistinct()) {
      throw new IllegalArgumentException("DISTINCT is not implemented for window functions");
    }
    List<?> partition = a.getPartitionExpressionList();
    List<OrderByElement> orderBy = a.getOrderByElements();
    WindowElement frame = a.getWindowElement();
    if (a.getWindowName() != null) {
      WindowDefinition def = null;
      for (WindowDefinition d : named == null ? Collections.<WindowDefinition>emptyList() : named) {
        if (a.getWindowName().equalsIgnoreCase(d.getWindowName())) {
          def = d;
        }
      }
      if (def == null) {
        throw new IllegalArgumentException("Unknown window: " + a.getWindowName());
      }
      partition = def.getPartitionExpressionList();
      orderBy = def.getOrderByElements();
      frame = def.getWindowElement();
    }
    List<Expression> partitionBy = new ArrayList<>();
    for (Object p : partition == null ? Collections.emptyList() : partition) {
      partitionBy.add((Expression) p);
    }
    if (frame != null) {
      for (WindowOffset bound : bounds(frame)) {
        boolean unbounded = bound.getExpression() == null;
        if (bound.getType() == WindowOffset.Type.EXPR
            || (!unbounded
                && bound.getType() != WindowOffset.Type.CURRENT
                && frame.getType() != WindowElement.Type.ROWS)) {
          throw new IllegalArgumentException("Unsupported window frame: " + frame);
        }
      }
    }
    return new Spec(
        a, name, partitionBy, orderBy == null ? Collections.emptyList() : orderBy, frame);
  }

  /** The expressions inside a window call, for visitors that the parser's adapter does not run. */
  static List<Expression> parts(AnalyticExpression a) {
    List<Expression> out = new ArrayList<>();
    for (Expression e :
        Arrays.asList(
            a.getExpression(), a.getOffset(), a.getDefaultValue(), a.getFilterExpression())) {
      if (e != null) {
        out.add(e);
      }
    }
    if (a.getPartitionExpressionList() != null) {
      for (Object p : a.getPartitionExpressionList()) {
        out.add((Expression) p);
      }
    }
    if (a.getOrderByElements() != null) {
      for (OrderByElement o : a.getOrderByElements()) {
        out.add(o.getExpression());
      }
    }
    return out;
  }

  /** {@code count(*)}: the parser keeps the star as an AllColumns argument or as a flag. */
  private static boolean star(AnalyticExpression a) {
    return a.isAllColumns()
        || a.getExpression() instanceof net.sf.jsqlparser.statement.select.AllColumns;
  }

  static String describe(List<Spec> specs) {
    List<String> calls = new ArrayList<>();
    for (Spec s : specs) {
      calls.add(s.call.toString());
    }
    return calls.toString();
  }

  /**
   * Computes every window call over the units (each a row, or the rows of a group) and returns the
   * units in input order with the values set in a copy of their first row.
   */
  static List<List<Map<String, Object>>> apply(
      List<List<Map<String, Object>>> units, List<Spec> specs, Evaluator.Context ctx) {
    List<List<Map<String, Object>>> out = new ArrayList<>(units.size());
    for (List<Map<String, Object>> unit : units) {
      List<Map<String, Object>> copy = new ArrayList<>(unit);
      copy.set(0, new LinkedHashMap<>(unit.get(0)));
      out.add(copy);
    }
    for (Spec s : specs) {
      compute(s, out, ctx);
    }
    return out;
  }

  private static void compute(
      Spec s, List<List<Map<String, Object>>> units, Evaluator.Context ctx) {
    Map<List<Object>, List<Integer>> partitions = new LinkedHashMap<>();
    for (int i = 0; i < units.size(); i++) {
      List<Object> key = new ArrayList<>(s.partitionBy.size());
      for (Expression p : s.partitionBy) {
        key.add(Operators.normalize(Evaluator.eval(p, units.get(i), ctx)));
      }
      partitions.computeIfAbsent(key, k -> new ArrayList<>()).add(i);
    }
    for (List<Integer> partition : partitions.values()) {
      Map<Integer, Object[]> keys = new HashMap<>(); // the ORDER BY values of each unit
      if (!s.orderBy.isEmpty()) {
        for (int i : partition) {
          Object[] k = new Object[s.orderBy.size()];
          for (int j = 0; j < k.length; j++) {
            k[j] = Evaluator.eval(s.orderBy.get(j).getExpression(), units.get(i), ctx);
          }
          keys.put(i, k);
        }
        partition.sort((x, y) -> compareKeys(s.orderBy, keys.get(x), keys.get(y)));
      }
      int m = partition.size();
      // Peers: the run of rows with the same ORDER BY values (every row, with no ORDER BY)
      int[] peerStart = new int[m];
      int[] peerEnd = new int[m];
      for (int j = 0; j < m; ) {
        int k = j + 1;
        while (k < m
            && (s.orderBy.isEmpty()
                || compareKeys(s.orderBy, keys.get(partition.get(j)), keys.get(partition.get(k)))
                    == 0)) {
          k++;
        }
        for (int t = j; t < k; t++) {
          peerStart[t] = j;
          peerEnd[t] = k;
        }
        j = k;
      }
      int denseRank = 0;
      for (int j = 0; j < m; j++) {
        if (peerStart[j] == j) {
          denseRank++;
        }
        List<Map<String, Object>> unit = units.get(partition.get(j));
        Object value;
        switch (s.name) {
          case "ROW_NUMBER":
            value = (long) (j + 1);
            break;
          case "RANK":
            value = (long) (peerStart[j] + 1);
            break;
          case "DENSE_RANK":
            value = (long) denseRank;
            break;
          case "PERCENT_RANK":
            value = m == 1 ? 0.0 : (double) peerStart[j] / (m - 1);
            break;
          case "CUME_DIST":
            value = (double) peerEnd[j] / m;
            break;
          case "NTILE":
            value = ntile(j, m, intArg(s.call.getExpression(), unit, ctx, s.call));
            break;
          case "LAG":
          case "LEAD":
            {
              Integer offset =
                  s.call.getOffset() == null ? 1 : intArg(s.call.getOffset(), unit, ctx, s.call);
              int target = offset == null ? -1 : s.name.equals("LAG") ? j - offset : j + offset;
              if (offset != null && target >= 0 && target < m) {
                value =
                    Evaluator.eval(s.call.getExpression(), units.get(partition.get(target)), ctx);
              } else {
                value =
                    s.call.getDefaultValue() == null
                        ? null
                        : Evaluator.eval(s.call.getDefaultValue(), unit, ctx);
              }
              break;
            }
          default:
            {
              int[] frame = frame(s, j, m, peerStart, peerEnd, unit, ctx);
              value = overFrame(s, frame[0], frame[1], partition, units, ctx);
            }
        }
        unit.get(0).put(s.key(), value);
      }
    }
  }

  private static int compareKeys(List<OrderByElement> orderBy, Object[] x, Object[] y) {
    for (int j = 0; j < orderBy.size(); j++) {
      int c = Evaluator.orderCompare(orderBy.get(j), x[j], y[j]);
      if (c != 0) {
        return c;
      }
    }
    return 0;
  }

  /** PostgreSQL's ntile: the first {@code m % n} buckets get one row more than the others. */
  @Nullable
  private static Object ntile(int j, int m, @Nullable Integer n) {
    if (n == null) {
      return null;
    }
    if (n <= 0) {
      throw new IllegalArgumentException("Argument of ntile must be greater than zero");
    }
    int q = m / n;
    int r = m % n;
    return j < r * (q + 1) ? (long) (j / (q + 1) + 1) : (long) ((j - r * (q + 1)) / q + r + 1);
  }

  @Nullable
  private static Integer intArg(
      @Nullable Expression e,
      List<Map<String, Object>> unit,
      Evaluator.Context ctx,
      AnalyticExpression call) {
    if (e == null) {
      throw new IllegalArgumentException("Missing argument: " + call);
    }
    Object v = Evaluator.eval(e, unit, ctx);
    if (v == null) {
      return null;
    }
    if (!(v instanceof Number)) {
      throw new IllegalArgumentException("Integer argument expected: " + call);
    }
    return ((Number) v).intValue();
  }

  private static List<WindowOffset> bounds(WindowElement frame) {
    if (frame.getRange() != null) {
      return Arrays.asList(frame.getRange().getStart(), frame.getRange().getEnd());
    }
    return Collections.singletonList(frame.getOffset());
  }

  /**
   * The frame of row {@code j} as a half-open range of positions in its sorted partition. Without a
   * frame clause it is the whole partition, or up to the current row's last peer when the window
   * has an ORDER BY (RANGE UNBOUNDED PRECEDING).
   */
  private static int[] frame(
      Spec s,
      int j,
      int m,
      int[] peerStart,
      int[] peerEnd,
      List<Map<String, Object>> unit,
      Evaluator.Context ctx) {
    if (s.frame == null) {
      return new int[] {0, s.orderBy.isEmpty() ? m : peerEnd[j]};
    }
    boolean rows = s.frame.getType() == WindowElement.Type.ROWS;
    List<WindowOffset> bounds = bounds(s.frame);
    int lo = bound(bounds.get(0), true, rows, j, m, peerStart, peerEnd, unit, ctx);
    int hi =
        bounds.size() == 1
            ? (rows ? j + 1 : peerEnd[j]) // "frame n PRECEDING" ends at the current row
            : bound(bounds.get(1), false, rows, j, m, peerStart, peerEnd, unit, ctx);
    return new int[] {Math.max(lo, 0), Math.min(hi, m)};
  }

  private static int bound(
      WindowOffset o,
      boolean start,
      boolean rows,
      int j,
      int m,
      int[] peerStart,
      int[] peerEnd,
      List<Map<String, Object>> unit,
      Evaluator.Context ctx) {
    switch (o.getType()) {
      case CURRENT:
        return start ? (rows ? j : peerStart[j]) : (rows ? j + 1 : peerEnd[j]);
      case PRECEDING:
        if (o.getExpression() == null) {
          return 0; // UNBOUNDED
        }
        return j - offset(o, unit, ctx) + (start ? 0 : 1);
      case FOLLOWING:
        if (o.getExpression() == null) {
          return m; // UNBOUNDED
        }
        return j + offset(o, unit, ctx) + (start ? 0 : 1);
      default:
        throw new IllegalArgumentException("Unsupported window frame bound: " + o);
    }
  }

  private static int offset(WindowOffset o, List<Map<String, Object>> unit, Evaluator.Context ctx) {
    Object v = Evaluator.eval(o.getExpression(), unit, ctx);
    if (!(v instanceof Number) || ((Number) v).longValue() < 0) {
      throw new IllegalArgumentException("Frame offset must be a non-negative integer: " + o);
    }
    return ((Number) v).intValue();
  }

  /** first_value, last_value, nth_value, or an aggregate over the frame {@code [lo, hi)}. */
  @Nullable
  private static Object overFrame(
      Spec s,
      int lo,
      int hi,
      List<Integer> partition,
      List<List<Map<String, Object>>> units,
      Evaluator.Context ctx) {
    AnalyticExpression a = s.call;
    switch (s.name) {
      case "FIRST_VALUE":
        return lo < hi
            ? Evaluator.eval(a.getExpression(), units.get(partition.get(lo)), ctx)
            : null;
      case "LAST_VALUE":
        return lo < hi
            ? Evaluator.eval(a.getExpression(), units.get(partition.get(hi - 1)), ctx)
            : null;
      case "NTH_VALUE":
        {
          Integer n = intArg(a.getOffset(), units.get(partition.get(Math.min(lo, hi))), ctx, a);
          if (n != null && n <= 0) {
            throw new IllegalArgumentException("Argument of nth_value must be greater than zero");
          }
          return n != null && lo + n - 1 < hi
              ? Evaluator.eval(a.getExpression(), units.get(partition.get(lo + n - 1)), ctx)
              : null;
        }
      default:
        break;
    }
    List<Object> values = new ArrayList<>();
    for (int t = lo; t < hi; t++) {
      List<Map<String, Object>> unit = units.get(partition.get(t));
      if (a.getFilterExpression() != null
          && !Evaluator.isTrue(Evaluator.eval(a.getFilterExpression(), unit, ctx))) {
        continue;
      }
      Object v = star(a) ? unit : Evaluator.eval(a.getExpression(), unit, ctx);
      if (v != null) {
        values.add(v);
      }
    }
    Object delimiter =
        s.name.equals("STRING_AGG") && a.getOffset() != null && !values.isEmpty()
            ? Evaluator.eval(a.getOffset(), units.get(partition.get(lo)), ctx)
            : null;
    return Evaluator.aggregateValues(s.name, values, delimiter, a);
  }
}
