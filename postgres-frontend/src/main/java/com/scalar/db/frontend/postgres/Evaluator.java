package com.scalar.db.frontend.postgres;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.AnalyticType;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.AnyType;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.CollateExpression;
import net.sf.jsqlparser.expression.DateTimeLiteralExpression;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExtractExpression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.IntervalExpression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NotExpression;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeKeyExpression;
import net.sf.jsqlparser.expression.TimezoneExpression;
import net.sf.jsqlparser.expression.TrimFunction;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Modulo;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsBooleanExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ParenthesedExpressionList;
import net.sf.jsqlparser.expression.operators.relational.RegExpMatchOperator;
import net.sf.jsqlparser.expression.operators.relational.RegExpMatchOperatorType;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.ParenthesedSelect;
import net.sf.jsqlparser.statement.select.Select;

/**
 * Evaluates a SQL expression in memory over rows that ScalarDB returned. A row maps {@code
 * qualifier.column} keys to the Java values ScalarDB uses. Expressions are evaluated against a
 * group of rows so that aggregate functions work; scalar expressions read the first row of the
 * group. NULL follows SQL three-valued logic: comparisons with NULL yield null, and only {@code
 * Boolean.TRUE} passes a filter.
 */
final class Evaluator {
  private static final List<String> AGGREGATES =
      Arrays.asList(
          "JSON_AGG",
          "JSONB_AGG",
          "JSON_OBJECT_AGG",
          "JSONB_OBJECT_AGG",
          "COUNT",
          "SUM",
          "AVG",
          "MIN",
          "MAX",
          "BOOL_AND",
          "BOOL_OR",
          "EVERY",
          "STRING_AGG",
          "ARRAY_AGG");

  private Evaluator() {}

  /** The outer row of a correlated subquery and the plans of the subqueries in the expression. */
  static final class Context {
    static final Context EMPTY = new Context(null, Collections.emptyMap(), null);

    @Nullable final Map<String, Object> outer;
    final Map<Select, QueryParser.Plan> subplans;
    @Nullable final Catalog catalog;
    final List<Object> parameters; // the statement's $n values, 1-based

    Context(
        @Nullable Map<String, Object> outer,
        Map<Select, QueryParser.Plan> subplans,
        @Nullable Catalog catalog) {
      this(outer, subplans, catalog, Collections.emptyList());
    }

    Context(
        @Nullable Map<String, Object> outer,
        Map<Select, QueryParser.Plan> subplans,
        @Nullable Catalog catalog,
        List<Object> parameters) {
      this.outer = outer;
      this.subplans = subplans;
      this.catalog = catalog;
      this.parameters = parameters;
    }

    List<Map<String, Object>> subquery(Select select, List<Map<String, Object>> group) {
      QueryParser.Plan plan = subplans.get(select);
      if (plan == null) {
        throw new IllegalArgumentException("Unsupported subquery: " + select);
      }
      Map<String, Object> row = new LinkedHashMap<>();
      if (outer != null) {
        row.putAll(outer);
      }
      if (!group.isEmpty()) {
        row.putAll(group.get(0));
      }
      return plan.evaluate(row);
    }
  }

  static boolean isTrue(@Nullable Object value) {
    return Boolean.TRUE.equals(value);
  }

  static boolean isAggregate(Function f) {
    return AGGREGATES.contains(functionName(f));
  }

  static boolean isAggregateName(String upperCaseName) {
    return AGGREGATES.contains(upperCaseName);
  }

  /** An aggregate with a FILTER clause, such as {@code count(*) FILTER (WHERE active)}. */
  static boolean isFilteredAggregate(Expression e) {
    return e instanceof AnalyticExpression
        && ((AnalyticExpression) e).getType() == AnalyticType.FILTER_ONLY
        && AGGREGATES.contains(((AnalyticExpression) e).getName().toUpperCase(Locale.ROOT));
  }

  /** The upper-cased function name without a {@code pg_catalog.} schema prefix. */
  private static String functionName(Function f) {
    String name = f.getName().toUpperCase(Locale.ROOT);
    return name.substring(name.lastIndexOf('.') + 1);
  }

  static boolean allTrue(List<Expression> conditions, Map<String, Object> row, Context ctx) {
    List<Map<String, Object>> group = Collections.singletonList(row);
    for (Expression c : conditions) {
      if (!isTrue(eval(c, group, ctx))) {
        return false;
      }
    }
    return true;
  }

  @Nullable
  static Object eval(Expression e, List<Map<String, Object>> group, Context ctx) {
    e = QueryParser.unwrap(e);
    if (e instanceof net.sf.jsqlparser.expression.BooleanValue) {
      return ((net.sf.jsqlparser.expression.BooleanValue) e).getValue();
    }
    if (e instanceof NullValue) {
      return null;
    }
    if (e instanceof LongValue) {
      return integer(((LongValue) e).getBigIntegerValue());
    }
    if (e instanceof DoubleValue) {
      // A literal with a decimal point or exponent is numeric in PostgreSQL, not a double
      return decimal(e.toString());
    }
    if (e instanceof StringValue) {
      return QueryParser.stringLiteral((StringValue) e);
    }
    if (e instanceof DateTimeLiteralExpression) {
      return temporal((DateTimeLiteralExpression) e);
    }
    if (e instanceof TimeKeyExpression) {
      return now(((TimeKeyExpression) e).getStringValue());
    }
    if (e instanceof net.sf.jsqlparser.expression.ArrayConstructor) {
      List<Object> out = new ArrayList<>();
      for (Expression item : ((net.sf.jsqlparser.expression.ArrayConstructor) e).getExpressions()) {
        out.add(eval(item, group, ctx));
      }
      return out;
    }
    if (e instanceof IntervalExpression) {
      IntervalExpression iv = (IntervalExpression) e;
      String p =
          iv.getExpression() != null
              ? text(eval(iv.getExpression(), group, ctx))
              : String.valueOf(iv.getParameter());
      p = p.startsWith("'") && p.endsWith("'") ? p.substring(1, p.length() - 1) : p;
      // INTERVAL '1' DAY carries its unit apart from the amount (the leading field of HOUR TO
      // MINUTE)
      String unit =
          iv.getIntervalQualifier() != null
              ? iv.getIntervalQualifier().getLeadingField()
              : iv.getIntervalType();
      return Interval.parse(unit == null ? p : p + " " + unit);
    }
    if (e instanceof TimezoneExpression) {
      TimezoneExpression tz = (TimezoneExpression) e;
      return atTimeZone(
          eval(tz.getLeftExpression(), group, ctx),
          eval(tz.getTimezoneExpressions().get(0), group, ctx));
    }
    if (e instanceof SignedExpression) {
      Expression inner = ((SignedExpression) e).getExpression();
      if (((SignedExpression) e).getSign() == '-' && inner instanceof LongValue) {
        // -9223372036854775808 is in range although its absolute value is not
        return integer(((LongValue) inner).getBigIntegerValue().negate());
      }
      Object v = eval(inner, group, ctx);
      if (((SignedExpression) e).getSign() == '~') {
        return v == null ? null : (Object) ~integral(v, e);
      }
      return ((SignedExpression) e).getSign() == '-' ? negate(v) : v;
    }
    if (e instanceof Column) {
      return column((Column) e, group, ctx);
    }
    if (e instanceof JdbcParameter) {
      int i = ((JdbcParameter) e).getIndex() - 1;
      if (i < 0 || i >= ctx.parameters.size()) {
        throw new IllegalArgumentException("Unbound parameter: " + e);
      }
      return ctx.parameters.get(i);
    }
    if (e instanceof Function) {
      Function f = (Function) e;
      return isAggregate(f) ? aggregate(f, group, ctx) : scalar(f, group, ctx);
    }
    if (e instanceof AnalyticExpression
        && ((AnalyticExpression) e).getType() == AnalyticType.OVER) {
      // Computed by Windows beforehand and left in the row under the call's hidden key
      Map<String, Object> row = group.isEmpty() ? Collections.emptyMap() : group.get(0);
      String key = Windows.KEY + e; // ponytail: deparsed per row; cache per call if it shows up
      if (!row.containsKey(key)) {
        throw new IllegalArgumentException(
            "Window functions are allowed only in the select list and ORDER BY: " + e);
      }
      return row.get(key);
    }
    if (isFilteredAggregate(e)) {
      AnalyticExpression a = (AnalyticExpression) e;
      Function f = new Function();
      f.setName(a.getName());
      f.setParameters(new ExpressionList<Expression>(a.getExpression()));
      f.setDistinct(a.isDistinct());
      List<Map<String, Object>> kept = new ArrayList<>();
      for (Map<String, Object> row : group) {
        if (isTrue(eval(a.getFilterExpression(), Collections.singletonList(row), ctx))) {
          kept.add(row);
        }
      }
      return aggregate(f, kept, ctx);
    }
    if (e instanceof BitwiseXor) {
      // PostgreSQL's ^ is exponentiation (# is XOR)
      Object l = eval(((BitwiseXor) e).getLeftExpression(), group, ctx);
      Object r = eval(((BitwiseXor) e).getRightExpression(), group, ctx);
      return l == null || r == null
          ? null
          : (Object) Math.pow(((Number) l).doubleValue(), ((Number) r).doubleValue());
    }
    if (e instanceof ExtractExpression) {
      ExtractExpression x = (ExtractExpression) e;
      return extract(x.getName(), eval(x.getExpression(), group, ctx));
    }
    if (e instanceof TrimFunction) {
      return trim((TrimFunction) e, group, ctx);
    }
    if (e instanceof CastExpression) {
      CastExpression cast = (CastExpression) e;
      String type = castType(cast.getColDataType());
      Object v = eval(cast.getLeftExpression(), group, ctx);
      String t = type.toLowerCase(Locale.ROOT);
      t = t.startsWith("pg_catalog.") ? t.substring("pg_catalog.".length()) : t;
      if (v instanceof String && ctx.catalog != null) {
        // An object name cast to its OID, as ORMs write 'table'::regclass in catalog queries
        switch (t) {
          case "regclass":
            return ctx.catalog.relationOid((String) v);
          case "regnamespace":
            return ctx.catalog.namespaceOid((String) v);
          case "regtype":
            return Catalog.typeOid((String) v);
          default:
            break;
        }
      }
      return cast(type, v);
    }
    if (e instanceof CaseExpression) {
      return caseOf((CaseExpression) e, group, ctx);
    }
    if (e instanceof CollateExpression) {
      return eval(((CollateExpression) e).getLeftExpression(), group, ctx);
    }
    if (e instanceof RegExpMatchOperator) {
      RegExpMatchOperator re = (RegExpMatchOperator) e;
      Object v = eval(re.getLeftExpression(), group, ctx);
      Object p = eval(re.getRightExpression(), group, ctx);
      if (v == null || p == null) {
        return null;
      }
      RegExpMatchOperatorType type = re.getOperatorType();
      boolean caseInsensitive =
          type == RegExpMatchOperatorType.MATCH_CASEINSENSITIVE
              || type == RegExpMatchOperatorType.NOT_MATCH_CASEINSENSITIVE;
      boolean negate =
          type == RegExpMatchOperatorType.NOT_MATCH_CASESENSITIVE
              || type == RegExpMatchOperatorType.NOT_MATCH_CASEINSENSITIVE;
      boolean found =
          Pattern.compile(text(p), caseInsensitive ? Pattern.CASE_INSENSITIVE : 0)
              .matcher(text(v))
              .find();
      return found != negate;
    }
    if (e instanceof ParenthesedSelect) {
      List<Map<String, Object>> rows = ctx.subquery((ParenthesedSelect) e, group);
      if (rows.isEmpty()) {
        return null;
      }
      if (rows.size() > 1 || rows.get(0).size() != 1) {
        throw new IllegalArgumentException("Subquery must return one row and one column: " + e);
      }
      return rows.get(0).values().iterator().next();
    }
    if (e instanceof ExistsExpression) {
      ExistsExpression exists = (ExistsExpression) e;
      if (!(exists.getRightExpression() instanceof ParenthesedSelect)) {
        throw new IllegalArgumentException("Unsupported EXISTS expression: " + e);
      }
      boolean empty =
          ctx.subquery((ParenthesedSelect) exists.getRightExpression(), group).isEmpty();
      return empty == exists.isNot();
    }
    if (e instanceof NotExpression) {
      Object v = eval(((NotExpression) e).getExpression(), group, ctx);
      return v == null ? null : !isTrue(v);
    }
    if (e instanceof AndExpression) {
      Object l = eval(((AndExpression) e).getLeftExpression(), group, ctx);
      Object r = eval(((AndExpression) e).getRightExpression(), group, ctx);
      if (Boolean.FALSE.equals(l) || Boolean.FALSE.equals(r)) {
        return false;
      }
      return l == null || r == null ? null : Boolean.TRUE;
    }
    if (e instanceof OrExpression) {
      Object l = eval(((OrExpression) e).getLeftExpression(), group, ctx);
      Object r = eval(((OrExpression) e).getRightExpression(), group, ctx);
      if (isTrue(l) || isTrue(r)) {
        return true;
      }
      return l == null || r == null ? null : Boolean.FALSE;
    }
    if (e instanceof IsNullExpression) {
      IsNullExpression n = (IsNullExpression) e;
      return (eval(n.getLeftExpression(), group, ctx) == null) != n.isNot();
    }
    if (e instanceof IsBooleanExpression) {
      IsBooleanExpression b = (IsBooleanExpression) e;
      Object v = eval(b.getLeftExpression(), group, ctx);
      boolean matches = b.isTrue() ? Boolean.TRUE.equals(v) : Boolean.FALSE.equals(v);
      return matches != b.isNot();
    }
    if (e instanceof LikeExpression) {
      LikeExpression like = (LikeExpression) e;
      Object v = eval(like.getLeftExpression(), group, ctx);
      Object p = eval(like.getRightExpression(), group, ctx);
      if (v == null || p == null) {
        return null;
      }
      String escape =
          like.getEscape() == null ? "\\" : String.valueOf(eval(like.getEscape(), group, ctx));
      if (like.getLikeKeyWord() == LikeExpression.KeyWord.SIMILAR_TO) {
        boolean similar =
            similarPattern(String.valueOf(p), escape).matcher(String.valueOf(v)).matches();
        return similar != like.isNot();
      }
      if (like.getLikeKeyWord() != LikeExpression.KeyWord.LIKE
          && like.getLikeKeyWord() != LikeExpression.KeyWord.ILIKE) {
        throw new IllegalArgumentException("Unsupported expression: " + e);
      }
      Pattern pattern = likePattern(String.valueOf(p), escape);
      if (like.getLikeKeyWord() == LikeExpression.KeyWord.ILIKE) {
        pattern =
            Pattern.compile(
                pattern.pattern(),
                pattern.flags() | Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE);
      }
      boolean matches = pattern.matcher(String.valueOf(v)).matches();
      return matches != like.isNot();
    }
    if (e instanceof InExpression) {
      InExpression in = (InExpression) e;
      Object v = eval(in.getLeftExpression(), group, ctx);
      List<Object> values = new ArrayList<>();
      if (in.getRightExpression() instanceof ParenthesedSelect) {
        for (Map<String, Object> row :
            ctx.subquery((ParenthesedSelect) in.getRightExpression(), group)) {
          values.add(row.values().iterator().next());
        }
      } else if (in.getRightExpression() instanceof ExpressionList) {
        for (Expression item : (ExpressionList<?>) in.getRightExpression()) {
          values.add(eval(item, group, ctx));
        }
      } else {
        throw new IllegalArgumentException("Unsupported IN expression: " + e);
      }
      if (v == null) {
        return null;
      }
      boolean sawNull = false;
      for (Object x : values) {
        if (x == null) {
          sawNull = true;
        } else if (compare(v, x) == 0) {
          return !in.isNot();
        }
      }
      return sawNull ? null : Boolean.valueOf(in.isNot());
    }
    if (e instanceof Between) {
      Between b = (Between) e;
      Object v = eval(b.getLeftExpression(), group, ctx);
      Object lo = eval(b.getBetweenExpressionStart(), group, ctx);
      Object hi = eval(b.getBetweenExpressionEnd(), group, ctx);
      if (v == null || lo == null || hi == null) {
        return null;
      }
      if (b.isUsingSymmetric() && compare(lo, hi) > 0) {
        Object swap = lo;
        lo = hi;
        hi = swap;
      }
      return (compare(v, lo) >= 0 && compare(v, hi) <= 0) != b.isNot();
    }
    if (e instanceof ComparisonOperator) {
      ComparisonOperator op = (ComparisonOperator) e;
      Object l = eval(op.getLeftExpression(), group, ctx);
      if (op.getRightExpression() instanceof AnyComparisonExpression) {
        AnyComparisonExpression any = (AnyComparisonExpression) op.getRightExpression();
        List<Object> values = new ArrayList<>();
        for (Map<String, Object> row : ctx.subquery(any.getSelect(), group)) {
          values.add(row.values().iterator().next());
        }
        return anyOrAll(op, l, any.getAnyType() == AnyType.ALL, values);
      }
      Function quantifier = QueryParser.arrayQuantifier(op.getRightExpression());
      if (quantifier != null) {
        // op ANY(array) / op ALL(array), for an array computed at run time
        Object array = eval(quantifier.getParameters().get(0), group, ctx);
        if (array == null) {
          return null;
        }
        return anyOrAll(op, l, quantifier.getName().equalsIgnoreCase("ALL"), elements(array));
      }
      Object r = eval(op.getRightExpression(), group, ctx);
      if (l == null || r == null) {
        return null;
      }
      if (l instanceof List && r instanceof List) {
        Integer order = rowCompare((List<?>) l, (List<?>) r);
        if (order == null) {
          return null;
        }
        l = order;
        r = 0;
      }
      Boolean result = compared(op, compare(l, r));
      if (result != null) {
        return result;
      }
    }
    if (e instanceof Concat) {
      Object l = eval(((Concat) e).getLeftExpression(), group, ctx);
      Object r = eval(((Concat) e).getRightExpression(), group, ctx);
      if (l == null || r == null) {
        return null;
      }
      if (l instanceof Json.Value || r instanceof Json.Value) {
        return new Json.Value(Json.concat(Json.of(l).node, Json.of(r).node), true, null);
      }
      return text(l) + text(r);
    }
    if (e instanceof Addition
        || e instanceof Subtraction
        || e instanceof Multiplication
        || e instanceof Division
        || e instanceof Modulo) {
      return arithmetic((BinaryExpression) e, group, ctx);
    }
    if (e instanceof net.sf.jsqlparser.expression.JsonExpression) {
      return json((net.sf.jsqlparser.expression.JsonExpression) e, group, ctx);
    }
    if (e instanceof net.sf.jsqlparser.expression.operators.relational.JsonOperator) {
      net.sf.jsqlparser.expression.operators.relational.JsonOperator op =
          (net.sf.jsqlparser.expression.operators.relational.JsonOperator) e;
      Object l = eval(op.getLeftExpression(), group, ctx);
      Object r = eval(op.getRightExpression(), group, ctx);
      if (l == null || r == null) {
        return null;
      }
      Json.Value a = Json.of(l);
      switch (op.getStringExpression()) {
        case "@>":
          return Json.contains(a.node, Json.of(r).node);
        case "<@":
          return Json.contains(Json.of(r).node, a.node);
        case "?":
          return Json.exists(a.node, text(r));
        case "?|":
        case "?&":
          {
            boolean all = op.getStringExpression().equals("?&");
            for (Object key : elements(r)) {
              if (Json.exists(a.node, text(key)) != all) {
                return !all;
              }
            }
            return all;
          }
        default:
          throw new IllegalArgumentException("Unsupported JSON operator: " + e);
      }
    }
    if (e instanceof net.sf.jsqlparser.expression.operators.relational.IsDistinctExpression) {
      net.sf.jsqlparser.expression.operators.relational.IsDistinctExpression d =
          (net.sf.jsqlparser.expression.operators.relational.IsDistinctExpression) e;
      Object l = eval(d.getLeftExpression(), group, ctx);
      Object r = eval(d.getRightExpression(), group, ctx);
      boolean distinct = l == null || r == null ? (l == null) != (r == null) : compare(l, r) != 0;
      return distinct != d.isNot();
    }
    if (e instanceof net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd
        || e instanceof net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr
        || e instanceof net.sf.jsqlparser.expression.operators.arithmetic.BitwiseLeftShift
        || e instanceof net.sf.jsqlparser.expression.operators.arithmetic.BitwiseRightShift
        || e instanceof net.sf.jsqlparser.expression.operators.relational.Intersects) {
      // & | << >> and PostgreSQL's # (XOR), on integers
      BinaryExpression b = (BinaryExpression) e;
      Object l = eval(b.getLeftExpression(), group, ctx);
      Object r = eval(b.getRightExpression(), group, ctx);
      if (l == null || r == null) {
        return null;
      }
      long x = integral(l, e);
      long y = integral(r, e);
      if (e instanceof net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd) {
        return x & y;
      }
      if (e instanceof net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr) {
        return x | y;
      }
      if (e instanceof net.sf.jsqlparser.expression.operators.arithmetic.BitwiseLeftShift) {
        return x << y;
      }
      if (e instanceof net.sf.jsqlparser.expression.operators.arithmetic.BitwiseRightShift) {
        return x >> y;
      }
      return x ^ y;
    }
    if (e instanceof net.sf.jsqlparser.expression.ArrayExpression) {
      net.sf.jsqlparser.expression.ArrayExpression a =
          (net.sf.jsqlparser.expression.ArrayExpression) e;
      Object array = eval(a.getObjExpression(), group, ctx);
      if (array == null) {
        return null;
      }
      List<?> items = elements(array);
      Expression index = a.getIndexExpression();
      Expression startIndex = a.getStartIndexExpression();
      Expression stopIndex = a.getStopIndexExpression();
      if (index instanceof net.sf.jsqlparser.expression.JsonExpression
          && ((net.sf.jsqlparser.expression.JsonExpression) index).getIdents().size() == 1) {
        // JSQLParser reads the slice [from:to] as a JSON path "from : to"
        net.sf.jsqlparser.expression.JsonExpression slice =
            (net.sf.jsqlparser.expression.JsonExpression) index;
        startIndex = slice.getExpression();
        stopIndex = slice.getIdents().get(0);
        index = null;
      }
      if (index != null) {
        Object i = eval(index, group, ctx);
        if (i == null) {
          return null;
        }
        long k = integral(i, e);
        return k < 1 || k > items.size() ? null : items.get((int) k - 1);
      }
      // a slice [from:to], 1-based and inclusive, clipped to the array as PostgreSQL does
      Object from = startIndex == null ? null : eval(startIndex, group, ctx);
      Object to = stopIndex == null ? null : eval(stopIndex, group, ctx);
      int start = from == null ? 1 : (int) Math.max(1, integral(from, e));
      int end = to == null ? items.size() : (int) Math.min(items.size(), integral(to, e));
      return start > end ? new ArrayList<>() : new ArrayList<Object>(items.subList(start - 1, end));
    }
    if (e instanceof ParenthesedExpressionList) {
      // a row value such as (a, b), compared element-wise
      List<Object> row = new ArrayList<>();
      for (Expression item : (ParenthesedExpressionList<?>) e) {
        row.add(eval(item, group, ctx));
      }
      return row;
    }
    throw new IllegalArgumentException("Unsupported expression: " + e);
  }

  /**
   * {@code j -> 'a' ->> 'b'}: JSQLParser nests a chain of JSON operators to the right, so the steps
   * are flattened and applied left to right. {@code ->} and {@code #>} yield JSON, {@code ->>} and
   * {@code #>>} text.
   */
  private static Object json(
      net.sf.jsqlparser.expression.JsonExpression j, List<Map<String, Object>> group, Context ctx) {
    List<Map.Entry<Expression, String>> steps = new ArrayList<>();
    jsonSteps(j, steps);
    Object cur = eval(j.getExpression(), group, ctx);
    for (Map.Entry<Expression, String> step : steps) {
      if (cur == null) {
        return null;
      }
      Json.Value v = Json.of(cur);
      Object key = eval(step.getKey(), group, ctx);
      com.fasterxml.jackson.databind.JsonNode node;
      switch (step.getValue()) {
        case "->":
        case "->>":
          node = Json.get(v.node, key);
          break;
        case "#>":
        case "#>>":
          node = key == null ? null : Json.path(v.node, elements(key));
          break;
        default:
          throw new IllegalArgumentException("Unsupported JSON operator: " + step.getValue());
      }
      boolean asText = step.getValue().endsWith(">>");
      cur =
          node == null
              ? null
              : asText ? Json.scalarText(node, v.binary) : new Json.Value(node, v.binary, null);
    }
    return cur;
  }

  private static void jsonSteps(
      net.sf.jsqlparser.expression.JsonExpression j, List<Map.Entry<Expression, String>> steps) {
    for (Map.Entry<Expression, String> ident : j.getIdentList()) {
      if (ident.getKey() instanceof net.sf.jsqlparser.expression.JsonExpression) {
        net.sf.jsqlparser.expression.JsonExpression inner =
            (net.sf.jsqlparser.expression.JsonExpression) ident.getKey();
        steps.add(new java.util.AbstractMap.SimpleEntry<>(inner.getExpression(), ident.getValue()));
        jsonSteps(inner, steps);
      } else {
        steps.add(ident);
      }
    }
  }

  /** The last operator of a JSON chain, which decides whether the result is JSON or text. */
  static String lastJsonOperator(net.sf.jsqlparser.expression.JsonExpression j) {
    List<Map.Entry<Expression, String>> idents = j.getIdentList();
    Map.Entry<Expression, String> last = idents.get(idents.size() - 1);
    return last.getKey() instanceof net.sf.jsqlparser.expression.JsonExpression
        ? lastJsonOperator((net.sf.jsqlparser.expression.JsonExpression) last.getKey())
        : last.getValue();
  }

  /** An integer operand, for bitwise operators and subscripts. */
  private static long integral(Object v, Expression e) {
    if (!isIntegral(v)) {
      throw new IllegalArgumentException("Operator requires integer operands: " + e);
    }
    return ((Number) v).longValue();
  }

  /** Rows compared element-wise; null when a pair decides nothing because one side is NULL. */
  @Nullable
  static Integer rowCompare(List<?> a, List<?> b) {
    if (a.size() != b.size()) {
      throw new IllegalArgumentException("Unequal number of entries in row expressions");
    }
    for (int i = 0; i < a.size(); i++) {
      if (a.get(i) == null || b.get(i) == null) {
        return null;
      }
      int c = compare(a.get(i), b.get(i));
      if (c != 0) {
        return c;
      }
    }
    return 0;
  }

  /**
   * PostgreSQL's SIMILAR TO pattern as a Java regex: {@code %} and {@code _} as in LIKE, the SQL
   * regex operators {@code | * + ? {} () []} kept, everything else literal, whole-string match.
   */
  static Pattern similarPattern(String pattern, String escape) {
    StringBuilder sb = new StringBuilder();
    boolean inClass = false;
    for (int i = 0; i < pattern.length(); i++) {
      char c = pattern.charAt(i);
      if (!escape.isEmpty() && c == escape.charAt(0) && i + 1 < pattern.length()) {
        sb.append(Pattern.quote(String.valueOf(pattern.charAt(++i))));
      } else if (inClass) {
        inClass = c != ']';
        sb.append(c == '\\' ? "\\\\" : String.valueOf(c));
      } else if (c == '%') {
        sb.append(".*");
      } else if (c == '_') {
        sb.append('.');
      } else if (c == '[') {
        inClass = true;
        sb.append(c);
      } else if ("|*+?{}()".indexOf(c) >= 0) {
        sb.append(c);
      } else {
        sb.append(Pattern.quote(String.valueOf(c)));
      }
    }
    return Pattern.compile(sb.toString(), Pattern.DOTALL);
  }

  /** A numeric from its text, with a non-negative scale as PostgreSQL stores it (1e3 is 1000). */
  static BigDecimal decimal(String s) {
    BigDecimal v = new BigDecimal(s.trim());
    return v.scale() < 0 ? v.setScale(0) : v;
  }

  /** A number as a numeric: a double by its shortest decimal digits, as it was written. */
  static BigDecimal toDecimal(Object v) {
    if (v instanceof BigDecimal) {
      return (BigDecimal) v;
    }
    return isIntegral(v) ? BigDecimal.valueOf(((Number) v).longValue()) : decimal(text(v));
  }

  /** Numeric division with PostgreSQL's result scale: at least 16 significant digits. */
  static BigDecimal divide(BigDecimal a, BigDecimal b) {
    if (b.signum() == 0) {
      throw new IllegalArgumentException("division by zero");
    }
    return a.divide(b, divScale(a, b), RoundingMode.HALF_UP);
  }

  /** PostgreSQL's select_div_scale, on base-10000 digit groups as its numeric is stored. */
  private static int divScale(BigDecimal a, BigDecimal b) {
    int[] first1 = leadingGroup(a);
    int[] first2 = leadingGroup(b);
    int qweight = first1[0] - first2[0];
    if (first1[1] <= first2[1]) {
      qweight--;
    }
    int scale = 16 - qweight * 4;
    scale = Math.max(scale, Math.max(a.scale(), b.scale()));
    return Math.max(0, Math.min(scale, 1000));
  }

  /** The weight (power of 10000) and value of the first non-zero base-10000 digit group. */
  private static int[] leadingGroup(BigDecimal x) {
    x = x.abs();
    if (x.signum() == 0) {
      return new int[] {0, 0};
    }
    int intDigits = x.precision() - x.scale(); // digits before the point; 0 or less below 1
    int weight = intDigits > 0 ? (intDigits - 1) / 4 : -((x.scale() - x.precision()) / 4 + 1);
    BigDecimal group = x.movePointLeft(4 * weight).setScale(0, RoundingMode.DOWN);
    return new int[] {weight, group.remainder(BigDecimal.valueOf(10000)).intValue()};
  }

  private static final Pattern TYPE_ARGS = Pattern.compile("^(.*?)\\s*\\(([^)]*)\\)(.*)$");

  /** The cast's type with its arguments, such as {@code numeric(10,2)}. */
  private static String castType(net.sf.jsqlparser.statement.create.table.ColDataType t) {
    String name = t.getDataType();
    List<String> args = t.getArgumentsStringList();
    java.util.regex.Matcher inline = TYPE_ARGS.matcher(name);
    if (args == null && inline.matches()) {
      // JSQLParser 5 keeps the arguments inside the name: numeric (10, 2)
      name = inline.group(1) + inline.group(3);
      args = Arrays.asList(inline.group(2).split("\\s*,\\s*"));
    }
    boolean array = t.getArrayData() != null && !t.getArrayData().isEmpty(); // int[] keeps [] apart
    return name
        + (args == null || args.isEmpty() ? "" : "(" + String.join(",", args) + ")")
        + (array ? "[]" : "");
  }

  /** Compares two values; a text value is converted to the other value's type first. */
  static int compare(Object a, Object b) {
    if (a instanceof Json.Value || b instanceof Json.Value) {
      Json.Value x = Json.of(a);
      Json.Value y = Json.of(b);
      return x.equals(y) ? 0 : x.toString().compareTo(y.toString());
    }
    if (a instanceof List && b instanceof List) {
      List<?> x = (List<?>) a;
      List<?> y = (List<?>) b;
      for (int i = 0; i < Math.min(x.size(), y.size()); i++) {
        if (x.get(i) == null || y.get(i) == null) {
          if ((x.get(i) == null) != (y.get(i) == null)) {
            return x.get(i) == null ? 1 : -1;
          }
          continue;
        }
        int c = compare(x.get(i), y.get(i));
        if (c != 0) {
          return c;
        }
      }
      return Integer.compare(x.size(), y.size());
    }
    if (a instanceof String && !(b instanceof String)) {
      a = coerce((String) a, b);
    } else if (b instanceof String && !(a instanceof String)) {
      b = coerce((String) b, a);
    }
    if (a instanceof Number && b instanceof Number) {
      if (!isFinite(a) || !isFinite(b)) {
        // PostgreSQL orders NaN above Infinity and treats NaN as equal to itself, as Double does
        return Double.compare(((Number) a).doubleValue(), ((Number) b).doubleValue());
      }
      return new BigDecimal(a.toString()).compareTo(new BigDecimal(b.toString()));
    }
    if (a instanceof Comparable && a.getClass().equals(b.getClass())) {
      @SuppressWarnings("unchecked")
      int c = ((Comparable<Object>) a).compareTo(b);
      return c;
    }
    LocalDateTime ta = asTimestamp(a);
    LocalDateTime tb = asTimestamp(b);
    if (ta != null && tb != null) {
      return ta.compareTo(tb); // timestamp, timestamptz (UTC) and date compare as timestamps
    }
    throw new IllegalArgumentException("Cannot compare " + a + " with " + b);
  }

  /** Accepts the boolean spellings PostgreSQL accepts. */
  static boolean parseBoolean(String s) {
    switch (s.trim().toLowerCase(Locale.ROOT)) {
      case "t":
      case "true":
      case "1":
      case "y":
      case "yes":
      case "on":
        return true;
      case "f":
      case "false":
      case "0":
      case "n":
      case "no":
      case "off":
        return false;
      default:
        throw new IllegalArgumentException("Invalid boolean: " + s);
    }
  }

  private static Object coerce(String s, Object target) {
    try {
      if (target instanceof Number) {
        return new BigDecimal(s);
      }
      if (target instanceof Boolean) {
        return parseBoolean(s);
      }
      if (target instanceof LocalDate) {
        return LocalDate.parse(s);
      }
      if (target instanceof LocalTime) {
        return LocalTime.parse(s);
      }
      if (target instanceof LocalDateTime) {
        return LocalDateTime.parse(s.replace(' ', 'T'));
      }
      if (target instanceof Instant) {
        return OffsetDateTime.parse(s.replace(' ', 'T')).toInstant();
      }
      if (target instanceof ByteBuffer) {
        return ByteBuffer.wrap(QueryParser.hex(s));
      }
    } catch (RuntimeException e) {
      throw new IllegalArgumentException(
          "Cannot convert '" + s + "' to " + target.getClass().getSimpleName(), e);
    }
    return s;
  }

  @Nullable
  private static Object column(Column c, List<Map<String, Object>> group, Context ctx) {
    String raw = QueryParser.unquote(c.getColumnName());
    // JSQLParser keeps an array subscript such as conkey[1] inside the column name
    int bracket = raw.indexOf('[');
    String name = bracket < 0 ? raw : raw.substring(0, bracket);
    if (c.getTable() == null && QueryParser.isBooleanLiteral(name)) {
      return name.equalsIgnoreCase("true");
    }
    Object value = columnValue(c, name, group, ctx);
    if (bracket < 0 && c.getArrayConstructor() == null) {
      return value;
    }
    Object subscript =
        bracket >= 0
            ? Long.parseLong(raw.substring(bracket + 1, raw.length() - 1).trim())
            : eval(c.getArrayConstructor().getExpressions().get(0), group, ctx);
    if (!(subscript instanceof Number)) {
      return null;
    }
    int index = ((Number) subscript).intValue();
    List<?> array = value instanceof List ? (List<?>) value : null;
    return array == null || index < 1 || index > array.size() ? null : array.get(index - 1);
  }

  @Nullable
  private static Object columnValue(
      Column c, String name, List<Map<String, Object>> group, Context ctx) {
    Map<String, Object> row = group.isEmpty() ? Collections.emptyMap() : group.get(0);
    List<Map<String, Object>> candidates =
        ctx.outer == null ? Collections.singletonList(row) : Arrays.asList(row, ctx.outer);
    if (c.getTable() != null) {
      String key = QueryParser.unquote(c.getTable().getName()) + "." + name;
      for (Map<String, Object> candidate : candidates) {
        if (candidate.containsKey(key)) {
          return candidate.get(key);
        }
      }
    } else {
      // Not resolved by the planner (e.g. ORDER BY on an output name): match the column part
      for (Map<String, Object> candidate : candidates) {
        for (Map.Entry<String, Object> entry : candidate.entrySet()) {
          if (entry.getKey().equals(name) || entry.getKey().endsWith("." + name)) {
            return entry.getValue();
          }
        }
      }
    }
    if (c.getTable() == null) {
      // SQL keywords that read like columns, as drivers send them (pgjdbc: SELECT current_catalog)
      switch (name.toLowerCase(Locale.ROOT)) {
        case "current_catalog":
          return ctx.catalog == null ? null : ctx.catalog.database;
        case "current_schema":
          return ctx.catalog == null ? null : "public"; // the connected namespace shows as public
        case "current_user":
        case "session_user":
        case "current_role":
          return Catalog.OWNER;
        default:
          break;
      }
    }
    throw new IllegalArgumentException("Unknown column: " + c);
  }

  private static Object temporal(DateTimeLiteralExpression e) {
    String v = e.getValue();
    v = v.startsWith("'") ? v.substring(1, v.length() - 1) : v;
    switch (e.getType()) {
      case DATE:
        return LocalDate.parse(v);
      case TIME:
        return LocalTime.parse(v);
      case TIMESTAMP:
        return LocalDateTime.parse(v.replace(' ', 'T'));
      default:
        return OffsetDateTime.parse(v.replace(' ', 'T')).toInstant();
    }
  }

  private static Object now(String keyword) {
    switch (keyword.toUpperCase(Locale.ROOT).replace("()", "")) {
      case "CURRENT_TIMESTAMP":
      case "NOW":
        return Instant.now();
      case "CURRENT_DATE":
        return LocalDate.now(ZoneOffset.UTC);
      case "CURRENT_TIME":
      case "LOCALTIME":
        return LocalTime.now(ZoneOffset.UTC);
      case "LOCALTIMESTAMP":
        return LocalDateTime.now(ZoneOffset.UTC);
      default:
        throw new IllegalArgumentException("Unsupported expression: " + keyword);
    }
  }

  @Nullable
  private static Object negate(@Nullable Object v) {
    if (v == null) {
      return null;
    }
    if (isIntegral(v)) {
      return -((Number) v).longValue();
    }
    if (v instanceof BigDecimal) {
      return ((BigDecimal) v).negate();
    }
    if (v instanceof Interval) {
      return ((Interval) v).negate();
    }
    if (v instanceof Number) {
      return -((Number) v).doubleValue();
    }
    throw new IllegalArgumentException("Cannot negate " + v);
  }

  /** False for a floating-point NaN or Infinity, which BigDecimal cannot represent. */
  static boolean isFinite(Object v) {
    return !(v instanceof Double || v instanceof Float)
        || Double.isFinite(((Number) v).doubleValue());
  }

  private static boolean isIntegral(Object v) {
    return v instanceof Integer || v instanceof Long || v instanceof Short || v instanceof Byte;
  }

  static String text(Object v) {
    if (v instanceof Json.Value) {
      return v.toString();
    }
    if (v instanceof BigDecimal) {
      return ((BigDecimal) v).toPlainString();
    }
    if (v instanceof Interval) {
      return v.toString();
    }
    if (v instanceof Catalog.Int2Vector) {
      return v.toString(); // pg_index.indkey prints as "1 2"
    }
    if (v instanceof List) {
      StringBuilder sb = new StringBuilder("{");
      for (Object item : (List<?>) v) {
        if (sb.length() > 1) {
          sb.append(',');
        }
        if (item == null) {
          sb.append("NULL");
          continue;
        }
        String s = text(item);
        boolean quote =
            s.isEmpty()
                || s.equalsIgnoreCase("NULL")
                || s.chars()
                    .anyMatch(
                        c ->
                            c == '{'
                                || c == '}'
                                || c == ','
                                || c == '"'
                                || c == '\\'
                                || Character.isWhitespace(c));
        sb.append(quote ? "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"" : s);
      }
      return sb.append('}').toString();
    }
    if (v instanceof LocalTime) {
      LocalTime t = (LocalTime) v;
      return String.format("%02d:%02d:%02d", t.getHour(), t.getMinute(), t.getSecond())
          + fraction(t.getNano());
    }
    if (v instanceof LocalDateTime) {
      return timestampText((LocalDateTime) v);
    }
    if (v instanceof Instant) {
      // Always UTC: PostgreSQL prints the offset as +00, with minutes only when they are not zero
      return timestampText(LocalDateTime.ofInstant((Instant) v, ZoneOffset.UTC)) + "+00";
    }
    if (v instanceof ByteBuffer) {
      ByteBuffer b = ((ByteBuffer) v).duplicate();
      byte[] bytes = new byte[b.remaining()];
      b.get(bytes);
      return "\\x" + hex(bytes);
    }
    if (v instanceof byte[]) {
      return "\\x" + hex((byte[]) v);
    }
    if (v instanceof Double) {
      return floatText((Double) v, 15);
    }
    if (v instanceof Float) {
      return floatText((Float) v, 6);
    }
    return String.valueOf(v);
  }

  /** PostgreSQL's timestamp text: the date and time, and the fraction without trailing zeros. */
  static String timestampText(LocalDateTime t) {
    return String.format(
            "%04d-%02d-%02d %02d:%02d:%02d",
            t.getYear(),
            t.getMonthValue(),
            t.getDayOfMonth(),
            t.getHour(),
            t.getMinute(),
            t.getSecond())
        + fraction(t.getNano());
  }

  private static String fraction(int nanos) {
    return nanos == 0 ? "" : String.format(".%06d", nanos / 1000).replaceAll("0+$", "");
  }

  static String hex(byte[] bytes) {
    StringBuilder sb = new StringBuilder();
    for (byte b : bytes) {
      sb.append(String.format("%02x", b));
    }
    return sb.toString();
  }

  /**
   * A float in PostgreSQL's output format: the shortest digits that round-trip, in plain notation
   * for decimal exponents from -4 up to {@code maxPlainExponent} - 1 and as {@code 1.5e+20}
   * otherwise.
   */
  private static String floatText(double d, int maxPlainExponent) {
    if (Double.isNaN(d)) {
      return "NaN";
    }
    if (Double.isInfinite(d)) {
      return d > 0 ? "Infinity" : "-Infinity";
    }
    if (d == 0) {
      return 1 / d < 0 ? "-0" : "0";
    }
    // Float.toString keeps a float's shortest digits; widening to double first would not
    BigDecimal b =
        new BigDecimal(maxPlainExponent == 6 ? Float.toString((float) d) : Double.toString(d))
            .stripTrailingZeros();
    int exponent = b.precision() - b.scale() - 1;
    if (exponent >= -4 && exponent < maxPlainExponent) {
      return b.toPlainString();
    }
    String digits = b.unscaledValue().abs().toString();
    int e = Math.abs(exponent);
    return (b.signum() < 0 ? "-" : "")
        + digits.charAt(0)
        + (digits.length() > 1 ? "." + digits.substring(1) : "")
        + (exponent < 0 ? "e-" : "e+")
        + (e < 10 ? "0" : "")
        + e;
  }

  @Nullable
  private static Object arithmetic(
      BinaryExpression e, List<Map<String, Object>> group, Context ctx) {
    Object l = eval(e.getLeftExpression(), group, ctx);
    Object r = eval(e.getRightExpression(), group, ctx);
    if (l == null || r == null) {
      return null;
    }
    if (l instanceof Json.Value && e instanceof Subtraction) {
      return new Json.Value(Json.delete(((Json.Value) l).node, r), true, null); // jsonb - key
    }
    if (l instanceof LocalDate || r instanceof LocalDate) {
      // date + integer, date - integer and date - date (a number of days), as in PostgreSQL
      if (l instanceof LocalDate && r instanceof LocalDate && e instanceof Subtraction) {
        return (int) java.time.temporal.ChronoUnit.DAYS.between((LocalDate) r, (LocalDate) l);
      }
      if (l instanceof LocalDate
          && isIntegral(r)
          && (e instanceof Addition || e instanceof Subtraction)) {
        long days = ((Number) r).longValue();
        return ((LocalDate) l).plusDays(e instanceof Addition ? days : -days);
      }
      if (isIntegral(l) && r instanceof LocalDate && e instanceof Addition) {
        return ((LocalDate) r).plusDays(((Number) l).longValue());
      }
    }
    Object temporal = temporalArithmetic(e, l, r);
    if (temporal != null) {
      return temporal;
    }
    if (!(l instanceof Number) || !(r instanceof Number)) {
      throw new IllegalArgumentException("Arithmetic on non-numeric values: " + e);
    }
    boolean division = e instanceof Division || e instanceof Modulo;
    if (division && ((Number) r).doubleValue() == 0) {
      throw new IllegalArgumentException("division by zero");
    }
    if (isIntegral(l) && isIntegral(r)) {
      long a = ((Number) l).longValue();
      long b = ((Number) r).longValue();
      long v;
      if (e instanceof Addition) {
        v = Math.addExact(a, b);
      } else if (e instanceof Subtraction) {
        v = Math.subtractExact(a, b);
      } else if (e instanceof Multiplication) {
        v = Math.multiplyExact(a, b);
      } else if (e instanceof Modulo) {
        v = a % b;
      } else if (a == Long.MIN_VALUE && b == -1) {
        throw new ArithmeticException("bigint out of range");
      } else {
        v = a / b;
      }
      // integer op integer stays integer in PostgreSQL and fails on overflow
      if (isInt(l, e.getLeftExpression()) && isInt(r, e.getRightExpression())) {
        if (v < Integer.MIN_VALUE || v > Integer.MAX_VALUE) {
          throw new ArithmeticException("integer out of range");
        }
        return (int) v;
      }
      return v;
    }
    if (!(l instanceof Double || l instanceof Float || r instanceof Double || r instanceof Float)) {
      // numeric with numeric or an integer stays numeric: exact, with PostgreSQL's result scale
      BigDecimal a = toDecimal(l);
      BigDecimal b = toDecimal(r);
      if (e instanceof Addition) {
        return a.add(b);
      }
      if (e instanceof Subtraction) {
        return a.subtract(b);
      }
      if (e instanceof Multiplication) {
        return a.multiply(b);
      }
      return e instanceof Modulo ? a.remainder(b) : divide(a, b);
    }
    double a = ((Number) l).doubleValue();
    double b = ((Number) r).doubleValue();
    if (e instanceof Addition) {
      return a + b;
    }
    if (e instanceof Subtraction) {
      return a - b;
    }
    if (e instanceof Multiplication) {
      return a * b;
    }
    return e instanceof Modulo ? a % b : a / b;
  }

  /**
   * Interval arithmetic: a timestamp, date or time plus or minus an interval, interval with
   * interval, interval times or divided by a number, and the difference of two timestamps or times,
   * which is an interval. Null when the operands are not of these kinds.
   */
  @Nullable
  private static Object temporalArithmetic(BinaryExpression e, Object l, Object r) {
    boolean add = e instanceof Addition;
    boolean sub = e instanceof Subtraction;
    if (r instanceof Interval && (add || sub)) {
      Interval iv = sub ? ((Interval) r).negate() : (Interval) r;
      if (l instanceof Interval) {
        return ((Interval) l).plus(iv);
      }
      Object shifted = shift(l, iv);
      if (shifted != null) {
        return shifted;
      }
    }
    if (l instanceof Interval && add) {
      Object shifted = shift(r, (Interval) l);
      if (shifted != null) {
        return shifted;
      }
    }
    if (l instanceof Interval
        && r instanceof Number
        && (e instanceof Multiplication || e instanceof Division)) {
      double f = ((Number) r).doubleValue();
      if (e instanceof Division && f == 0) {
        throw new IllegalArgumentException("division by zero");
      }
      return ((Interval) l).times(e instanceof Division ? 1 / f : f);
    }
    if (r instanceof Interval && l instanceof Number && e instanceof Multiplication) {
      return ((Interval) r).times(((Number) l).doubleValue());
    }
    if (sub) {
      LocalDateTime a = asTimestamp(l);
      LocalDateTime b = asTimestamp(r);
      if (a != null && b != null && !(l instanceof LocalDate && r instanceof LocalDate)) {
        return Interval.between(b, a);
      }
      if (l instanceof LocalTime && r instanceof LocalTime) {
        return new Interval(
            0, 0, java.time.temporal.ChronoUnit.MICROS.between((LocalTime) r, (LocalTime) l));
      }
    }
    return null;
  }

  /** A timestamp, timestamptz, date or time moved by an interval; null for other values. */
  @Nullable
  private static Object shift(Object v, Interval iv) {
    if (v instanceof LocalDateTime) {
      return iv.addTo((LocalDateTime) v);
    }
    if (v instanceof Instant) {
      return iv.addTo((Instant) v);
    }
    if (v instanceof LocalDate) {
      return iv.addTo(((LocalDate) v).atStartOfDay()); // date + interval is a timestamp
    }
    if (v instanceof LocalTime) {
      return iv.addTo((LocalTime) v);
    }
    return null;
  }

  /** A timestamp, timestamptz (in UTC) or date as a local timestamp; null for other values. */
  @Nullable
  private static LocalDateTime asTimestamp(Object v) {
    if (v instanceof LocalDateTime) {
      return (LocalDateTime) v;
    }
    if (v instanceof Instant) {
      return LocalDateTime.ofInstant((Instant) v, ZoneOffset.UTC);
    }
    if (v instanceof LocalDate) {
      return ((LocalDate) v).atStartOfDay();
    }
    return null;
  }

  /**
   * {@code AT TIME ZONE}: a timestamptz becomes the local timestamp in the zone, and a timestamp
   * (taken as local time in the zone) becomes a timestamptz.
   */
  @Nullable
  private static Object atTimeZone(@Nullable Object v, @Nullable Object zone) {
    if (v == null || zone == null) {
      return null;
    }
    java.time.ZoneId z;
    if (zone instanceof Interval) {
      z = ZoneOffset.ofTotalSeconds((int) (((Interval) zone).micros / 1_000_000L));
    } else {
      String name = text(zone);
      try {
        z = java.time.ZoneId.of(name, java.time.ZoneId.SHORT_IDS);
      } catch (java.time.DateTimeException notAZone) {
        throw new IllegalArgumentException("time zone \"" + name + "\" not recognized");
      }
    }
    if (v instanceof Instant) {
      return LocalDateTime.ofInstant((Instant) v, z);
    }
    LocalDateTime local = asTimestamp(v);
    if (local != null) {
      return local.atZone(z).toInstant();
    }
    throw new IllegalArgumentException("AT TIME ZONE needs a timestamp: " + text(v));
  }

  /** An INT value, or an integer literal small enough to be int4 in PostgreSQL. */
  private static boolean isInt(Object v, Expression e) {
    return v instanceof Integer
        || (e instanceof LongValue && ((LongValue) e).getBigIntegerValue().bitLength() < 32);
  }

  /** An integer literal as a long, or as a numeric beyond bigint. */
  private static Object integer(BigInteger v) {
    return v.bitLength() < 64 ? (Object) v.longValue() : (Object) new BigDecimal(v);
  }

  /** The comparison's outcome given {@code compare(left, right)}; null for an unknown operator. */
  @Nullable
  private static Boolean compared(ComparisonOperator op, int c) {
    if (op instanceof EqualsTo) {
      return c == 0;
    }
    if (op instanceof NotEqualsTo) {
      return c != 0;
    }
    if (op instanceof GreaterThan) {
      return c > 0;
    }
    if (op instanceof GreaterThanEquals) {
      return c >= 0;
    }
    if (op instanceof MinorThan) {
      return c < 0;
    }
    if (op instanceof MinorThanEquals) {
      return c <= 0;
    }
    return null;
  }

  /**
   * {@code x op ANY|SOME|ALL (subquery)} with SQL's three-valued logic: ANY is true if some
   * comparison is true, ALL is false if some comparison is false, and otherwise a NULL on either
   * side makes the result NULL.
   */
  @Nullable
  /** The elements of an array value: a list, or PostgreSQL's array text. */
  static List<?> elements(Object array) {
    return array instanceof List
        ? (List<?>) array
        : QueryParser.parseArrayText(String.valueOf(array));
  }

  @Nullable
  private static Boolean anyOrAll(
      ComparisonOperator op, @Nullable Object left, boolean all, List<?> values) {
    boolean sawNull = false;
    for (Object right : values) {
      if (left == null || right == null) {
        sawNull = true;
        continue;
      }
      Boolean result = compared(op, compare(left, right));
      if (result == null) {
        throw new IllegalArgumentException("Unsupported expression: " + op);
      }
      if (result != all) {
        return result;
      }
    }
    return sawNull ? null : all;
  }

  /** {@code EXTRACT(field FROM v)} and {@code date_part}: an integer, or a fraction for seconds. */
  @Nullable
  static Object extract(String field, @Nullable Object v) {
    if (v == null) {
      return null;
    }
    if (v instanceof Interval) {
      return ((Interval) v).extract(field);
    }
    String unit = field.toUpperCase(Locale.ROOT);
    if (v instanceof LocalTime
        && !Arrays.asList("HOUR", "MINUTE", "SECOND", "EPOCH").contains(unit)) {
      throw new IllegalArgumentException("Unit \"" + field + "\" not supported for type time");
    }
    LocalDateTime t =
        v instanceof LocalDateTime
            ? (LocalDateTime) v
            : v instanceof LocalDate
                ? ((LocalDate) v).atStartOfDay()
                : v instanceof LocalTime
                    ? LocalDate.of(1970, 1, 1).atTime((LocalTime) v) // epoch: seconds of the day
                    : v instanceof Instant
                        ? LocalDateTime.ofInstant((Instant) v, ZoneOffset.UTC)
                        : null;
    if (t == null) {
      throw new IllegalArgumentException("Cannot extract " + field + " from " + text(v));
    }
    switch (field.toUpperCase(Locale.ROOT)) {
      case "YEAR":
        return (long) t.getYear();
      case "QUARTER":
        return (long) ((t.getMonthValue() - 1) / 3 + 1);
      case "MONTH":
        return (long) t.getMonthValue();
      case "WEEK":
        return (long) t.get(java.time.temporal.IsoFields.WEEK_OF_WEEK_BASED_YEAR);
      case "DAY":
        return (long) t.getDayOfMonth();
      case "DOW":
        return (long) (t.getDayOfWeek().getValue() % 7); // Sunday is 0
      case "ISODOW":
        return (long) t.getDayOfWeek().getValue();
      case "DOY":
        return (long) t.getDayOfYear();
      case "HOUR":
        return (long) t.getHour();
      case "MINUTE":
        return (long) t.getMinute();
      case "SECOND":
        return BigDecimal.valueOf(t.getSecond()).add(BigDecimal.valueOf(t.getNano() / 1000, 6));
      case "EPOCH":
        return BigDecimal.valueOf(t.toEpochSecond(ZoneOffset.UTC))
            .add(BigDecimal.valueOf(t.getNano() / 1000, 6));
      default:
        throw new IllegalArgumentException("Unsupported EXTRACT field: " + field);
    }
  }

  /** {@code date_trunc(unit, v)}: the timestamp truncated to the start of the unit. */
  @Nullable
  private static Object dateTrunc(String unit, @Nullable Object v) {
    if (v == null) {
      return null;
    }
    LocalDateTime t = v instanceof LocalDate ? ((LocalDate) v).atStartOfDay() : (LocalDateTime) v;
    switch (unit.toLowerCase(Locale.ROOT)) {
      case "year":
        return t.toLocalDate().withDayOfYear(1).atStartOfDay();
      case "quarter":
        return t.toLocalDate()
            .withMonth((t.getMonthValue() - 1) / 3 * 3 + 1)
            .withDayOfMonth(1)
            .atStartOfDay();
      case "month":
        return t.toLocalDate().withDayOfMonth(1).atStartOfDay();
      case "week":
        return t.toLocalDate().minusDays(t.getDayOfWeek().getValue() - 1).atStartOfDay();
      case "day":
        return t.truncatedTo(java.time.temporal.ChronoUnit.DAYS);
      case "hour":
        return t.truncatedTo(java.time.temporal.ChronoUnit.HOURS);
      case "minute":
        return t.truncatedTo(java.time.temporal.ChronoUnit.MINUTES);
      case "second":
        return t.truncatedTo(java.time.temporal.ChronoUnit.SECONDS);
      default:
        throw new IllegalArgumentException("Unsupported date_trunc unit: " + unit);
    }
  }

  /**
   * Orders two sort keys for one ORDER BY element: PostgreSQL puts NULLs last for ASC and first for
   * DESC unless NULLS FIRST/LAST says otherwise.
   */
  static int orderCompare(OrderByElement o, @Nullable Object a, @Nullable Object b) {
    if (a == null || b == null) {
      boolean nullsFirst =
          o.getNullOrdering() == null
              ? !o.isAsc()
              : o.getNullOrdering() == OrderByElement.NullOrdering.NULLS_FIRST;
      return a == b ? 0 : (a == null) == nullsFirst ? -1 : 1;
    }
    return o.isAsc() ? compare(a, b) : -compare(a, b);
  }

  @Nullable
  private static Object caseOf(CaseExpression c, List<Map<String, Object>> group, Context ctx) {
    Object subject =
        c.getSwitchExpression() == null ? null : eval(c.getSwitchExpression(), group, ctx);
    for (WhenClause when : c.getWhenClauses()) {
      Object condition = eval(when.getWhenExpression(), group, ctx);
      boolean matches =
          c.getSwitchExpression() == null
              ? isTrue(condition)
              : subject != null && condition != null && compare(subject, condition) == 0;
      if (matches) {
        return eval(when.getThenExpression(), group, ctx);
      }
    }
    return c.getElseExpression() == null ? null : eval(c.getElseExpression(), group, ctx);
  }

  @Nullable
  static Object cast(String type, @Nullable Object v) {
    if (v == null) {
      return null;
    }
    String t = type.toLowerCase(Locale.ROOT).trim();
    t = t.startsWith("pg_catalog.") ? t.substring("pg_catalog.".length()) : t;
    String s = text(v);
    try {
      if (t.equals("oid") || t.startsWith("reg")) {
        // regclass, regtype, regnamespace: an OID given as a number stays numeric
        return v instanceof Number
            ? ((Number) v).longValue()
            : s.matches("\\d+") ? Long.parseLong(s) : v;
      }
      if (t.equals("interval")) {
        return v instanceof Interval ? v : Interval.parse(s);
      }
      if (t.equals("uuid")) {
        return java.util.UUID.fromString(s.trim()).toString();
      }
      if (t.equals("json") || t.equals("jsonb")) {
        boolean binary = t.equals("jsonb");
        if (v instanceof Json.Value) {
          Json.Value j = (Json.Value) v;
          return j.binary == binary ? j : new Json.Value(j.node, binary, null);
        }
        return Json.cast(s, binary);
      }
      if (t.equals("bytea")) {
        return v instanceof ByteBuffer || v instanceof byte[]
            ? v
            : ByteBuffer.wrap(QueryParser.hex(s));
      }
      if (t.endsWith("[]")) {
        String element = t.substring(0, t.length() - 2).trim();
        List<Object> out = new ArrayList<>();
        for (Object item : elements(v)) {
          out.add(item == null || !(item instanceof String) ? item : cast(element, item));
        }
        return out;
      }
      if (t.equals("bigint") || t.equals("int8")) {
        return v instanceof Number ? rounded((Number) v) : new BigDecimal(s).longValueExact();
      }
      if (t.contains("int")) {
        return v instanceof Number
            ? Math.toIntExact(rounded((Number) v))
            : new BigDecimal(s).intValueExact();
      }
      if (t.startsWith("numeric") || t.startsWith("decimal")) {
        BigDecimal d = v instanceof Number ? toDecimal(v) : decimal(s);
        int comma = t.indexOf(',');
        if (comma > 0) {
          int scale = Integer.parseInt(t.substring(comma + 1, t.indexOf(')')).trim());
          d = d.setScale(scale, RoundingMode.HALF_UP);
        }
        return d;
      }
      if (t.contains("float") || t.contains("double") || t.equals("real")) {
        return v instanceof Number ? ((Number) v).doubleValue() : Double.parseDouble(s);
      }
      if (t.contains("char") || t.equals("text") || t.equals("string")) {
        return s;
      }
      if (t.equals("boolean") || t.equals("bool")) {
        return v instanceof Boolean
            ? v
            : v instanceof Number ? ((Number) v).intValue() != 0 : parseBoolean(s);
      }
      if (t.equals("date")) {
        return v instanceof LocalDate ? v : LocalDate.parse(s);
      }
      if (t.equals("time")) {
        return v instanceof LocalTime ? v : LocalTime.parse(s);
      }
      if (t.equals("timestamptz") || t.contains("with time zone")) {
        return v instanceof Instant ? v : OffsetDateTime.parse(s.replace(' ', 'T')).toInstant();
      }
      if (t.startsWith("timestamp")) {
        return v instanceof LocalDateTime ? v : LocalDateTime.parse(s.replace(' ', 'T'));
      }
    } catch (RuntimeException e) {
      throw new IllegalArgumentException("Cannot cast '" + s + "' to " + type, e);
    }
    throw new IllegalArgumentException("Unsupported cast type: " + type);
  }

  /**
   * A number rounded to an integer the way PostgreSQL casts numeric (half away from zero), failing
   * when out of range. NUMERIC columns are doubles here, so this also applies to them; a true
   * float8 .5 rounds to even in PostgreSQL.
   */
  private static long rounded(Number n) {
    return new BigDecimal(n.toString()).setScale(0, RoundingMode.HALF_UP).longValueExact();
  }

  @Nullable
  private static Object trim(TrimFunction t, List<Map<String, Object>> group, Context ctx) {
    boolean fromForm = t.getFromExpression() != null;
    Object str = eval(fromForm ? t.getFromExpression() : t.getExpression(), group, ctx);
    Object chars =
        fromForm && t.getExpression() != null ? eval(t.getExpression(), group, ctx) : " ";
    if (str == null || chars == null) {
      return null;
    }
    String spec = t.getTrimSpecification() == null ? "BOTH" : t.getTrimSpecification().name();
    return trim(text(str), text(chars), !spec.equals("TRAILING"), !spec.equals("LEADING"));
  }

  private static String trim(String s, String chars, boolean leading, boolean trailing) {
    int start = 0;
    int end = s.length();
    while (leading && start < end && chars.indexOf(s.charAt(start)) >= 0) {
      start++;
    }
    while (trailing && end > start && chars.indexOf(s.charAt(end - 1)) >= 0) {
      end--;
    }
    return s.substring(start, end);
  }

  @Nullable
  private static Object scalar(Function f, List<Map<String, Object>> group, Context ctx) {
    String name = functionName(f);
    if (name.equals("GROUPING")) {
      // grouping(a, b): a bit per argument, 1 when the grouping set left that expression out
      Object outside = group.isEmpty() ? null : group.get(0).get(Operators.GROUPING_KEY);
      long bits = 0;
      if (f.getParameters() != null) {
        for (Expression p : f.getParameters()) {
          boolean left = outside instanceof Set && ((Set<?>) outside).contains(p.toString());
          bits = bits * 2 + (left ? 1 : 0);
        }
      }
      return bits;
    }
    if (name.equals("ARRAY")
        && f.getParameters() != null
        && f.getParameters().size() == 1
        && f.getParameters().get(0) instanceof Select) {
      // ARRAY(SELECT ...): the first column of the subquery as a list
      List<Object> values = new ArrayList<>();
      for (Map<String, Object> row : ctx.subquery((Select) f.getParameters().get(0), group)) {
        values.add(row.values().iterator().next());
      }
      return values;
    }
    List<Object> args = new ArrayList<>();
    if (f.getParameters() != null) {
      for (Expression p : f.getParameters()) {
        args.add(eval(p, group, ctx));
      }
    } else if (f.getNamedParameters() != null) {
      // position(sub IN s), substring(s FROM n FOR m): the operands in written order
      for (Object p : f.getNamedParameters().getExpressions()) {
        args.add(eval((Expression) p, group, ctx));
      }
      if (name.equals("POSITION")) {
        Collections.reverse(args); // the same arguments as strpos(s, sub)
      }
    }
    Object catalogResult = catalogFunction(name, args, ctx);
    if (catalogResult != NOT_A_CATALOG_FUNCTION) {
      return catalogResult;
    }
    switch (name) {
      case "COALESCE":
        for (Object a : args) {
          if (a != null) {
            return a;
          }
        }
        return null;
      case "CONCAT":
        StringBuilder sb = new StringBuilder();
        for (Object a : args) {
          if (a != null) {
            sb.append(text(a));
          }
        }
        return sb.toString();
      case "NULLIF":
        return arg(args, 0) != null
                && arg(args, 1) != null
                && compare(args.get(0), args.get(1)) == 0
            ? null
            : arg(args, 0);
      case "GREATEST":
      case "LEAST":
        {
          // not strict: NULL arguments are ignored, and the result is NULL only if all are
          Object best = null;
          for (Object a : args) {
            if (a != null && (best == null || (compare(a, best) > 0) == name.equals("GREATEST"))) {
              best = a;
            }
          }
          return best;
        }
      case "JSON_BUILD_OBJECT":
      case "JSONB_BUILD_OBJECT":
        return new Json.Value(Json.buildObject(args), name.startsWith("JSONB"), null);
      case "JSON_BUILD_ARRAY":
      case "JSONB_BUILD_ARRAY":
        return new Json.Value(Json.buildArray(args), name.startsWith("JSONB"), null);
      case "NOW":
        return Instant.now();
      case "VERSION":
        return "PostgreSQL 16.0 (ScalarDB frontend)";
      default:
        break;
    }
    // The remaining functions are strict: any NULL argument yields NULL
    for (Object a : args) {
      if (a == null) {
        return null;
      }
    }
    switch (name) {
      case "UPPER":
        return str(args, 0).toUpperCase(Locale.ROOT);
      case "LOWER":
        return str(args, 0).toLowerCase(Locale.ROOT);
      case "GEN_RANDOM_UUID":
        return java.util.UUID.randomUUID().toString();
      case "TO_JSON":
      case "TO_JSONB":
        return new Json.Value(Json.toNode(args.get(0)), name.equals("TO_JSONB"), null);
      case "JSON_TYPEOF":
      case "JSONB_TYPEOF":
        return Json.typeOf(Json.of(args.get(0)).node);
      case "JSON_ARRAY_LENGTH":
      case "JSONB_ARRAY_LENGTH":
        return Json.arrayLength(Json.of(args.get(0)).node);
      case "JSON_EXTRACT_PATH":
      case "JSONB_EXTRACT_PATH":
      case "JSON_EXTRACT_PATH_TEXT":
      case "JSONB_EXTRACT_PATH_TEXT":
        {
          Json.Value v = Json.of(args.get(0));
          com.fasterxml.jackson.databind.JsonNode node =
              Json.path(v.node, args.subList(1, args.size()));
          boolean binary = name.startsWith("JSONB");
          return node == null
              ? null
              : name.endsWith("_TEXT")
                  ? Json.scalarText(node, binary)
                  : new Json.Value(node, binary, null);
        }
      case "JSON_ARRAY_ELEMENTS":
      case "JSONB_ARRAY_ELEMENTS":
      case "JSON_ARRAY_ELEMENTS_TEXT":
      case "JSONB_ARRAY_ELEMENTS_TEXT":
        return Json.elements(
            Json.of(args.get(0)).node, name.startsWith("JSONB"), name.endsWith("_TEXT"));
      case "LENGTH":
      case "CHAR_LENGTH":
      case "CHARACTER_LENGTH":
      case "OCTET_LENGTH":
        {
          Object a = arg(args, 0);
          if (a instanceof ByteBuffer) {
            return (long) ((ByteBuffer) a).remaining();
          }
          if (a instanceof byte[]) {
            return (long) ((byte[]) a).length;
          }
          String s = str(args, 0);
          return (long)
              (name.equals("OCTET_LENGTH")
                  ? s.getBytes(java.nio.charset.StandardCharsets.UTF_8).length
                  : s.codePointCount(0, s.length()));
        }
      case "SUBSTRING":
      case "SUBSTR":
        {
          String s = str(args, 0);
          int from = Math.max(num(args, 1).intValue() - 1, 0);
          int to =
              args.size() > 2 ? Math.min(from + num(args, 2).intValue(), s.length()) : s.length();
          return from >= s.length() ? "" : s.substring(from, Math.max(from, to));
        }
      case "LEFT":
        return str(args, 0).substring(0, Math.min(num(args, 1).intValue(), str(args, 0).length()));
      case "RIGHT":
        return str(args, 0).substring(Math.max(str(args, 0).length() - num(args, 1).intValue(), 0));
      case "REPLACE":
        return str(args, 0).replace(str(args, 1), str(args, 2));
      case "STARTS_WITH":
        return str(args, 0).startsWith(str(args, 1));
      case "POSITION":
      case "STRPOS":
        return (long) (str(args, 0).indexOf(str(args, 1)) + 1);
      case "TRIM":
      case "BTRIM":
        return trim(str(args, 0), args.size() > 1 ? str(args, 1) : " ", true, true);
      case "LTRIM":
        return trim(str(args, 0), args.size() > 1 ? str(args, 1) : " ", true, false);
      case "RTRIM":
        return trim(str(args, 0), args.size() > 1 ? str(args, 1) : " ", false, true);
      case "ABS":
        return isIntegral(args.get(0))
            ? (Object) Math.abs(num(args, 0).longValue())
            : args.get(0) instanceof BigDecimal
                ? (Object) ((BigDecimal) args.get(0)).abs()
                : (Object) Math.abs(num(args, 0).doubleValue());
      case "ROUND":
        {
          int scale = args.size() > 1 ? num(args, 1).intValue() : 0;
          if (!isFinite(args.get(0))) {
            return args.get(0);
          }
          BigDecimal rounded =
              new BigDecimal(num(args, 0).toString()).setScale(scale, RoundingMode.HALF_UP);
          return isIntegral(args.get(0))
              ? (Object) rounded.longValue()
              : args.get(0) instanceof BigDecimal
                  ? (Object) rounded
                  : (Object) rounded.doubleValue();
        }
      case "FLOOR":
        return isIntegral(args.get(0))
            ? args.get(0)
            : args.get(0) instanceof BigDecimal
                ? (Object) ((BigDecimal) args.get(0)).setScale(0, RoundingMode.FLOOR)
                : (Object) Math.floor(num(args, 0).doubleValue());
      case "CEIL":
      case "CEILING":
        return isIntegral(args.get(0))
            ? args.get(0)
            : args.get(0) instanceof BigDecimal
                ? (Object) ((BigDecimal) args.get(0)).setScale(0, RoundingMode.CEILING)
                : (Object) Math.ceil(num(args, 0).doubleValue());
      case "MOD":
        if (isIntegral(args.get(0)) && isIntegral(args.get(1))) {
          return num(args, 0).longValue() % num(args, 1).longValue();
        }
        if (num(args, 0) instanceof Double
            || num(args, 0) instanceof Float
            || num(args, 1) instanceof Double
            || num(args, 1) instanceof Float) {
          return num(args, 0).doubleValue() % num(args, 1).doubleValue();
        }
        return toDecimal(args.get(0)).remainder(toDecimal(args.get(1)));
      case "POWER":
      case "POW":
        return Math.pow(num(args, 0).doubleValue(), num(args, 1).doubleValue());
      case "SQRT":
        return Math.sqrt(num(args, 0).doubleValue());
      case "SIGN":
        {
          double d = num(args, 0).doubleValue();
          return isIntegral(args.get(0))
              ? (Object) (long) Math.signum(d)
              : args.get(0) instanceof BigDecimal
                  ? (Object) BigDecimal.valueOf(((BigDecimal) args.get(0)).signum())
                  : (Object) Math.signum(d);
        }
      case "REVERSE":
        return new StringBuilder(str(args, 0)).reverse().toString();
      case "DATE_PART":
        return extract(str(args, 0), arg(args, 1));
      case "UNNEST":
        return arg(args, 0) == null
            ? Collections.emptyList()
            : new ArrayList<>(elements(args.get(0)));
      case "GENERATE_SUBSCRIPTS":
        {
          List<Object> subscripts = new ArrayList<>();
          int count = arg(args, 0) == null ? 0 : elements(args.get(0)).size();
          for (long i = 1; i <= count; i++) {
            subscripts.add(i);
          }
          return subscripts;
        }
      case "CARDINALITY":
        return arg(args, 0) == null ? null : (Object) (long) elements(args.get(0)).size();
      case "AGE":
        {
          // age(a, b) is the calendar difference; age(a) counts from today's midnight
          Object a = arg(args, 0);
          Object b = args.size() > 1 ? arg(args, 1) : null;
          if (a == null || (args.size() > 1 && b == null)) {
            return null;
          }
          LocalDateTime to =
              args.size() > 1 ? asTimestamp(a) : LocalDate.now(ZoneOffset.UTC).atStartOfDay();
          LocalDateTime from = args.size() > 1 ? asTimestamp(b) : asTimestamp(a);
          if (to == null || from == null) {
            throw new IllegalArgumentException("age needs timestamps: " + f);
          }
          return Interval.age(from, to);
        }
      case "DATE_TRUNC":
        return dateTrunc(str(args, 0), arg(args, 1));
      default:
        throw new IllegalArgumentException("Unsupported function: " + f);
    }
  }

  private static final Object NOT_A_CATALOG_FUNCTION = new Object();

  /** The pg_catalog functions psql uses when describing objects. */
  @Nullable
  private static Object catalogFunction(String name, List<Object> args, Context ctx) {
    try {
      switch (name) {
        case "PG_GET_USERBYID":
          return Catalog.OWNER;
        case "PG_ENCODING_TO_CHAR":
          return "UTF8";
        case "PG_TABLE_IS_VISIBLE":
        case "PG_RELATION_IS_PUBLISHABLE":
          return true;
        case "ARRAY_LENGTH":
        case "ARRAY_UPPER":
          {
            // null for an empty array, as PostgreSQL has no dimension to report
            Object a = arg(args, 0);
            List<?> items = a == null ? Collections.emptyList() : elements(a);
            return items.isEmpty() ? null : (Object) (long) items.size();
          }
        case "ARRAY_TO_STRING":
          if (!(arg(args, 0) instanceof List)) {
            return null;
          }
          StringBuilder sb = new StringBuilder();
          for (Object v : (List<?>) args.get(0)) {
            sb.append(sb.length() == 0 ? "" : str(args, 1)).append(text(v));
          }
          return sb.toString();
        case "FORMAT_TYPE":
          return arg(args, 0) instanceof Number ? Catalog.typeName(num(args, 0).longValue()) : null;
        case "TO_REGTYPE":
          try {
            return arg(args, 0) == null ? null : (Object) Catalog.typeOid(str(args, 0));
          } catch (IllegalArgumentException unknownType) {
            return null;
          }
        case "TO_REGCLASS":
          try {
            return arg(args, 0) == null || ctx.catalog == null
                ? null
                : (Object) ctx.catalog.relationOid(str(args, 0));
          } catch (IllegalArgumentException unknownRelation) {
            return null;
          }
        case "CURRENT_SCHEMAS":
          {
            if (ctx.catalog == null) {
              return null;
            }
            List<Object> schemas = new ArrayList<>();
            if (isTrue(arg(args, 0))) {
              schemas.add("pg_catalog");
            }
            schemas.add("public"); // the connected namespace shows as public
            return schemas;
          }
        case "PG_GET_INDEXDEF":
          return ctx.catalog == null || !(arg(args, 0) instanceof Number)
              ? null
              : ctx.catalog.indexDef(num(args, 0).longValue());
        case "PG_GET_CONSTRAINTDEF":
          return ctx.catalog == null || !(arg(args, 0) instanceof Number)
              ? null
              : ctx.catalog.constraintDef(num(args, 0).longValue());
        case "PG_GET_EXPR":
        case "PG_PARTITION_ANCESTORS":
        case "COL_DESCRIPTION":
        case "OBJ_DESCRIPTION":
        case "SHOBJ_DESCRIPTION":
          return null;
        case "CURRENT_SCHEMA":
          return ctx.catalog == null ? null : "public"; // the connected namespace shows as public
        case "CURRENT_DATABASE":
          return ctx.catalog == null ? null : ctx.catalog.database;
        case "PG_BACKEND_PID":
          return 0L;
        default:
          return NOT_A_CATALOG_FUNCTION;
      }
    } catch (com.scalar.db.exception.storage.ExecutionException e) {
      throw new IllegalStateException("Catalog lookup failed: " + e.getMessage(), e);
    }
  }

  @Nullable
  private static Object arg(List<Object> args, int i) {
    return i < args.size() ? args.get(i) : null;
  }

  private static String str(List<Object> args, int i) {
    if (i >= args.size()) {
      throw new IllegalArgumentException("Missing function argument " + (i + 1));
    }
    return text(args.get(i));
  }

  private static Number num(List<Object> args, int i) {
    if (i >= args.size() || !(args.get(i) instanceof Number)) {
      throw new IllegalArgumentException("Expected a numeric argument at position " + (i + 1));
    }
    return (Number) args.get(i);
  }

  @Nullable
  private static Object aggregate(Function f, List<Map<String, Object>> group, Context ctx) {
    String name = f.getName().toUpperCase(Locale.ROOT);
    ExpressionList<?> params = f.getParameters();
    boolean star =
        f.isAllColumns()
            || (params != null && params.size() == 1 && params.get(0) instanceof AllColumns);
    int arity = name.equals("STRING_AGG") || name.endsWith("OBJECT_AGG") ? 2 : 1;
    if (!star && (params == null || params.size() != arity)) {
      throw new IllegalArgumentException("Expected " + arity + " argument(s): " + f);
    }
    if (f.getOrderByElements() != null) {
      // string_agg(x, ',' ORDER BY y): aggregate the rows in that order
      List<OrderByElement> orderBy = f.getOrderByElements();
      group = new ArrayList<>(group);
      group.sort(
          (x, y) -> {
            for (OrderByElement o : orderBy) {
              int c =
                  orderCompare(
                      o,
                      eval(o.getExpression(), Collections.singletonList(x), ctx),
                      eval(o.getExpression(), Collections.singletonList(y), ctx));
              if (c != 0) {
                return c;
              }
            }
            return 0;
          });
    }
    if (name.endsWith("OBJECT_AGG")) {
      // json_object_agg(key, value): one member per row, keys as text
      if (group.isEmpty()) {
        return null;
      }
      List<Object> pairs = new ArrayList<>();
      for (Map<String, Object> row : group) {
        Object key = eval(params.get(0), Collections.singletonList(row), ctx);
        if (key == null) {
          throw new IllegalArgumentException("field name must not be null");
        }
        pairs.add(key);
        pairs.add(eval(params.get(1), Collections.singletonList(row), ctx));
      }
      return new Json.Value(Json.buildObject(pairs), name.startsWith("JSONB"), null);
    }
    List<Object> values = new ArrayList<>();
    for (Map<String, Object> row : group) {
      Object v = star ? row : eval(params.get(0), Collections.singletonList(row), ctx);
      if (v != null
          || name.equals("ARRAY_AGG")
          || name.endsWith("JSON_AGG")
          || name.endsWith("JSONB_AGG")) {
        values.add(v); // array_agg and json_agg keep NULL elements
      }
    }
    if (f.isDistinct()) {
      values = new ArrayList<>(new LinkedHashSet<>(values));
    }
    Object delimiter =
        name.equals("STRING_AGG") && !group.isEmpty()
            ? eval(params.get(1), group.subList(0, 1), ctx)
            : null;
    return aggregateValues(name, values, delimiter, f);
  }

  /** Applies an aggregate to the non-null values collected over its rows or window frame. */
  @Nullable
  static Object aggregateValues(
      String name, List<Object> values, @Nullable Object delimiter, Expression f) {
    switch (name) {
      case "COUNT":
        return (long) values.size();
      case "MIN":
      case "MAX":
        Object best = null;
        for (Object v : values) {
          if (best == null || (compare(v, best) < 0) == name.equals("MIN")) {
            best = v;
          }
        }
        return best;
      case "SUM":
      case "AVG":
        if (values.isEmpty()) {
          return null;
        }
        BigDecimal sum = BigDecimal.ZERO;
        double nonFinite = 0; // NaN and Infinity propagate as in floating-point addition
        boolean integral = true;
        boolean floating = false; // a double or float input makes the result a double
        for (Object v : values) {
          if (!(v instanceof Number)) {
            throw new IllegalArgumentException(name + " requires numeric values: " + f);
          }
          if (isFinite(v)) {
            sum = sum.add(new BigDecimal(v.toString()));
          } else {
            nonFinite += ((Number) v).doubleValue();
          }
          integral &= isIntegral(v);
          floating |= v instanceof Double || v instanceof Float;
        }
        if (nonFinite != 0 || Double.isNaN(nonFinite)) {
          return nonFinite;
        }
        if (name.equals("AVG")) {
          // the average of integers or numerics is numeric, with PostgreSQL's division scale
          return floating
              ? (Object) (sum.doubleValue() / values.size())
              : divide(sum, BigDecimal.valueOf(values.size()));
        }
        if (floating) {
          return sum.doubleValue();
        }
        if (integral) {
          try {
            return sum.longValueExact();
          } catch (ArithmeticException beyondBigint) {
            return sum;
          }
        }
        return sum;
      case "BOOL_AND":
      case "EVERY":
      case "BOOL_OR":
        {
          if (values.isEmpty()) {
            return null;
          }
          boolean and = !name.equals("BOOL_OR");
          for (Object v : values) {
            if (!(v instanceof Boolean)) {
              throw new IllegalArgumentException(name + " requires boolean values: " + f);
            }
            if ((Boolean) v != and) {
              return !and;
            }
          }
          return and;
        }
      case "ARRAY_AGG":
        return values.isEmpty() ? null : new ArrayList<>(values);
      case "JSON_AGG":
      case "JSONB_AGG":
        return values.isEmpty()
            ? null
            : new Json.Value(Json.buildArray(values), name.equals("JSONB_AGG"), null);
      case "STRING_AGG":
        {
          if (values.isEmpty()) {
            return null;
          }
          StringBuilder joined = new StringBuilder();
          for (int i = 0; i < values.size(); i++) {
            if (i > 0 && delimiter != null) {
              joined.append(text(delimiter));
            }
            joined.append(text(values.get(i)));
          }
          return joined.toString();
        }
      default:
        throw new AssertionError();
    }
  }

  /** Translates a SQL LIKE pattern into a regex; {@code %} is any string and {@code _} any char. */
  static Pattern likePattern(String pattern, String escape) {
    StringBuilder regex = new StringBuilder();
    for (int i = 0; i < pattern.length(); i++) {
      char c = pattern.charAt(i);
      if (!escape.isEmpty() && c == escape.charAt(0) && i + 1 < pattern.length()) {
        i++;
        regex.append(Pattern.quote(String.valueOf(pattern.charAt(i))));
      } else if (c == '%') {
        regex.append(".*");
      } else if (c == '_') {
        regex.append('.');
      } else {
        regex.append(Pattern.quote(String.valueOf(c)));
      }
    }
    return Pattern.compile(regex.toString(), Pattern.DOTALL);
  }
}
