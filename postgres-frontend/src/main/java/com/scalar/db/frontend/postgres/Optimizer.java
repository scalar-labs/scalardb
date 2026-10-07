package com.scalar.db.frontend.postgres;

import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.TableMetadata;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;

/**
 * Rule-based rewrites of a {@link LogicalPlan}, applied in order. Each rule moves conjuncts from
 * the WHERE clause or a join's ON clause into something cheaper: a ScalarDB condition, a keyed
 * lookup of one table per outer row, or a hash-join key.
 */
final class Optimizer {
  private Optimizer() {}

  static void optimize(LogicalPlan plan) {
    pushDownConditions(plan);
    orToInList(plan);
    inListLookups(plan);
    keyedLookups(plan);
    lateConditions(plan);
    pushDownOr(plan);
    hashJoins(plan);
  }

  /**
   * {@code c = 1 OR c = 2} on one column becomes {@code c IN (1, 2)}, which can be a keyed read.
   */
  static void orToInList(LogicalPlan plan) {
    for (int i = 0; i < plan.where.size(); i++) {
      Expression c = plan.where.get(i);
      if (!(c instanceof OrExpression)) {
        continue;
      }
      Expression column = null;
      List<Expression> values = new ArrayList<>();
      for (Expression d : disjuncts(c)) {
        if (!(d instanceof EqualsTo)
            || !QueryParser.isColumn(((EqualsTo) d).getLeftExpression())
            || !QueryParser.isLiteral(((EqualsTo) d).getRightExpression())
            || QueryParser.isNull(((EqualsTo) d).getRightExpression())) {
          column = null;
          break;
        }
        Expression left = ((EqualsTo) d).getLeftExpression();
        if (column == null) {
          column = left;
        } else if (!sameColumn(column, left)) {
          column = null;
          break;
        }
        values.add(((EqualsTo) d).getRightExpression());
      }
      if (column != null) {
        plan.where.set(i, new InExpression(column, new ExpressionList<>(values)));
      }
    }
  }

  /**
   * {@code t.k IN (v1, v2, ...)} on a key column of a table becomes a join with a Values source of
   * the distinct values, which {@link #keyedLookups} then turns into one keyed read per value.
   */
  static void inListLookups(LogicalPlan plan) {
    int lists = 0;
    for (int i = 0; i < plan.where.size(); i++) {
      if (!(plan.where.get(i) instanceof InExpression)) {
        continue;
      }
      InExpression in = (InExpression) plan.where.get(i);
      if (in.isNot()
          || !QueryParser.isColumn(in.getLeftExpression())
          || !(in.getRightExpression() instanceof ExpressionList)
          || in.getRightExpression() instanceof net.sf.jsqlparser.statement.select.Select) {
        continue;
      }
      Set<LogicalPlan.Source> refs = localSources(in.getLeftExpression(), plan);
      LogicalPlan.Source s = refs.size() == 1 ? refs.iterator().next() : null;
      String column = QueryParser.columnName(in.getLeftExpression());
      if (s == null
          || !s.isTable()
          || plan.nullable(s)
          || !keyed(s, Collections.singleton(column))) {
        continue;
      }
      Set<Object> values = new LinkedHashSet<>();
      boolean literals = true;
      for (Expression e : (ExpressionList<?>) in.getRightExpression()) {
        if (!QueryParser.isLiteral(e)) {
          literals = false;
          break;
        }
        QueryParser.embed(e); // the value becomes part of the plan
        Object v =
            Evaluator.eval(
                e, Collections.<Map<String, Object>>emptyList(), QueryParser.literalContext());
        if (v != null) {
          values.add(v); // NULL never matches, and duplicates would duplicate rows
        }
      }
      if (!literals) {
        continue;
      }
      lists++;
      String qualifier = "$in" + lists;
      LogicalPlan.Source list =
          new LogicalPlan.Source(qualifier, Collections.singletonList("v"), null, null);
      list.catalogTable = "Values IN " + values.toString().replace('[', '(').replace(']', ')');
      list.catalogRows = new ArrayList<>();
      for (Object v : values) {
        list.catalogRows.add(Collections.singletonMap(qualifier + ".v", v));
      }
      int position = plan.sources.indexOf(s);
      plan.sources.add(position, list);
      plan.joins.add(Math.max(position - 1, 0), new LogicalPlan.Join(LogicalPlan.Join.Kind.INNER));
      Column value = new Column(new Table(qualifier), "v");
      plan.where.set(i, new EqualsTo(in.getLeftExpression(), value));
    }
  }

  /**
   * An OR whose branches are all {@code column op literal} conditions on one table becomes part of
   * that table's read, as ScalarDB's OR of ANDs.
   */
  static void pushDownOr(LogicalPlan plan) {
    for (Iterator<Expression> it = plan.where.iterator(); it.hasNext(); ) {
      Expression c = it.next();
      Set<LogicalPlan.Source> refs = localSources(c, plan);
      LogicalPlan.Source only = refs.size() == 1 ? refs.iterator().next() : null;
      if (only == null || !only.isTable() || plan.nullable(only)) {
        continue;
      }
      List<List<ConditionalExpression>> terms = orTerms(c, only.target.metadata);
      if (terms != null) {
        only.pushedOr.add(terms);
        it.remove();
      }
    }
    for (int i = 0; i < plan.joins.size(); i++) {
      LogicalPlan.Source right = plan.sources.get(i + 1);
      for (Iterator<Expression> it = plan.joins.get(i).on.iterator(); it.hasNext(); ) {
        Expression c = it.next();
        Set<LogicalPlan.Source> refs = localSources(c, plan);
        if (refs.size() != 1 || !refs.contains(right) || !right.isTable()) {
          continue;
        }
        List<List<ConditionalExpression>> terms = orTerms(c, right.target.metadata);
        if (terms != null) {
          right.pushedOr.add(terms);
          it.remove();
        }
      }
    }
  }

  /** The OR's alternatives as ScalarDB conditions, or null if any leaf cannot be pushed down. */
  @Nullable
  private static List<List<ConditionalExpression>> orTerms(Expression c, TableMetadata metadata) {
    if (!(c instanceof OrExpression) || !QueryParser.subselects(c).isEmpty()) {
      return null;
    }
    List<List<ConditionalExpression>> terms = new ArrayList<>();
    for (Expression d : disjuncts(c)) {
      List<ConditionalExpression> term = new ArrayList<>();
      for (Expression leaf : QueryParser.conjuncts(d)) {
        ConditionalExpression p = QueryParser.pushable(leaf, metadata);
        if (p == null) {
          return null;
        }
        term.add(p);
      }
      terms.add(term);
    }
    return terms;
  }

  private static List<Expression> disjuncts(Expression e) {
    List<Expression> out = new ArrayList<>();
    e = QueryParser.unwrap(e);
    if (e instanceof OrExpression) {
      out.addAll(disjuncts(((OrExpression) e).getLeftExpression()));
      out.addAll(disjuncts(((OrExpression) e).getRightExpression()));
    } else {
      out.add(e);
    }
    return out;
  }

  private static boolean sameColumn(Expression a, Expression b) {
    return qualifierOf(a).equals(qualifierOf(b))
        && QueryParser.columnName(a).equals(QueryParser.columnName(b));
  }

  /**
   * A conjunct of the form {@code column op literal} on one ScalarDB table becomes a condition of
   * that table's read. WHERE conjuncts on the right side of a LEFT JOIN stay where they are, since
   * they apply after the null extension; ON conjuncts on that side are fine.
   */
  static void pushDownConditions(LogicalPlan plan) {
    for (Iterator<Expression> it = plan.where.iterator(); it.hasNext(); ) {
      Expression c = it.next();
      Set<LogicalPlan.Source> refs = localSources(c, plan);
      LogicalPlan.Source only = refs.size() == 1 ? refs.iterator().next() : null;
      ConditionalExpression p =
          only != null
                  && only.isTable()
                  && !plan.nullable(only)
                  && QueryParser.subselects(c).isEmpty()
                  && !QueryParser.containsParameter(c) // bound later, see lateConditions
              ? QueryParser.pushable(c, only.target.metadata)
              : null;
      if (p != null) {
        only.pushed.add(p);
        it.remove();
      }
    }
    for (int i = 0; i < plan.joins.size(); i++) {
      LogicalPlan.Source right = plan.sources.get(i + 1);
      for (Iterator<Expression> it = plan.joins.get(i).on.iterator(); it.hasNext(); ) {
        Expression c = it.next();
        Set<LogicalPlan.Source> refs = localSources(c, plan);
        ConditionalExpression p =
            refs.size() == 1
                    && refs.contains(right)
                    && right.isTable()
                    && QueryParser.subselects(c).isEmpty()
                ? QueryParser.pushable(c, right.target.metadata)
                : null;
        if (p != null) {
          right.pushed.add(p);
          it.remove();
        }
      }
    }
  }

  /**
   * A {@code column op $n} conjunct on a table becomes a condition of its read, built when the plan
   * is opened with the placeholder's value, so the plan can be reused with other values. Only reads
   * that are issued at open time qualify: the first source, and keyed lookups.
   */
  static void lateConditions(LogicalPlan plan) {
    for (Iterator<Expression> it = plan.where.iterator(); it.hasNext(); ) {
      Expression c = it.next();
      boolean constant =
          c instanceof net.sf.jsqlparser.expression.operators.relational.ComparisonOperator
              && QueryParser.isColumn(
                  ((net.sf.jsqlparser.expression.operators.relational.ComparisonOperator) c)
                      .getLeftExpression())
              && QueryParser.isConstant(
                  ((net.sf.jsqlparser.expression.operators.relational.ComparisonOperator) c)
                      .getRightExpression());
      if ((!QueryParser.containsParameter(c) && !constant)
          || !QueryParser.subselects(c).isEmpty()) {
        continue;
      }
      Set<LogicalPlan.Source> refs = localSources(c, plan);
      LogicalPlan.Source only = refs.size() == 1 ? refs.iterator().next() : null;
      if (only == null
          || !only.isTable()
          || plan.nullable(only)
          || (plan.sources.indexOf(only) != 0 && only.lookupParams == null)
          || QueryParser.pushable(c, only.target.metadata, null, QueryParser.literalContext())
              == null) {
        continue;
      }
      only.pushedLate.add(c);
      it.remove();
    }
  }

  /**
   * A table whose partition key (or a secondary index) is bound by equalities to earlier tables, or
   * to the enclosing query, is read once per outer row instead of being scanned whole.
   */
  static void keyedLookups(LogicalPlan plan) {
    List<LogicalPlan.Source> sources = plan.sources;
    for (int i = 0; i < sources.size(); i++) {
      LogicalPlan.Source s = sources.get(i);
      if (!s.isTable()) {
        continue;
      }
      LogicalPlan.Join join = i == 0 ? null : plan.joins.get(i - 1);
      if (join != null && join.rightOuter()) {
        continue; // a lookup never sees the right rows that match nothing
      }
      List<Expression> pool = new ArrayList<>();
      if (join != null) {
        pool.addAll(join.on);
      }
      if ((join == null || join.kind == LogicalPlan.Join.Kind.INNER) && !plan.nullable(s)) {
        pool.addAll(plan.where);
      }
      Map<String, Expression> params = new LinkedHashMap<>();
      List<Expression> used = new ArrayList<>();
      for (Expression c : pool) {
        if (!(c instanceof EqualsTo)) {
          continue;
        }
        EqualsTo eq = (EqualsTo) c;
        String column = null;
        Expression other = null;
        if (QueryParser.isColumn(eq.getLeftExpression())
            && s.qualifier.equals(qualifierOf(eq.getLeftExpression()))) {
          column = QueryParser.columnName(eq.getLeftExpression());
          other = eq.getRightExpression();
        } else if (QueryParser.isColumn(eq.getRightExpression())
            && s.qualifier.equals(qualifierOf(eq.getRightExpression()))) {
          column = QueryParser.columnName(eq.getRightExpression());
          other = eq.getLeftExpression();
        }
        if (column == null
            || params.containsKey(column)
            || !QueryParser.subselects(other).isEmpty()
            || !refersOnlyToEarlier(other, sources, i)) {
          continue;
        }
        params.put(column, other);
        used.add(c);
      }
      if (params.isEmpty() || !keyed(s, params.keySet())) {
        continue;
      }
      s.lookupParams = params;
      plan.where.removeAll(used);
      if (join != null) {
        join.on.removeAll(used);
      }
    }
  }

  /**
   * An equality between a table and the tables before it (or the enclosing query) that could not
   * become a keyed lookup drives a hash join instead of a nested loop.
   */
  static void hashJoins(LogicalPlan plan) {
    List<LogicalPlan.Source> sources = plan.sources;
    for (int i = 1; i < sources.size(); i++) {
      LogicalPlan.Source s = sources.get(i);
      if (s.lookupParams != null || s.lateral) {
        continue; // a lateral subquery is re-run per left row, never hashed once
      }
      LogicalPlan.Join join = plan.joins.get(i - 1);
      List<Expression> pool = new ArrayList<>(join.on);
      if (join.kind == LogicalPlan.Join.Kind.INNER) {
        pool.addAll(plan.where);
      }
      List<Expression> used = new ArrayList<>();
      for (Expression c : pool) {
        if (!(c instanceof EqualsTo) || !QueryParser.subselects(c).isEmpty()) {
          continue;
        }
        Expression a = ((EqualsTo) c).getLeftExpression();
        Expression b = ((EqualsTo) c).getRightExpression();
        Set<Integer> refsA = localRefs(a, sources);
        Set<Integer> refsB = localRefs(b, sources);
        Expression mine = null;
        Expression theirs = null;
        if (refsA.equals(Collections.singleton(i)) && earlierOnly(refsB, i)) {
          mine = a;
          theirs = b;
        } else if (refsB.equals(Collections.singleton(i)) && earlierOnly(refsA, i)) {
          mine = b;
          theirs = a;
        }
        if (mine == null) {
          continue;
        }
        join.hashRight.add(mine);
        join.hashLeft.add(theirs);
        used.add(c);
      }
      plan.where.removeAll(used);
      join.on.removeAll(used);
    }
  }

  /** The sources of this plan that {@code e} refers to, in order of reference. */
  static Set<LogicalPlan.Source> localSources(Expression e, LogicalPlan plan) {
    Set<LogicalPlan.Source> refs = new LinkedHashSet<>();
    for (Column c : QueryParser.columnRefs(e)) {
      String qualifier = qualifierOf(c);
      for (LogicalPlan.Source s : plan.sources) {
        if (s.qualifier.equals(qualifier)) {
          refs.add(s);
        }
      }
    }
    return refs;
  }

  private static String qualifierOf(Expression column) {
    Table table = ((Column) column).getTable();
    return table == null ? "" : QueryParser.unquote(table.getName());
  }

  /**
   * True if every column of {@code e} belongs to a source before {@code index} or to an outer
   * query.
   */
  private static boolean refersOnlyToEarlier(
      Expression e, List<LogicalPlan.Source> sources, int index) {
    for (Column c : QueryParser.columnRefs(e)) {
      String qualifier = qualifierOf(c);
      for (int i = index; i < sources.size(); i++) {
        if (sources.get(i).qualifier.equals(qualifier)) {
          return false;
        }
      }
    }
    return true;
  }

  /** The positions of the sources that {@code e} refers to. */
  private static Set<Integer> localRefs(Expression e, List<LogicalPlan.Source> sources) {
    Set<Integer> refs = new HashSet<>();
    for (Column c : QueryParser.columnRefs(e)) {
      String qualifier = qualifierOf(c);
      for (int i = 0; i < sources.size(); i++) {
        if (sources.get(i).qualifier.equals(qualifier)) {
          refs.add(i);
        }
      }
    }
    return refs;
  }

  /** True if the refs name at least one earlier source and none at or after {@code index}. */
  private static boolean earlierOnly(Set<Integer> refs, int index) {
    if (refs.isEmpty()) {
      return false;
    }
    for (int r : refs) {
      if (r >= index) {
        return false;
      }
    }
    return true;
  }

  /** True if binding {@code columns} (plus the literal '=' conditions) gives a keyed read. */
  private static boolean keyed(LogicalPlan.Source s, Set<String> columns) {
    TableMetadata metadata = s.target.metadata;
    Set<String> bound = new HashSet<>(columns);
    for (ConditionalExpression c : s.pushed) {
      if (c.getOperator() == ConditionalExpression.Operator.EQ) {
        bound.add(c.getColumn().getName());
      }
    }
    if (bound.containsAll(metadata.getPartitionKeyNames())) {
      return true;
    }
    for (String index : metadata.getSecondaryIndexNames()) {
      if (columns.contains(index)) {
        return true;
      }
    }
    return false;
  }
}
