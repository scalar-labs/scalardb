package com.scalar.db.frontend.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.AndConditionSet;
import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.ConditionSetBuilder;
import com.scalar.db.api.Delete;
import com.scalar.db.api.DistributedTransaction;
import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.DistributedTransactionManager;
import com.scalar.db.api.Get;
import com.scalar.db.api.Insert;
import com.scalar.db.api.Result;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Selection;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.api.TransactionCrudOperable;
import com.scalar.db.api.Update;
import com.scalar.db.api.Upsert;
import com.scalar.db.common.ResultImpl;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.BooleanColumn;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.DoubleColumn;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class QueryParserTest {
  private static final String NS = "ns";
  private static final TableMetadata METADATA =
      TableMetadata.newBuilder()
          .addColumn("id", DataType.INT)
          .addColumn("seq", DataType.BIGINT)
          .addColumn("name", DataType.TEXT)
          .addColumn("price", DataType.DOUBLE)
          .addColumn("active", DataType.BOOLEAN)
          .addPartitionKey("id")
          .addClusteringKey("seq")
          .addSecondaryIndex("name")
          .build();

  private DistributedTransactionAdmin admin;
  private QueryParser parser;
  private DistributedTransaction transaction;

  @BeforeEach
  public void setUp() throws Exception {
    admin = mock(DistributedTransactionAdmin.class);
    when(admin.getTableMetadata(NS, "t")).thenReturn(METADATA);
    when(admin.getNamespaceNames()).thenReturn(Collections.singleton(NS));
    when(admin.getNamespaceTableNames(NS)).thenReturn(Collections.singleton("t"));
    parser = new QueryParser(admin, NS);
    transaction = mock(DistributedTransaction.class);
    when(transaction.get(any())).thenReturn(java.util.Optional.empty());
    TestScanners.stub(transaction);
  }

  private static Result row(
      int id, long seq, String name, @Nullable Double price, @Nullable Boolean active) {
    Map<String, Column<?>> columns = new LinkedHashMap<>();
    columns.put("id", IntColumn.of("id", id));
    columns.put("seq", BigIntColumn.of("seq", seq));
    columns.put("name", TextColumn.of("name", name));
    columns.put(
        "price", price == null ? DoubleColumn.ofNull("price") : DoubleColumn.of("price", price));
    columns.put(
        "active",
        active == null ? BooleanColumn.ofNull("active") : BooleanColumn.of("active", active));
    return new ResultImpl(columns, METADATA);
  }

  /** Answers Get, partition Scan, index Scan and ScanAll from an in-memory table. */
  private void stubTable(List<Result> rows) throws Exception {
    when(transaction.scan(any()))
        .thenAnswer(
            invocation -> {
              Scan scan = invocation.getArgument(0);
              List<Result> out = new ArrayList<>();
              for (Result r : rows) {
                if (scan instanceof com.scalar.db.api.ScanAll
                    || (scan instanceof com.scalar.db.api.ScanWithIndex
                        && r.getText("name").equals(scan.getPartitionKey().getTextValue(0)))
                    || (!(scan instanceof com.scalar.db.api.ScanWithIndex)
                        && r.getInt("id") == scan.getPartitionKey().getIntValue(0))) {
                  out.add(r);
                }
              }
              return out;
            });
    when(transaction.get(any()))
        .thenAnswer(
            invocation -> {
              Get get = invocation.getArgument(0);
              for (Result r : rows) {
                if (r.getInt("id") == get.getPartitionKey().getIntValue(0)
                    && r.getBigInt("seq") == get.getClusteringKey().get().getBigIntValue(0)) {
                  return java.util.Optional.of(r);
                }
              }
              return java.util.Optional.empty();
            });
  }

  private static Map<String, Object> map(Object... namesAndValues) {
    Map<String, Object> map = new LinkedHashMap<>();
    for (int i = 0; i < namesAndValues.length; i += 2) {
      map.put((String) namesAndValues[i], namesAndValues[i + 1]);
    }
    return map;
  }

  @Test
  public void parse_SelectWithFullPrimaryKey_ShouldReturnGet() throws Exception {
    assertThat(
            parser
                .parse("SELECT name FROM t WHERE id = 1 AND seq = 2 AND active = TRUE")
                .getOperations())
        .containsExactly(
            Get.newBuilder()
                .namespace(NS)
                .table("t")
                .partitionKey(Key.ofInt("id", 1))
                .clusteringKey(Key.ofBigInt("seq", 2))
                .projections("id", "seq", "active", "name")
                .where(ConditionBuilder.column("active").isEqualToBoolean(true))
                .build());
  }

  @Test
  public void parse_SelectOnPartitionWithClusteringOrder_ShouldPushDownRangeOrderAndLimit()
      throws Exception {
    QueryParser.Plan plan =
        parser.parse(
            "SELECT * FROM ns.t WHERE id = 1 AND seq > -2 AND price < 3.5 "
                + "ORDER BY seq DESC LIMIT 10 OFFSET 2");

    assertThat(plan.getOperations())
        .containsExactly(
            Scan.newBuilder()
                .namespace(NS)
                .table("t")
                .partitionKey(Key.ofInt("id", 1))
                .start(Key.ofBigInt("seq", -2), false)
                .ordering(Scan.Ordering.desc("seq"))
                .limit(12)
                .where(ConditionBuilder.column("price").isLessThanDouble(3.5))
                .build());
    when(transaction.scan(any()))
        .thenReturn(
            Arrays.asList(
                row(1, 3, "c", 3.0, true), row(1, 2, "b", 2.0, false), row(1, 1, "a", 1.0, true)));
    assertThat(plan.execute(transaction))
        .containsExactly(map("id", 1, "seq", 1L, "name", "a", "price", 1.0, "active", true));
  }

  @Test
  public void parse_SelectWithIndexColumn_ShouldReturnIndexScan() throws Exception {
    assertThat(parser.parse("SELECT * FROM t WHERE name = 'x' AND price >= 1").getOperations())
        .containsExactly(
            Scan.newBuilder()
                .namespace(NS)
                .table("t")
                .indexKey(Key.ofText("name", "x"))
                .where(ConditionBuilder.column("price").isGreaterThanOrEqualToDouble(1))
                .build());
  }

  @Test
  public void parse_SelectWithoutKey_ShouldReturnScanAllWithPushedConditions() throws Exception {
    assertThat(
            parser
                .parse("SELECT id FROM t WHERE name LIKE 'a%' AND price IS NOT NULL")
                .getOperations())
        .containsExactly(
            Scan.newBuilder()
                .namespace(NS)
                .table("t")
                .all()
                .projections("name", "price", "id")
                .where(ConditionBuilder.column("name").isLikeText("a%", "\\"))
                .and(ConditionBuilder.column("price").isNotNullDouble())
                .build());
  }

  @Test
  public void parse_SelectWithOrCondition_ShouldFilterInMemory() throws Exception {
    QueryParser.Plan plan =
        parser.parse("SELECT name FROM t WHERE id = 1 AND (seq = 1 OR UPPER(name) = 'B')");

    assertThat(plan.getOperations())
        .containsExactly(
            Scan.newBuilder()
                .namespace(NS)
                .table("t")
                .partitionKey(Key.ofInt("id", 1))
                .projections("id", "seq", "name")
                .build());
    when(transaction.scan(any()))
        .thenReturn(
            Arrays.asList(
                row(1, 1, "a", 1.5, true), row(1, 2, "b", 2.5, false), row(1, 3, "c", 1.0, true)));
    assertThat(plan.execute(transaction)).containsExactly(map("name", "a"), map("name", "b"));
  }

  @Test
  public void execute_AggregateFailingWhileOpening_ShouldCloseTheScanner() throws Exception {
    TransactionCrudOperable.Scanner scanner = mock(TransactionCrudOperable.Scanner.class);
    when(scanner.one())
        .thenReturn(java.util.Optional.of(row(1, 1, "a", 1.5, true)))
        .thenReturn(java.util.Optional.empty());
    doReturn(scanner).when(transaction).getScanner(any());
    QueryParser.Plan plan = parser.parse("SELECT active, nosuchfn(name) FROM t GROUP BY active");

    assertThatThrownBy(() -> plan.execute(transaction))
        .hasMessageContaining("Unsupported function");
    verify(scanner).close();
  }

  @Test
  public void parse_SelectWithAggregates_ShouldGroupInMemory() throws Exception {
    QueryParser.Plan plan =
        parser.parse(
            "SELECT active, COUNT(*) AS n, SUM(price) AS total FROM t GROUP BY active "
                + "HAVING COUNT(*) > 1 ORDER BY n DESC, active");

    assertThat(plan.getOperations())
        .containsExactly(
            Scan.newBuilder()
                .namespace(NS)
                .table("t")
                .all()
                .projections("active", "price")
                .build());
    when(transaction.scan(any()))
        .thenReturn(
            Arrays.asList(
                row(1, 1, "a", 1.5, true),
                row(1, 2, "b", 2.5, false),
                row(1, 3, "c", 3.0, true),
                row(2, 1, "d", 4.0, null),
                row(2, 2, "e", 1.0, null)));
    assertThat(plan.execute(transaction))
        .containsExactly(
            map("active", true, "n", 2L, "total", 4.5), map("active", null, "n", 2L, "total", 5.0));
  }

  @Test
  public void parse_SelectWithExpressionsDistinctAndOffset_ShouldSortAndSliceInMemory()
      throws Exception {
    QueryParser.Plan plan =
        parser.parse("SELECT DISTINCT id, price * 2 AS p FROM t ORDER BY p DESC LIMIT 2 OFFSET 1");

    assertThat(plan.getOperations())
        .containsExactly(
            Scan.newBuilder().namespace(NS).table("t").all().projections("id", "price").build());
    when(transaction.scan(any()))
        .thenReturn(
            Arrays.asList(
                row(1, 1, "a", 1.5, true),
                row(1, 2, "b", 1.5, true),
                row(1, 3, "c", 3.0, true),
                row(2, 1, "d", 4.0, true)));
    assertThat(plan.execute(transaction))
        .containsExactly(map("id", 1, "p", 6.0), map("id", 1, "p", 3.0));
  }

  @Test
  public void parse_LeftJoin_ShouldPushDownPerTableAndJoinInMemory() throws Exception {
    QueryParser.Plan plan =
        parser.parse(
            "SELECT a.name, b.name AS other FROM t a LEFT JOIN t b "
                + "ON a.id = b.id AND b.seq = 2 WHERE a.seq = 1 ORDER BY a.name");

    Scan left =
        Scan.newBuilder()
            .namespace(NS)
            .table("t")
            .all()
            .projections("seq", "id", "name")
            .where(ConditionBuilder.column("seq").isEqualToBigInt(1))
            .build();
    // b is bound on its full primary key (a.id, 2): a Get per row of a, not a scan
    assertThat(plan.getOperations()).containsExactly(left);
    assertThat(String.join("\n", parser.explain(plan)))
        .contains(
            "-> Left Lookup Join (per outer row): ScalarDB Get ns.t partitionKey={id=a.id} "
                + "clusteringKey={seq=2} projections=[id, seq, name] AS b");
    stubTable(Arrays.asList(row(1, 2, "x", 1.0, true), row(2, 2, "y", 1.0, true)));
    when(transaction.scan(left))
        .thenReturn(
            Arrays.asList(
                row(2, 1, "b", 1.0, true), row(1, 1, "a", 1.0, true), row(3, 1, "c", 1.0, true)));
    assertThat(plan.execute(transaction))
        .containsExactly(
            map("name", "a", "other", "x"),
            map("name", "b", "other", "y"),
            map("name", "c", "other", null));
  }

  @Test
  public void parse_SelectWithSubqueries_ShouldEvaluateInMemory() throws Exception {
    QueryParser.Plan plan =
        parser.parse(
            "SELECT x.name FROM t x WHERE x.price > (SELECT AVG(price) FROM t) "
                + "AND EXISTS (SELECT 1 FROM t y WHERE y.id = x.id AND y.seq <> x.seq) "
                + "AND x.seq IN (SELECT seq FROM t) ORDER BY x.name");

    assertThat(plan.getOperations())
        .containsExactly(
            Scan.newBuilder()
                .namespace(NS)
                .table("t")
                .all()
                .projections("price", "id", "seq", "name")
                .build());
    // the correlated EXISTS reads y's partition for each row of x
    assertThat(String.join("\n", parser.explain(plan)))
        .contains(
            "Lookup (per outer row): ScalarDB Scan ns.t partitionKey={id=x.id} "
                + "projections=[id, seq] AS y");
    stubTable(
        Arrays.asList(
            row(1, 1, "a", 1.0, true),
            row(1, 2, "b", 3.0, false),
            row(2, 1, "c", 5.0, true),
            row(2, 2, "d", 4.0, false)));
    assertThat(plan.execute(transaction)).containsExactly(map("name", "c"), map("name", "d"));
  }

  @Test
  public void parse_SelectFromDerivedTable_ShouldEvaluateInMemory() throws Exception {
    QueryParser.Plan plan =
        parser.parse(
            "SELECT s.n, COUNT(*) AS c FROM (SELECT name AS n, id FROM t WHERE price > 1) s "
                + "GROUP BY s.n ORDER BY c DESC, n");

    assertThat(plan.getOperations()).isEmpty();
    when(transaction.scan(any()))
        .thenReturn(
            Arrays.asList(
                row(1, 1, "a", 2.0, true), row(2, 1, "a", 3.0, true), row(2, 2, "b", 4.0, true)));
    assertThat(plan.execute(transaction))
        .containsExactly(map("n", "a", "c", 2L), map("n", "b", "c", 1L));
  }

  @Test
  public void parse_SelectWithScalarFunctionsCaseAndCast_ShouldEvaluateInMemory() throws Exception {
    QueryParser.Plan plan =
        parser.parse(
            "SELECT UPPER(name) AS u, LENGTH(name) AS l, COALESCE(price, 0) AS p, "
                + "CASE WHEN active THEN 'y' ELSE 'n' END AS c, CAST(seq AS TEXT) || '!' AS s, "
                + "ROUND(price * 3, 1) AS r, seq % 2 AS m FROM t WHERE id = 1 AND seq = 1");

    when(transaction.get(any())).thenReturn(java.util.Optional.of(row(1, 1, "ab", null, true)));
    assertThat(plan.execute(transaction))
        .containsExactly(map("u", "AB", "l", 2L, "p", 0L, "c", "y", "s", "1!", "r", null, "m", 1L));
  }

  @Test
  public void execute_SyntaxUnblockedByJsqlparser5_ShouldEvaluateLikePostgres() throws Exception {
    assertThat(
            parser
                .parse(
                    "SELECT $$it's$$ AS s, 5 # 3 AS x, 1 << 2 AS l, 8 >> 1 AS r, ~5 AS n, 6 & 3 AS a,"
                        + " 6 | 3 AS o")
                .execute(transaction))
        .containsExactly(map("s", "it's", "x", 6L, "l", 4L, "r", 4L, "n", -6L, "a", 2L, "o", 7L));
    assertThat(
            parser
                .parse(
                    "SELECT 1 IS DISTINCT FROM NULL AS a, NULL IS NOT DISTINCT FROM NULL AS b,"
                        + " 1 IS DISTINCT FROM 1 AS c")
                .execute(transaction))
        .containsExactly(map("a", true, "b", true, "c", false));
    assertThat(
            parser
                .parse(
                    "SELECT 'abc' SIMILAR TO '(a|x)%' AS a, 'abc' SIMILAR TO 'a_' AS b,"
                        + " 'a.c' SIMILAR TO 'a.c' AS c, 'abc' NOT SIMILAR TO 'a%' AS d,"
                        + " 'a%' SIMILAR TO 'a!%' ESCAPE '!' AS e")
                .execute(transaction))
        .containsExactly(map("a", true, "b", false, "c", true, "d", false, "e", true));
    assertThat(
            parser
                .parse(
                    "SELECT 2 BETWEEN SYMMETRIC 3 AND 1 AS a, (1, 'x') = (1, 'x') AS b,"
                        + " (1, 2) < (1, 3) AS c, (1, NULL) = (1, 2) AS d, (ARRAY[10, 20, 30])[2] AS e,"
                        + " (ARRAY[10, 20, 30])[2:3] AS f, (ARRAY[10, 20, 30])[5] AS g")
                .execute(transaction))
        .containsExactly(
            map(
                "a",
                true,
                "b",
                true,
                "c",
                true,
                "d",
                null,
                "e",
                20L,
                "f",
                Arrays.asList(20L, 30L),
                "g",
                null));
    assertThat(
            parser
                .parse(
                    "SELECT a FROM (VALUES (1, 'a'), (3, 'c')) AS v(a, b)"
                        + " WHERE (a, b) IN ((1, 'a'), (2, 'b'))")
                .execute(transaction))
        .containsExactly(map("a", 1L));
    assertThat(
            parser
                .parse(
                    "SELECT length(gen_random_uuid()::text) AS n,"
                        + " 'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11'::uuid AS u, '{\"a\":1}'::json::text AS j")
                .execute(transaction))
        .containsExactly(
            map("n", 36L, "u", "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", "j", "{\"a\":1}"));
    assertThat(parser.parse("ANALYZE t").commandTag(0)).isEqualTo("ANALYZE");
    assertThat(parser.parse("VACUUM ANALYZE t").commandTag(0)).isEqualTo("VACUUM");
    assertThat(parser.parse("SELECT 1 EXCEPT ALL SELECT 2").execute(transaction))
        .containsExactly(map("?column?", 1L));
    // grouping() tells a subtotal row from a NULL key
    assertThat(
            parser
                .parse(
                    "SELECT a, grouping(a) AS g, count(*) AS n FROM (VALUES (1), (1), (NULL)) AS v(a)"
                        + " GROUP BY ROLLUP (a) ORDER BY g, a")
                .execute(transaction))
        .containsExactly(
            map("a", 1L, "g", 0L, "n", 2L),
            map("a", null, "g", 0L, "n", 1L),
            map("a", null, "g", 1L, "n", 3L));
    assertThat(
            parser
                .parse("SELECT v, n FROM unnest(ARRAY['x', 'y']) WITH ORDINALITY AS u(v, n)")
                .execute(transaction))
        .containsExactly(map("v", "x", "n", 1L), map("v", "y", "n", 2L));
    assertThat(
            parser
                .parse(
                    "SELECT a FROM (VALUES (3), (2), (2), (1)) AS v(a) ORDER BY a DESC"
                        + " FETCH FIRST 2 ROWS WITH TIES")
                .execute(transaction))
        .containsExactly(map("a", 3L), map("a", 2L), map("a", 2L));
  }

  @Test
  public void parse_SelectWithoutFrom_ShouldReturnOneRow() throws Exception {
    QueryParser.Plan plan = parser.parse("SELECT 1 AS one, 'a' || 'b' AS ab, NOW() IS NOT NULL");

    assertThat(plan.getOperations()).isEmpty();
    assertThat(plan.getOutputColumns()).containsExactly("one", "ab", "?column?");
    assertThat(plan.execute(transaction))
        .containsExactly(map("one", 1L, "ab", "ab", "?column?", true));
  }

  @Test
  public void execute_ScalarAndDateFunctions_ShouldMatchPostgres() throws Exception {
    QueryParser.Plan plan =
        parser.parse(
            "SELECT position('e' in 'abcde'), greatest(1, NULL, 3), least(2, 5), sign(-4),"
                + " reverse('abc'), 2 ^ 3, EXTRACT(YEAR FROM DATE '2024-02-29'),"
                + " EXTRACT(DOW FROM DATE '2024-02-25'),"
                + " date_trunc('month', TIMESTAMP '2024-02-29 13:45:00'),"
                + " DATE '2024-02-28' + 2, DATE '2024-03-01' - DATE '2024-02-01'");

    assertThat(plan.execute(transaction).get(0).values())
        .containsExactly(
            5L,
            3L,
            2L,
            -1L,
            "cba",
            8.0,
            2024L,
            0L,
            java.time.LocalDateTime.of(2024, 2, 1, 0, 0),
            java.time.LocalDate.of(2024, 3, 1),
            29);
  }

  @Test
  public void execute_FilteredAndNewAggregates_ShouldGroupInMemory() throws Exception {
    when(transaction.scan(any()))
        .thenReturn(
            Arrays.asList(
                row(1, 1, "a", 1.5, true), row(1, 2, "b", 2.5, false), row(1, 3, "c", 1.0, true)));
    QueryParser.Plan plan =
        parser.parse(
            "SELECT active AS on_sale, count(*) FILTER (WHERE price > 1.2) AS pricey,"
                + " bool_or(price > 2) AS any_dear, string_agg(name, '|' ORDER BY name DESC) AS names"
                + " FROM t GROUP BY on_sale ORDER BY on_sale");

    assertThat(plan.execute(transaction))
        .containsExactly(
            map("on_sale", false, "pricey", 1L, "any_dear", true, "names", "b"),
            map("on_sale", true, "pricey", 1L, "any_dear", false, "names", "c|a"));
  }

  @Test
  public void constant_WithRepeatedLabels_ShouldKeepThemAndKeyRowsByUniqueNames() {
    // psql's \\d asks a catalog query with "pubname, NULL, NULL"; the fallback answers it empty
    QueryParser.Plan plan =
        QueryParser.Plan.constant(
            Arrays.asList("pubname", "?column?", "?column?"), Collections.emptyList(), "SELECT");

    assertThat(plan.getColumnLabels()).containsExactly("pubname", "?column?", "?column?");
    assertThat(plan.getOutputColumns()).containsExactly("pubname", "?column?", "?column?_2");
  }

  @Test
  public void label_ShouldFollowPostgresColumnNaming() throws Exception {
    QueryParser.Plan plan =
        parser.parse(
            "SELECT id, t.name, COUNT(*), count(name), UPPER(name), CAST(price AS INT),"
                + " CAST(1.5 AS INTEGER), '7'::bigint, CASE WHEN active THEN 1 END, id + 1,"
                + " (SELECT MAX(seq) FROM t), EXISTS (SELECT 1 FROM t), TRIM(name), 1, true"
                + " FROM t GROUP BY id, name, price, active");

    assertThat(plan.getColumnLabels())
        .containsExactly(
            "id",
            "name",
            "count",
            "count",
            "upper",
            "price",
            "int4",
            "int8",
            "case",
            "?column?",
            "max",
            "exists",
            "btrim",
            "?column?",
            "?column?");
    // internal names stay unique so rows can be keyed by them
    assertThat(plan.getOutputColumns())
        .containsExactly(
            "id",
            "name",
            "count",
            "count_2",
            "upper",
            "price",
            "int4",
            "int8",
            "case",
            "?column?",
            "max",
            "exists",
            "btrim",
            "?column?_2",
            "?column?_3");
  }

  @Test
  public void parse_MultiRowInsert_ShouldReturnOneInsertPerRow() throws Exception {
    assertThat(
            parser
                .parse(
                    "INSERT INTO t (id, seq, name, price, active) "
                        + "VALUES (1, 2, 'x', 3.5, FALSE), (2, 1, 'y', NULL, NULL)")
                .getOperations())
        .containsExactly(
            Insert.newBuilder()
                .namespace(NS)
                .table("t")
                .partitionKey(Key.ofInt("id", 1))
                .clusteringKey(Key.ofBigInt("seq", 2))
                .textValue("name", "x")
                .doubleValue("price", 3.5)
                .booleanValue("active", false)
                .build(),
            Insert.newBuilder()
                .namespace(NS)
                .table("t")
                .partitionKey(Key.ofInt("id", 2))
                .clusteringKey(Key.ofBigInt("seq", 1))
                .textValue("name", "y")
                .doubleValue("price", null)
                .booleanValue("active", null)
                .build());
  }

  @Test
  public void parse_UpdateWithExtraCondition_ShouldReturnConditionalUpdate() throws Exception {
    assertThat(
            parser
                .parse(
                    "UPDATE t SET name = 'y', price = NULL "
                        + "WHERE id = 1 AND seq = 2 AND active = TRUE")
                .getOperations())
        .containsExactly(
            Update.newBuilder()
                .namespace(NS)
                .table("t")
                .partitionKey(Key.ofInt("id", 1))
                .clusteringKey(Key.ofBigInt("seq", 2))
                .textValue("name", "y")
                .doubleValue("price", null)
                .condition(
                    ConditionBuilder.updateIf(
                        Collections.singletonList(
                            ConditionBuilder.column("active").isEqualToBoolean(true))))
                .build());
  }

  @Test
  public void parse_Delete_ShouldReturnDelete() throws Exception {
    assertThat(parser.parse("DELETE FROM t WHERE seq = 2 AND id = 1").getOperations())
        .containsExactly(
            Delete.newBuilder()
                .namespace(NS)
                .table("t")
                .partitionKey(Key.ofInt("id", 1))
                .clusteringKey(Key.ofBigInt("seq", 2))
                .condition(ConditionBuilder.deleteIfExists())
                .build());
  }

  @Test
  public void parse_UnsupportedSql_ShouldThrowIllegalArgumentException() throws Exception {
    when(transaction.scan(any())).thenReturn(Collections.singletonList(row(1, 1, "a", 1.0, true)));
    for (String sql :
        new String[] {
          "DELETE FROM t WHERE nope = 1",
          "UPDATE t SET nope = 'x' WHERE id = 1 AND seq = 1 OR active = TRUE",
          "SELECT * FROM t a JOIN t b ON a.id = b.id WHERE name = 'x'",
          "SELECT * FROM (SELECT 1)",
          "SELECT * FROM missing",
          "SELECT nope FROM t",
          "SELECT * FROM t WHERE id = 1 OR nope = 2",
          "SELECT * FROM t ORDER BY nope",
          "SELECT NOPE(name) FROM t",
          "this is not sql",
        }) {
      assertThatThrownBy(() -> parser.parse(sql).execute(transaction))
          .as(sql)
          .isInstanceOf(IllegalArgumentException.class);
    }
  }

  @Test
  public void parse_Explain_ShouldDescribePushdownAndInMemorySteps() throws Exception {
    QueryParser.Plan plan =
        parser.parse(
            "EXPLAIN SELECT c.name, COUNT(*) AS n FROM t c JOIN t o ON c.id = o.id "
                + "WHERE o.price > 2 AND (c.seq = 1 OR c.seq = 2) AND c.id = 1 "
                + "GROUP BY c.name ORDER BY n DESC LIMIT 3");

    assertThat(plan.getOutputColumns()).containsExactly("QUERY PLAN");
    assertThat(plan.commandTag(0)).isEqualTo("EXPLAIN");
    List<String> lines = new ArrayList<>();
    for (Map<String, Object> row : plan.execute(transaction)) {
      lines.add((String) row.get("QUERY PLAN"));
    }
    assertThat(lines)
        .containsExactly(
            "Project [name, n]",
            "  -> Limit 3",
            "    -> Sort n DESC",
            "      -> Aggregate [name, n] GROUP BY c.name",
            "        -> Lookup Join (per outer row): ScalarDB Scan ns.t partitionKey={id=c.id} "
                + "conditions=[price > 2.0] projections=[price, id] AS o",
            "          -> Lookup Join (per outer row): ScalarDB Get ns.t partitionKey={id=1} "
                + "clusteringKey={seq=$in1.v} projections=[seq, id, name] AS c",
            "            -> Values IN (1, 2) AS $in1 (in memory)");
  }

  @Test
  public void parse_PsqlListTablesQuery_ShouldAnswerFromMetadata() throws Exception {
    // psql \dt public.*, verbatim: OPERATOR(pg_catalog.~), COLLATE, regex, ORDER BY ordinals;
    // the connected namespace is shown as schema public
    QueryParser.Plan plan =
        parser.parse(
            "SELECT n.nspname as \"Schema\", c.relname as \"Name\", "
                + "CASE c.relkind WHEN 'r' THEN 'table' WHEN 'i' THEN 'index' END as \"Type\", "
                + "pg_catalog.pg_get_userbyid(c.relowner) as \"Owner\" "
                + "FROM pg_catalog.pg_class c "
                + "LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
                + "LEFT JOIN pg_catalog.pg_am am ON am.oid = c.relam "
                + "WHERE c.relkind IN ('r','p','t','s','') "
                + "AND n.nspname OPERATOR(pg_catalog.~) '^(public)$' COLLATE pg_catalog.default "
                + "ORDER BY 1,2");

    assertThat(plan.getOperations()).isEmpty();
    assertThat(plan.execute(transaction))
        .containsExactly(
            map("Schema", "public", "Name", "t", "Type", "table", "Owner", "scalardb"));
    assertThat(
            parser
                .parse(
                    "SELECT column_name, data_type, is_nullable, udt_name "
                        + "FROM information_schema.columns "
                        + "WHERE table_name = 't' ORDER BY ordinal_position")
                .execute(transaction))
        .containsExactly(
            map(
                "column_name",
                "id",
                "data_type",
                "integer",
                "is_nullable",
                "NO",
                "udt_name",
                "int4"),
            map(
                "column_name",
                "seq",
                "data_type",
                "bigint",
                "is_nullable",
                "NO",
                "udt_name",
                "int8"),
            map(
                "column_name",
                "name",
                "data_type",
                "text",
                "is_nullable",
                "YES",
                "udt_name",
                "text"),
            map(
                "column_name",
                "price",
                "data_type",
                "double precision",
                "is_nullable",
                "YES",
                "udt_name",
                "float8"),
            map(
                "column_name",
                "active",
                "data_type",
                "boolean",
                "is_nullable",
                "YES",
                "udt_name",
                "bool"));
  }

  @Test
  public void parse_PsqlDescribeTableQueries_ShouldAnswerFromMetadata() throws Exception {
    // psql \d t: find the OID, then columns and indexes, verbatim
    List<Map<String, Object>> found =
        parser
            .parse(
                "SELECT c.oid, n.nspname, c.relname FROM pg_catalog.pg_class c "
                    + "LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
                    + "WHERE c.relname OPERATOR(pg_catalog.~) '^(t)$' COLLATE pg_catalog.default "
                    + "AND pg_catalog.pg_table_is_visible(c.oid) ORDER BY 2, 3")
            .execute(transaction);
    assertThat(found).hasSize(1);
    String oid = String.valueOf(found.get(0).get("oid"));

    List<Map<String, Object>> columns =
        parser
            .parse(
                "SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod), "
                    + "(SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid, true) "
                    + " FROM pg_catalog.pg_attrdef d WHERE d.adrelid = a.attrelid "
                    + " AND d.adnum = a.attnum AND a.atthasdef), a.attnotnull, "
                    + "(SELECT c.collname FROM pg_catalog.pg_collation c, pg_catalog.pg_type t "
                    + " WHERE c.oid = a.attcollation AND t.oid = a.atttypid "
                    + " AND a.attcollation <> t.typcollation) AS attcollation, "
                    + "a.attidentity, a.attgenerated FROM pg_catalog.pg_attribute a "
                    + "WHERE a.attrelid = '"
                    + oid
                    + "' AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum")
            .execute(transaction);
    assertThat(columns).hasSize(5);
    assertThat(columns.get(0).values()).containsExactly("id", "integer", null, true, null, "", "");
    assertThat(columns.get(3).values())
        .containsExactly("price", "double precision", null, false, null, "", "");

    List<Map<String, Object>> indexes =
        parser
            .parse(
                "SELECT c2.relname, i.indisprimary, i.indisunique, "
                    + "pg_catalog.pg_get_indexdef(i.indexrelid, 0, true) AS def, "
                    + "pg_catalog.pg_get_constraintdef(con.oid, true) AS con, contype "
                    + "FROM pg_catalog.pg_class c, pg_catalog.pg_class c2, pg_catalog.pg_index i "
                    + "LEFT JOIN pg_catalog.pg_constraint con ON (conrelid = i.indrelid "
                    + "AND conindid = i.indexrelid AND contype IN ('p','u','x')) "
                    + "WHERE c.oid = '"
                    + oid
                    + "' AND c.oid = i.indrelid AND i.indexrelid = c2.oid "
                    + "ORDER BY i.indisprimary DESC, c2.relname")
            .execute(transaction);
    assertThat(indexes)
        .containsExactly(
            map(
                "relname",
                "t_pkey",
                "indisprimary",
                true,
                "indisunique",
                true,
                "def",
                "CREATE UNIQUE INDEX t_pkey ON public.t USING btree (id, seq)",
                "con",
                "PRIMARY KEY (id, seq)",
                "contype",
                "p"),
            map(
                "relname",
                "t_name_idx",
                "indisprimary",
                false,
                "indisunique",
                false,
                "def",
                "CREATE INDEX t_name_idx ON public.t USING btree (name)",
                "con",
                null,
                "contype",
                null));
  }

  @Test
  public void parse_CatalogQueryBeyondEngine_ShouldReturnNothing() throws Exception {
    // a UNION over catalog tables now runs for real
    QueryParser.Plan union =
        parser.parse(
            "SELECT pubname FROM pg_catalog.pg_publication p WHERE p.puballtables "
                + "UNION SELECT pubname FROM pg_catalog.pg_publication ORDER BY 1");
    assertThat(union.getOutputColumns()).containsExactly("pubname");
    assertThat(union.execute(transaction)).isEmpty();

    // what the engine cannot plan at all still answers with no rows for catalog queries, with the
    // output columns described so drivers see an empty result rather than no result
    QueryParser.Plan plan =
        parser.parse(
            "SELECT relname, c.oid AS id, count(*) FROM pg_catalog.pg_class c, json_each('{}') j");
    assertThat(plan.getOutputColumns()).containsExactly("relname", "id", "count");
    assertThat(plan.execute(transaction)).isEmpty();
    assertThat(plan.commandTag(0)).isEqualTo("SELECT 0");
    assertThat(
            parser
                .parse("SELECT * FROM pg_catalog.pg_class EXCEPT ALL SELECT 'x'")
                .getOutputColumns())
        .isEmpty();
  }

  @Test
  public void parse_PsqlListDatabasesAndDescribePlus_ShouldAnswerFromMetadata() throws Exception {
    // psql \l as sent to a version-16 server
    assertThat(
            parser
                .parse(
                    "SELECT d.datname as \"Name\", pg_catalog.pg_get_userbyid(d.datdba) as \"Owner\", "
                        + "pg_catalog.pg_encoding_to_char(d.encoding) as \"Encoding\", "
                        + "d.daticulocale as \"Locale\", "
                        + "CASE WHEN pg_catalog.array_length(d.datacl, 1) = 0 THEN '(none)' "
                        + "ELSE pg_catalog.array_to_string(d.datacl, E'\\n') END AS \"Access privileges\" "
                        + "FROM pg_catalog.pg_database d ORDER BY 1")
                .execute(transaction))
        .containsExactly(
            map(
                "Name",
                NS,
                "Owner",
                "scalardb",
                "Encoding",
                "UTF8",
                "Locale",
                null,
                "Access privileges",
                null));

    // psql \d+ table header query: ARRAY(SELECT ... FROM unnest(...) x) and a self LEFT JOIN
    List<Map<String, Object>> found =
        parser
            .parse(
                "SELECT c.oid FROM pg_catalog.pg_class c WHERE c.relname = 't' AND c.relkind = 'r'")
            .execute(transaction);
    List<Map<String, Object>> header =
        parser
            .parse(
                "SELECT c.relkind, pg_catalog.array_to_string(c.reloptions || "
                    + "array(select 'toast.' || x from pg_catalog.unnest(tc.reloptions) x), ', ') "
                    + "AS options, am.amname FROM pg_catalog.pg_class c "
                    + "LEFT JOIN pg_catalog.pg_class tc ON (c.reltoastrelid = tc.oid) "
                    + "LEFT JOIN pg_catalog.pg_am am ON (c.relam = am.oid) WHERE c.oid = '"
                    + found.get(0).get("oid")
                    + "'")
            .execute(transaction);
    assertThat(header).containsExactly(map("relkind", "r", "options", null, "amname", "heap"));
  }

  @Test
  public void parse_CommaJoinOnPartitionKey_ShouldLookUpPerOuterRow() throws Exception {
    QueryParser.Plan plan =
        parser.parse(
            "SELECT a.name, b.name AS other FROM t a, t b WHERE b.id = a.id AND b.seq = 2 "
                + "AND b.price > 0");

    assertThat(parser.explain(plan))
        .containsExactly(
            "Project [name, other]",
            "  -> Lookup Join (per outer row): ScalarDB Get ns.t partitionKey={id=a.id} "
                + "clusteringKey={seq=2} conditions=[price > 0.0] "
                + "projections=[id, seq, price, name] AS b",
            "    -> ScalarDB ScanAll ns.t projections=[id, name] AS a");
    stubTable(Arrays.asList(row(1, 2, "x", 1.0, true), row(2, 2, "y", 1.0, true)));
    when(transaction.scan((Scan) plan.getOperations().get(0)))
        .thenReturn(
            Arrays.asList(
                row(1, 1, "a", 1.0, true), row(2, 1, "b", 1.0, true), row(3, 1, "c", 1.0, true)));
    assertThat(plan.execute(transaction))
        .containsExactly(map("name", "a", "other", "x"), map("name", "b", "other", "y"));
  }

  @Test
  public void parse_JoinOnNonKeyColumns_ShouldUseHashJoin() throws Exception {
    QueryParser.Plan plan =
        parser.parse(
            "SELECT a.name, b.name AS other FROM t a JOIN t b ON a.price = b.price AND a.id < b.id");

    assertThat(parser.explain(plan))
        .containsExactly(
            "Project [name, other]",
            "  -> Hash Join ON a.price = b.price AND a.id < b.id",
            "    -> ScalarDB ScanAll ns.t projections=[price, id, name] AS a",
            "    -> ScalarDB ScanAll ns.t projections=[price, id, name] AS b");
    stubTable(
        Arrays.asList(
            row(1, 1, "a", 1.0, true),
            row(2, 1, "b", 1.0, true),
            row(3, 1, "c", 2.0, true),
            row(4, 1, "d", 1.0, true)));
    assertThat(plan.execute(transaction))
        .containsExactly(
            map("name", "a", "other", "b"),
            map("name", "a", "other", "d"),
            map("name", "b", "other", "d"));
  }

  @Test
  public void open_LimitAfterInMemoryFilter_ShouldStopPullingAndCloseScanner() throws Exception {
    List<Result> rows = new ArrayList<>();
    for (int i = 1; i <= 100; i++) {
      rows.add(row(i, 1, "n" + i, 1.0, i % 2 == 0));
    }
    java.util.Iterator<Result> iterator = rows.iterator();
    java.util.concurrent.atomic.AtomicInteger pulled =
        new java.util.concurrent.atomic.AtomicInteger();
    com.scalar.db.api.TransactionCrudOperable.Scanner scanner =
        mock(com.scalar.db.api.TransactionCrudOperable.Scanner.class);
    when(scanner.one())
        .thenAnswer(
            invocation -> {
              pulled.incrementAndGet();
              return iterator.hasNext()
                  ? java.util.Optional.of(iterator.next())
                  : java.util.Optional.empty();
            });
    org.mockito.Mockito.doReturn(scanner).when(transaction).getScanner(any());

    // "active" is filtered in memory (NOT), so the LIMIT cannot be pushed down
    QueryParser.Plan plan = parser.parse("SELECT name FROM t WHERE NOT active LIMIT 2");
    try (QueryParser.Cursor cursor = plan.open(transaction)) {
      assertThat(cursor.next()).isEqualTo(map("name", "n1"));
      assertThat(cursor.next()).isEqualTo(map("name", "n3"));
      assertThat(cursor.next()).isNull();
      verify(scanner, never()).close();
    }
    verify(scanner).close();
    assertThat(pulled.get()).isEqualTo(3); // n1, n2 (filtered out), n3; nothing after the limit
  }

  @Test
  public void parse_ShouldBindThenOptimizeIntoLogicalPlan() throws Exception {
    QueryParser.Plan plan =
        parser.parse(
            "SELECT a.name, b.name AS other FROM t a JOIN t b ON b.id = a.id AND b.seq = 2 "
                + "WHERE a.price > 1 AND a.name <> b.name");

    LogicalPlan logical = plan.getLogical();
    assertThat(logical).isNotNull();
    assertThat(logical.sources).extracting(s -> s.qualifier).containsExactly("a", "b");
    assertThat(logical.outputNames).containsExactly("name", "other");
    // pushed to ScalarDB: a.price > 1 (WHERE) and b.seq = 2 (ON)
    assertThat(logical.sources.get(0).pushed)
        .extracting(c -> c.getColumn().getName())
        .containsExactly("price");
    assertThat(logical.sources.get(1).pushed)
        .extracting(c -> c.getColumn().getName())
        .containsExactly("seq");
    // b.id = a.id became a keyed lookup of b; a.name <> b.name stays a WHERE conjunct
    assertThat(logical.sources.get(1).lookupParams.keySet()).containsExactly("id");
    assertThat(logical.joins.get(0).on).isEmpty();
    assertThat(logical.where).extracting(Object::toString).containsExactly("a.name <> b.name");
  }

  @Test
  public void parse_CreateTable_ShouldBuildMetadataFromKeysAndOptions() throws Exception {
    QueryParser.Plan plan =
        parser.parse(
            "CREATE TABLE ns.o (customer_id int, order_id bigint, item text, "
                + "amount double precision, paid boolean, PRIMARY KEY (customer_id, order_id)) "
                + "WITH (clustering_key = 'order_id DESC')");
    assertThat(plan.commandTag(0)).isEqualTo("CREATE TABLE");
    plan.getDdl().run();
    verify(admin)
        .createTable(
            NS,
            "o",
            TableMetadata.newBuilder()
                .addColumn("customer_id", DataType.INT)
                .addColumn("order_id", DataType.BIGINT)
                .addColumn("item", DataType.TEXT)
                .addColumn("amount", DataType.DOUBLE)
                .addColumn("paid", DataType.BOOLEAN)
                .addPartitionKey("customer_id")
                .addClusteringKey("order_id", Scan.Ordering.Order.DESC)
                .build(),
            false);

    parser
        .parse(
            "CREATE TABLE IF NOT EXISTS c (id int PRIMARY KEY, name varchar(20), created timestamptz)")
        .getDdl()
        .run();
    verify(admin)
        .createTable(
            NS,
            "c",
            TableMetadata.newBuilder()
                .addColumn("id", DataType.INT)
                .addColumn("name", DataType.TEXT)
                .addColumn("created", DataType.TIMESTAMPTZ)
                .addPartitionKey("id")
                .build(),
            true);
    assertThatThrownBy(() -> parser.parse("CREATE TABLE nokey (id int, name text)"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("PRIMARY KEY");
  }

  @Test
  public void parse_OtherDdl_ShouldCallAdmin() throws Exception {
    when(admin.getNamespaceTableNames(NS)).thenReturn(Collections.singleton("t"));
    when(admin.namespaceExists(NS)).thenReturn(true);

    parser.parse("CREATE SCHEMA IF NOT EXISTS shop").getDdl().run();
    verify(admin).createNamespace("shop", true);
    parser.parse("CREATE INDEX t_price ON t (price)").getDdl().run();
    verify(admin).createIndex(NS, "t", "price", false);
    parser.parse("DROP INDEX t_name_idx").getDdl().run();
    verify(admin).dropIndex(NS, "t", "name");
    parser.parse("ALTER TABLE t ADD COLUMN note text, ADD flag boolean").getDdl().run();
    verify(admin).addNewColumnToTable(NS, "t", "note", DataType.TEXT);
    verify(admin).addNewColumnToTable(NS, "t", "flag", DataType.BOOLEAN);
    parser.parse("TRUNCATE t").getDdl().run();
    verify(admin).truncateTable(NS, "t");
    parser.parse("DROP TABLE IF EXISTS ns.t").getDdl().run();
    verify(admin).dropTable(NS, "t", true);
    // public is the connected namespace, in DDL as in queries
    parser.parse("TRUNCATE public.t").getDdl().run();
    verify(admin, times(2)).truncateTable(NS, "t");
    parser.parse("CREATE SCHEMA public").getDdl().run();
    verify(admin).createNamespace(NS, false);
    assertThatThrownBy(() -> parser.parse("DROP SCHEMA public CASCADE"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("public");
    parser.parse("DROP SCHEMA ns CASCADE").getDdl().run();
    verify(admin).dropNamespace(NS, false);
    parser.parse("CREATE COORDINATOR TABLES").getDdl().run();
    verify(admin).createCoordinatorTables(true);
    assertThat(parser.parse("DROP SCHEMA ns CASCADE").commandTag(0)).isEqualTo("DROP SCHEMA");
    assertThat(parser.parse("SELECT 1").getDdl()).isNull();
  }

  @Test
  public void parse_InListOnPartitionKey_ShouldLookUpEachValue() throws Exception {
    QueryParser.Plan plan =
        parser.parse("SELECT name FROM t WHERE id IN (2, 1, 2, NULL) AND seq = 1 AND price > 0");

    assertThat(parser.explain(plan))
        .containsExactly(
            "Project [name]",
            "  -> Lookup Join (per outer row): ScalarDB Get ns.t partitionKey={id=$in1.v} "
                + "clusteringKey={seq=1} conditions=[price > 0.0] "
                + "projections=[id, seq, price, name] AS t",
            "    -> Values IN (2, 1) AS $in1 (in memory)");
    stubTable(
        Arrays.asList(
            row(1, 1, "a", 1.0, true), row(2, 1, "b", 1.0, true), row(3, 1, "c", 1.0, true)));
    assertThat(plan.execute(transaction)).containsExactly(map("name", "b"), map("name", "a"));

    // OR of equalities on the key is the same thing
    assertThat(
            String.join(
                "\n", parser.explain(parser.parse("SELECT name FROM t WHERE id = 1 OR id = 2"))))
        .contains("Values IN (1, 2) AS $in1");
    // an IN on a non-key column stays an in-memory filter
    assertThat(
            String.join(
                "\n", parser.explain(parser.parse("SELECT name FROM t WHERE price IN (1, 2)"))))
        .contains("Filter t.price IN (1, 2)");
  }

  @Test
  public void parse_OrOnOneTable_ShouldPushDisjunctionToScalarDb() throws Exception {
    QueryParser.Plan plan =
        parser.parse(
            "SELECT name FROM t WHERE (price > 5 OR active = TRUE AND seq < 3) AND name LIKE 'a%'");

    Set<AndConditionSet> alternatives = new LinkedHashSet<>();
    alternatives.add(
        ConditionSetBuilder.andConditionSet(
                new HashSet<>(
                    Arrays.asList(
                        ConditionBuilder.column("name").isLikeText("a%", "\\"),
                        ConditionBuilder.column("price").isGreaterThanDouble(5))))
            .build());
    alternatives.add(
        ConditionSetBuilder.andConditionSet(
                new HashSet<>(
                    Arrays.asList(
                        ConditionBuilder.column("name").isLikeText("a%", "\\"),
                        ConditionBuilder.column("active").isEqualToBoolean(true),
                        ConditionBuilder.column("seq").isLessThanBigInt(3))))
            .build());
    assertThat(plan.getOperations())
        .containsExactly(
            Scan.newBuilder()
                .namespace(NS)
                .table("t")
                .all()
                .projections("price", "active", "seq", "name")
                .whereOr(alternatives)
                .build());
    assertThat(parser.explain(plan))
        .containsExactly(
            "Project [name]",
            "  -> ScalarDB ScanAll ns.t conditions=[active = true AND name LIKE 'a%' AND seq < 3 OR "
                + "name LIKE 'a%' AND price > 5.0] "
                + "projections=[price, active, seq, name] AS t");
    // an OR spanning two tables cannot be pushed down
    assertThat(
            String.join(
                "\n",
                parser.explain(
                    parser.parse(
                        "SELECT a.name FROM t a, t b WHERE b.id = a.id AND (a.price > 1 OR b.price > 1)"))))
        .contains("Filter a.price > 1 OR b.price > 1");
  }

  @Test
  public void parse_SetOperations_ShouldCombineMembers() throws Exception {
    stubTable(
        Arrays.asList(
            row(1, 1, "a", 1.0, true),
            row(1, 2, "a", 1.0, true),
            row(2, 1, "b", 1.0, true),
            row(3, 1, "c", 1.0, true)));

    QueryParser.Plan union =
        parser.parse(
            "SELECT name FROM t WHERE id = 1 UNION SELECT name FROM t WHERE id = 2 "
                + "ORDER BY 1 DESC LIMIT 5");
    assertThat(parser.explain(union))
        .containsExactly(
            "Limit 5",
            "  -> Sort 1 DESC",
            "    -> Union",
            "      -> Project [name]",
            "        -> ScalarDB Scan ns.t partitionKey={id=1} projections=[id, name] AS t",
            "      -> Project [name]",
            "        -> ScalarDB Scan ns.t partitionKey={id=2} projections=[id, name] AS t");
    assertThat(union.getOutputColumns()).containsExactly("name");
    assertThat(union.execute(transaction)).containsExactly(map("name", "b"), map("name", "a"));

    assertThat(
            parser
                .parse(
                    "SELECT name FROM t WHERE id = 1 UNION ALL SELECT name AS other FROM t WHERE id = 2")
                .execute(transaction))
        .containsExactly(map("name", "a"), map("name", "a"), map("name", "b"));
    assertThat(
            parser
                .parse("SELECT name FROM t INTERSECT SELECT name FROM t WHERE id = 1")
                .execute(transaction))
        .containsExactly(map("name", "a"));
    assertThat(
            parser
                .parse("SELECT name FROM t EXCEPT SELECT name FROM t WHERE id = 1")
                .execute(transaction))
        .containsExactly(map("name", "b"), map("name", "c"));
    // INTERSECT binds tighter than UNION: a UNION (b INTERSECT c)
    assertThat(
            String.join(
                "\n",
                parser.explain(
                    parser.parse(
                        "SELECT name FROM t WHERE id = 1 UNION SELECT name FROM t "
                            + "INTERSECT SELECT name FROM t WHERE id = 2"))))
        .startsWith("Union\n  -> Project [name]\n    -> ScalarDB Scan ns.t partitionKey={id=1}")
        .contains("  -> Intersect");
    // VALUES as a member, and a set operation as a subquery
    assertThat(parser.parse("SELECT 1 AS x UNION ALL VALUES (2), (3)").execute(transaction))
        .containsExactly(map("x", 1L), map("x", 2L), map("x", 3L));
    assertThat(
            parser
                .parse("SELECT name FROM t WHERE id IN (SELECT 2 UNION ALL SELECT 3) ORDER BY name")
                .execute(transaction))
        .containsExactly(map("name", "b"), map("name", "c"));
    assertThatThrownBy(() -> parser.parse("SELECT name FROM t UNION SELECT name, id FROM t"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("same number of columns");
  }

  @Test
  public void parse_UpdateByScan_ShouldReadThenUpdateEachRowUnderTheCap() throws Exception {
    QueryParser.Plan plan =
        parser.parse("UPDATE t SET price = price * 2, active = FALSE WHERE id = 1 AND NOT active");

    assertThat(plan.getWrite()).isNotNull();
    assertThat(plan.commandTag(3)).isEqualTo("UPDATE 3");
    assertThat(parser.explain(plan))
        .containsExactly(
            "Update ns.t: one ScalarDB Update per row read",
            "  -> Project [id, seq, $1, $2]",
            "    -> Filter NOT t.active",
            "      -> ScalarDB Scan ns.t partitionKey={id=1} projections=[id, active, seq, price] AS t");
    stubTable(Arrays.asList(row(1, 1, "a", 1.5, true), row(1, 2, "b", 0.5, false)));
    assertThat(plan.executeWrite(transaction, 10)).isEqualTo(1);
    verify(transaction)
        .mutate(
            Collections.singletonList(
                Update.newBuilder()
                    .namespace(NS)
                    .table("t")
                    .partitionKey(Key.ofInt("id", 1))
                    .clusteringKey(Key.ofBigInt("seq", 2))
                    .doubleValue("price", 1.0)
                    .booleanValue("active", false)
                    .build()));

    // the cap is checked before anything is written
    QueryParser.Plan all = parser.parse("UPDATE t SET active = TRUE WHERE id = 1");
    assertThatThrownBy(() -> all.executeWrite(transaction, 1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("max_rows_per_write");
    verify(transaction, times(1)).mutate(any());
    assertThatThrownBy(() -> parser.parse("UPDATE t SET id = 2 WHERE seq = 1"))
        .hasMessageContaining("primary key column");
  }

  @Test
  public void execute_JsonOperatorsAndFunctions_ShouldFollowPostgres() throws Exception {
    String j = "'{\"a\": {\"b\": [10, \"s\"]}, \"n\": 1.50, \"t\": \"x\"}'::jsonb";
    assertThat(
            parser
                .parse(
                    "SELECT "
                        + j
                        + " -> 'a' -> 'b' ->> 0 AS b0, "
                        + j
                        + " #>> '{a,b,1}' AS b1, "
                        + j
                        + " ->> 'n' AS n, "
                        + j
                        + " -> 'zz' AS zz, "
                        + j
                        + " @> '{\"t\": \"x\"}' AS has, "
                        + j
                        + " ? 'n' AS k, ("
                        + j
                        + " || '{\"z\": null}') - 'a' AS edited, jsonb_typeof("
                        + j
                        + " -> 'a') AS kind")
                .execute(transaction))
        .containsExactly(
            map(
                "b0",
                "10",
                "b1",
                "s",
                "n",
                "1.50",
                "zz",
                null,
                "has",
                true,
                "k",
                true,
                "edited",
                Json.cast("{\"n\": 1.50, \"t\": \"x\", \"z\": null}", true),
                "kind",
                "object"));
    // jsonb prints sorted by key length then bytes; json keeps its order and " : "
    assertThat(
            parser
                .parse(
                    "SELECT jsonb_build_object('bb', 1, 'a', ARRAY[1, 2], 'B', true)::text AS b,"
                        + " json_build_object('b', 1, 'a', 'x')::text AS j,"
                        + " '{\"b\":1,\"a\":2}'::json::text AS v")
                .execute(transaction))
        .containsExactly(
            map(
                "b",
                "{\"B\": true, \"a\": [1, 2], \"bb\": 1}",
                "j",
                "{\"b\" : 1, \"a\" : \"x\"}",
                "v",
                "{\"b\":1,\"a\":2}"));
    assertThat(
            parser
                .parse(
                    "SELECT k, json_agg(v ORDER BY v)::text AS vs,"
                        + " jsonb_object_agg(k, v ORDER BY v)::text AS o"
                        + " FROM (VALUES ('x', 2), ('x', 1), ('y', NULL)) AS t(k, v)"
                        + " GROUP BY k ORDER BY k")
                .execute(transaction))
        .containsExactly(
            map("k", "x", "vs", "[1, 2]", "o", "{\"x\": 2}"), // the last value of a key wins
            map("k", "y", "vs", "[null]", "o", "{\"y\": null}"));
    assertThat(
            parser
                .parse(
                    "SELECT t.id, e.v FROM (VALUES (1, '[\"b\", \"a\"]'), (2, '[]')) AS t(id, j),"
                        + " jsonb_array_elements_text(t.j::jsonb) AS e(v) ORDER BY t.id, e.v")
                .execute(transaction))
        .containsExactly(map("id", 1L, "v", "a"), map("id", 1L, "v", "b"));
  }

  @Test
  public void parse_LateralSubquery_ShouldRunPerLeftRow() throws Exception {
    String q = "(VALUES (1), (2), (3)) AS q(id)";
    assertThat(
            parser
                .parse(
                    "SELECT p.id, l.m FROM (VALUES (1), (2), (3)) AS p(id)"
                        + " JOIN LATERAL (SELECT max(q.id) AS m FROM "
                        + q
                        + " WHERE q.id < p.id) l ON true ORDER BY p.id")
                .execute(transaction))
        .containsExactly(map("id", 1L, "m", null), map("id", 2L, "m", 1L), map("id", 3L, "m", 2L));
    // LEFT JOIN LATERAL keeps a left row whose subquery yields nothing; the comma form is a cross
    assertThat(
            parser
                .parse(
                    "SELECT p.id, l.m FROM (VALUES (1), (3)) AS p(id)"
                        + " LEFT JOIN LATERAL (SELECT q.id AS m FROM "
                        + q
                        + " WHERE q.id < p.id ORDER BY q.id DESC LIMIT 1) l ON true ORDER BY p.id")
                .execute(transaction))
        .containsExactly(map("id", 1L, "m", null), map("id", 3L, "m", 2L));
    assertThat(
            parser
                .parse(
                    "SELECT count(*) AS n FROM (VALUES (2), (3)) AS p(id), LATERAL (SELECT q.id"
                        + " FROM "
                        + q
                        + " WHERE q.id < p.id) l")
                .execute(transaction))
        .containsExactly(map("n", 3L));
    assertThat(
            parser.explain(
                parser.parse(
                    "SELECT p.id FROM (VALUES (1)) AS p(id) JOIN LATERAL (SELECT 1 AS x) l ON true")))
        .anyMatch(line -> line.contains("Lateral INNER join l (subquery per left row)"));
  }

  @Test
  public void parse_UpdateFromAndDeleteUsing_ShouldJoinThenWriteEachTargetKeyOnce()
      throws Exception {
    stubTable(Arrays.asList(row(1, 1, "a", 1.5, true), row(2, 2, "b", 2.5, false)));

    // two joined rows match the same target row: PostgreSQL writes it once
    QueryParser.Plan update =
        parser.parse(
            "UPDATE t SET name = v.n FROM (VALUES (1.5, 'x'), (1.5, 'y')) AS v(p, n)"
                + " WHERE t.price = v.p");
    assertThat(update.getWrite()).isNotNull();
    assertThat(parser.explain(update).get(0)).contains("joined with FROM");
    assertThat(update.executeWrite(transaction, 10)).isEqualTo(1);
    verify(transaction)
        .mutate(
            Collections.singletonList(
                Update.newBuilder()
                    .namespace(NS)
                    .table("t")
                    .partitionKey(Key.ofInt("id", 1))
                    .clusteringKey(Key.ofBigInt("seq", 1))
                    .textValue("name", "x")
                    .build()));

    QueryParser.Plan delete =
        parser.parse("DELETE FROM t USING (VALUES (2.5)) AS v(p) WHERE t.price = v.p");
    assertThat(delete.executeWrite(transaction, 10)).isEqualTo(1);
    verify(transaction)
        .mutate(
            Collections.singletonList(
                Delete.newBuilder()
                    .namespace(NS)
                    .table("t")
                    .partitionKey(Key.ofInt("id", 2))
                    .clusteringKey(Key.ofBigInt("seq", 2))
                    .build()));
  }

  @Test
  public void parse_DeleteByScanAndInsertSelect_ShouldReadThenMutate() throws Exception {
    stubTable(Arrays.asList(row(1, 2, "a", 1.5, true), row(2, 2, "b", 2.5, false)));

    QueryParser.Plan delete = parser.parse("DELETE FROM t WHERE seq = 2");
    assertThat(parser.explain(delete))
        .containsExactly(
            "Delete ns.t: one ScalarDB Delete per row read",
            "  -> Project [id, seq]",
            "    -> ScalarDB ScanAll ns.t conditions=[seq = 2] projections=[seq, id] AS t");
    assertThat(delete.executeWrite(transaction, 100)).isEqualTo(2);
    verify(transaction)
        .mutate(
            Arrays.asList(
                Delete.newBuilder()
                    .namespace(NS)
                    .table("t")
                    .partitionKey(Key.ofInt("id", 1))
                    .clusteringKey(Key.ofBigInt("seq", 2))
                    .build(),
                Delete.newBuilder()
                    .namespace(NS)
                    .table("t")
                    .partitionKey(Key.ofInt("id", 2))
                    .clusteringKey(Key.ofBigInt("seq", 2))
                    .build()));

    QueryParser.Plan copy =
        parser.parse(
            "INSERT INTO t (id, seq, name, price, active) "
                + "SELECT id + 10, seq, UPPER(name), price, active FROM t WHERE id = 1");
    assertThat(copy.commandTag(1)).isEqualTo("INSERT 0 1");
    assertThat(copy.executeWrite(transaction, 100)).isEqualTo(1);
    verify(transaction)
        .mutate(
            Collections.singletonList(
                Insert.newBuilder()
                    .namespace(NS)
                    .table("t")
                    .partitionKey(Key.ofInt("id", 11))
                    .clusteringKey(Key.ofBigInt("seq", 2))
                    .textValue("name", "A")
                    .doubleValue("price", 1.5)
                    .booleanValue("active", true)
                    .build()));
    assertThatThrownBy(() -> parser.parse("INSERT INTO t (id, seq) SELECT id FROM t"))
        .hasMessageContaining("one value per target column");
    assertThat(parser.parse("EXPLAIN DELETE FROM t WHERE seq = 2").execute(transaction))
        .extracting(r -> r.get("QUERY PLAN"))
        .containsExactly(
            "Delete ns.t: one ScalarDB Delete per row read",
            "  -> Project [id, seq]",
            "    -> ScalarDB ScanAll ns.t conditions=[seq = 2] projections=[seq, id] AS t");
  }

  @Test
  public void parse_ExplainAnalyze_ShouldRunTheQueryAndReportActualCounts() throws Exception {
    stubTable(
        Arrays.asList(
            row(1, 1, "a", 1.5, true), row(1, 2, "b", 0.5, false), row(2, 1, "c", 2.0, true)));

    List<Map<String, Object>> report =
        parser
            .parse(
                "EXPLAIN ANALYZE SELECT a.name, b.name FROM t a JOIN t b"
                    + " ON b.id = a.id AND b.seq = a.seq WHERE a.id = 1 AND a.price > 1")
            .execute(transaction);

    assertThat(report)
        .extracting(r -> (String) r.get("QUERY PLAN"))
        .satisfiesExactly(
            l -> assertThat(l).isEqualTo("Project [name, name_2] (actual rows=2)"),
            l ->
                assertThat(l)
                    .isEqualTo(
                        "  -> Lookup Join (per outer row): ScalarDB Get ns.t"
                            + " partitionKey={id=a.id} clusteringKey={seq=a.seq}"
                            + " projections=[id, seq, name] AS b"
                            + " (actual rows=2 ScalarDB reads=1)"),
            l ->
                assertThat(l)
                    .isEqualTo(
                        "    -> ScalarDB Scan ns.t partitionKey={id=1} conditions=[price > 1.0]"
                            + " projections=[id, price, seq, name] AS a"
                            + " (actual rows=2 ScalarDB reads=1)"),
            l -> assertThat(l).matches("Execution Time: \\d+\\.\\d{3} ms"));
  }

  @Test
  public void parse_Cte_ShouldPlanEachReferenceAsASubquery() throws Exception {
    stubTable(Arrays.asList(row(1, 1, "a", 1.5, true), row(1, 2, "b", 0.5, false)));

    QueryParser.Plan plan =
        parser.parse(
            "WITH cheap AS (SELECT id, seq, price FROM t WHERE id = 1 AND price < 1),"
                + " v(k, label) AS (VALUES (2, 'two'), (3, 'three'))"
                + " SELECT cheap.seq, v.label FROM cheap JOIN v ON v.k = cheap.seq");
    assertThat(parser.explain(plan))
        .containsExactly(
            "Project [seq, label]",
            "  -> Hash Join ON cheap.seq = v.k",
            "    -> Subquery AS cheap",
            "      -> Project [id, seq, price]",
            "        -> ScalarDB Scan ns.t partitionKey={id=1} conditions=[price < 1.0]"
                + " projections=[id, price, seq] AS t",
            "    -> Subquery AS v",
            "      -> Values");
    assertThat(plan.execute(transaction)).containsExactly(row("seq", 2L, "label", "two"));

    // a CTE sees the ones before it
    assertThat(
            parser
                .parse("WITH a AS (SELECT 1 AS x), b AS (SELECT x + 1 AS y FROM a) SELECT y FROM b")
                .execute(transaction))
        .containsExactly(Collections.singletonMap("y", 2L));
  }

  @Test
  public void parse_InsertOnConflict_ShouldUpsertOrReadThenDecide() throws Exception {
    stubTable(Collections.singletonList(row(1, 1, "a", 1.5, true)));

    // the plain EXCLUDED form is a ScalarDB Upsert
    QueryParser.Plan upsert =
        parser.parse(
            "INSERT INTO t (id, seq, name) VALUES (1, 1, 'z')"
                + " ON CONFLICT (id, seq) DO UPDATE SET name = EXCLUDED.name");
    assertThat(upsert.getOperations())
        .containsExactly(
            Upsert.newBuilder()
                .namespace(NS)
                .table("t")
                .partitionKey(Key.ofInt("id", 1))
                .clusteringKey(Key.ofBigInt("seq", 1))
                .textValue("name", "z")
                .build());
    assertThat(upsert.commandTag(0)).isEqualTo("INSERT 0 1");

    // otherwise each row is looked up first
    QueryParser.Plan merge =
        parser.parse(
            "INSERT INTO t (id, seq, name, price, active) VALUES (1, 1, 'z', 9.0, TRUE),"
                + " (3, 3, 'new', 1.0, FALSE)"
                + " ON CONFLICT (id, seq) DO UPDATE SET price = t.price + EXCLUDED.price"
                + " WHERE t.active RETURNING id, seq, name, price");
    assertThat(merge.execute(transaction))
        .containsExactly(
            row("id", 1, "seq", 1L, "name", "a", "price", 10.5),
            row("id", 3, "seq", 3L, "name", "new", "price", 1.0));
    verify(transaction)
        .mutate(
            Arrays.asList(
                Update.newBuilder()
                    .namespace(NS)
                    .table("t")
                    .partitionKey(Key.ofInt("id", 1))
                    .clusteringKey(Key.ofBigInt("seq", 1))
                    .doubleValue("price", 10.5)
                    .build(),
                Insert.newBuilder()
                    .namespace(NS)
                    .table("t")
                    .partitionKey(Key.ofInt("id", 3))
                    .clusteringKey(Key.ofBigInt("seq", 3))
                    .textValue("name", "new")
                    .doubleValue("price", 1.0)
                    .booleanValue("active", false)
                    .build()));

    QueryParser.Plan skip =
        parser.parse(
            "INSERT INTO t (id, seq, name) VALUES (1, 1, 'dup'), (5, 5, 'n')"
                + " ON CONFLICT DO NOTHING");
    assertThat(skip.executeWrite(transaction, 100)).isEqualTo(1);
    verify(transaction)
        .mutate(
            Collections.singletonList(
                Insert.newBuilder()
                    .namespace(NS)
                    .table("t")
                    .partitionKey(Key.ofInt("id", 5))
                    .clusteringKey(Key.ofBigInt("seq", 5))
                    .textValue("name", "n")
                    .build()));
    assertThatThrownBy(
            () ->
                parser.parse("INSERT INTO t (id, seq) VALUES (1, 1) ON CONFLICT (name) DO NOTHING"))
        .hasMessageContaining("primary key");
  }

  @Test
  public void parse_Returning_ShouldYieldTheRowsLeftByTheWrite() throws Exception {
    stubTable(Arrays.asList(row(1, 1, "a", 1.5, true), row(1, 2, "b", 0.5, false)));

    QueryParser.Plan update =
        parser.parse("UPDATE t SET price = price * 2 WHERE id = 1 RETURNING seq, price AS p");
    assertThat(update.getOutputColumns()).containsExactly("seq", "p");
    assertThat(update.execute(transaction))
        .containsExactly(row("seq", 1L, "p", 3.0), row("seq", 2L, "p", 1.0));

    // a keyed DELETE with RETURNING reads the row first (with a Get)
    QueryParser.Plan delete = parser.parse("DELETE FROM t WHERE id = 1 AND seq = 2 RETURNING *");
    assertThat(parser.explain(delete))
        .containsExactly(
            "Delete ns.t: one ScalarDB Delete per row read",
            "  -> Project [id, seq, name, price, active]",
            "    -> ScalarDB Get ns.t partitionKey={id=1} clusteringKey={seq=2}"
                + " projections=[id, seq, name, price, active] AS t");
    assertThat(delete.execute(transaction))
        .containsExactly(row("id", 1, "seq", 2L, "name", "b", "price", 0.5, "active", false));

    QueryParser.Plan insert =
        parser.parse(
            "INSERT INTO t (id, seq, name) VALUES (7, 7, 'g') RETURNING id * 2 AS d, name");
    assertThat(insert.execute(transaction)).containsExactly(row("d", 14, "name", "g"));
    assertThat(insert.commandTag(1)).isEqualTo("INSERT 0 1");
  }

  private static Map<String, Object> row(Object... namesAndValues) {
    Map<String, Object> row = new LinkedHashMap<>();
    for (int i = 0; i < namesAndValues.length; i += 2) {
      row.put((String) namesAndValues[i], namesAndValues[i + 1]);
    }
    return row;
  }

  @Test
  public void parse_RightFullUsingAndNaturalJoins_ShouldNullExtendEitherSide() throws Exception {
    stubTable(Arrays.asList(row(1, 1, "a", 1.5, true), row(2, 1, "b", 2.5, false)));

    QueryParser.Plan right =
        parser.parse(
            "SELECT t.name, v.k FROM t RIGHT JOIN (VALUES (1), (3)) AS v(k) ON v.k = t.id"
                + " ORDER BY v.k");
    assertThat(parser.explain(right)).anyMatch(l -> l.contains("Right Hash Join ON t.id = v.k"));
    assertThat(right.execute(transaction))
        .containsExactly(row("name", "a", "k", 1L), row("name", null, "k", 3L));

    // FULL JOIN ... USING: the merged column comes out once, from whichever side has it
    QueryParser.Plan full =
        parser.parse(
            "SELECT * FROM (VALUES (1, 'x'), (3, 'z')) AS v(id, tag) FULL JOIN t USING (id)"
                + " ORDER BY id");
    assertThat(full.getOutputColumns())
        .containsExactly("id", "v.tag", "t.seq", "t.name", "t.price", "t.active");
    assertThat(full.execute(transaction))
        .containsExactly(
            row(
                "id",
                1L,
                "v.tag",
                "x",
                "t.seq",
                1L,
                "t.name",
                "a",
                "t.price",
                1.5,
                "t.active",
                true),
            row(
                "id",
                2,
                "v.tag",
                null,
                "t.seq",
                1L,
                "t.name",
                "b",
                "t.price",
                2.5,
                "t.active",
                false),
            row(
                "id",
                3L,
                "v.tag",
                "z",
                "t.seq",
                null,
                "t.name",
                null,
                "t.price",
                null,
                "t.active",
                null));

    // NATURAL JOIN: the common column is unambiguous and the lookup still applies
    QueryParser.Plan natural =
        parser.parse("SELECT id, name, tag FROM (VALUES (2, 'y')) AS v(id, tag) NATURAL JOIN t");
    assertThat(parser.explain(natural))
        .anyMatch(
            l ->
                l.contains(
                    "Lookup Join (per outer row): ScalarDB Scan ns.t partitionKey={id=v.id}"));
    assertThat(natural.execute(transaction))
        .containsExactly(row("id", 2L, "name", "b", "tag", "y"));

    // a WHERE condition on the null-extended side stays after the join
    QueryParser.Plan where =
        parser.parse(
            "SELECT v.k FROM t RIGHT JOIN (VALUES (1), (3)) AS v(k) ON v.k = t.id"
                + " WHERE t.name IS NULL");
    assertThat(where.execute(transaction)).containsExactly(row("k", 3L));
    assertThatThrownBy(() -> parser.parse("SELECT * FROM t JOIN (VALUES (1)) AS v(x) USING (id)"))
        .hasMessageContaining("USING column");
  }

  @Test
  public void parse_SelectForUpdate_ShouldPlanLikeAPlainSelect() throws Exception {
    QueryParser.Plan plan = parser.parse("SELECT name FROM t WHERE id = 1 AND seq = 2 FOR UPDATE");

    // Locks are not taken: Consensus Commit validates the read at commit time instead
    assertThat(plan.getOperations())
        .containsExactly(
            Get.newBuilder()
                .namespace(NS)
                .table("t")
                .partitionKey(Key.ofInt("id", 1))
                .clusteringKey(Key.ofBigInt("seq", 2))
                .projections("id", "seq", "name")
                .build());
  }

  @Test
  public void parse_WithParameters_ShouldPlanLikeLiteralsAndReuseTheParsedStatement()
      throws Exception {
    String sql = "SELECT name FROM t WHERE id = $1 AND seq = $2 AND price > $3";
    QueryParser.Plan first = parser.parse(sql, Arrays.asList(1L, 2L, 0.5));
    QueryParser.Plan second = parser.parse(sql, Arrays.asList(3L, 4L, 1.5));

    assertThat(first.getOperations())
        .containsExactly(
            Get.newBuilder()
                .namespace(NS)
                .table("t")
                .partitionKey(Key.ofInt("id", 1))
                .clusteringKey(Key.ofBigInt("seq", 2))
                .projections("id", "seq", "price", "name")
                .where(ConditionBuilder.column("price").isGreaterThanDouble(0.5))
                .build());
    assertThat(second.getOperations())
        .containsExactly(
            Get.newBuilder()
                .namespace(NS)
                .table("t")
                .partitionKey(Key.ofInt("id", 3))
                .clusteringKey(Key.ofBigInt("seq", 4))
                .projections("id", "seq", "price", "name")
                .where(ConditionBuilder.column("price").isGreaterThanDouble(1.5))
                .build());
    // the plan is bound to the placeholders, not to the values, so it can be reused
    assertThat(parser.explain(second))
        .containsExactly(
            "Project [name]",
            "  -> Lookup (per outer row): ScalarDB Get ns.t partitionKey={id=$1}"
                + " clusteringKey={seq=$2} projections=[id, seq, price, name]"
                + " conditions=t.price > $3 AS t");
    // LIMIT takes a parameter too (pushed into the scan here)
    assertThat(
            parser.explain(
                parser.parse("SELECT name FROM t WHERE id = $1 LIMIT $2", Arrays.asList(1L, 3L))))
        .anyMatch(l -> l.contains("limit=3"));

    // a null parameter behaves like a NULL literal (not pushed), a string one like a string
    assertThat(
            parser
                .parse("SELECT id FROM t WHERE price = $1", Collections.singletonList(null))
                .getOperations())
        .allSatisfy(op -> assertThat(op).isInstanceOf(com.scalar.db.api.ScanAll.class));
    assertThat(
            parser
                .parse(
                    "INSERT INTO t (id, seq, name, active) VALUES ($1, $2, $3, $4)",
                    Arrays.asList(7L, 8L, "n", true))
                .getOperations())
        .containsExactly(
            Insert.newBuilder()
                .namespace(NS)
                .table("t")
                .partitionKey(Key.ofInt("id", 7))
                .clusteringKey(Key.ofBigInt("seq", 8))
                .textValue("name", "n")
                .booleanValue("active", true)
                .build());

    // parameters are evaluated in memory too, with the values the plan was made with
    stubTable(Arrays.asList(row(1, 1, "a", 1.5, true), row(1, 2, "b", 0.5, false)));
    assertThat(
            parser
                .parse(
                    "SELECT name, price * $1 AS p FROM t WHERE id = $2 ORDER BY seq",
                    Arrays.asList(2L, 1L))
                .execute(transaction))
        .containsExactly(row("name", "a", "p", 3.0), row("name", "b", "p", 1.0));
    assertThatThrownBy(
            () -> parser.parse("SELECT name FROM t WHERE id = $2", Collections.singletonList(1L)))
        .hasMessageContaining("parameter $2");
  }

  @Test
  public void parse_CacheablePlan_ShouldRebindToOtherValues() throws Exception {
    stubTable(Arrays.asList(row(1, 1, "a", 1.5, true), row(1, 2, "b", 0.5, false)));
    QueryParser.Plan plan =
        parser.parse("SELECT name FROM t WHERE id = $1 AND price > $2", Arrays.asList(1L, 1.0));

    assertThat(plan.isCacheable()).isTrue();
    // the stubbed scan ignores pushed conditions: the read itself is checked below
    assertThat(plan.execute(transaction)).containsExactly(row("name", "a"), row("name", "b"));
    assertThat(plan.getOperations())
        .containsExactly(
            Scan.newBuilder()
                .namespace(NS)
                .table("t")
                .partitionKey(Key.ofInt("id", 1))
                .projections("id", "price", "name")
                .where(ConditionBuilder.column("price").isGreaterThanDouble(1.0))
                .build());
    plan.rebind(Arrays.asList(1L, 0.1));
    assertThat(plan.execute(transaction)).containsExactly(row("name", "a"), row("name", "b"));
    assertThat(plan.getOperations())
        .first()
        .isEqualTo(
            Scan.newBuilder()
                .namespace(NS)
                .table("t")
                .partitionKey(Key.ofInt("id", 1))
                .projections("id", "price", "name")
                .where(ConditionBuilder.column("price").isGreaterThanDouble(0.1))
                .build());

    // DML binds its values when the mutations are built
    QueryParser.Plan insert =
        parser.parse(
            "INSERT INTO t (id, seq, name) VALUES ($1, $2, $3)", Arrays.asList(1L, 1L, "x"));
    assertThat(insert.isCacheable()).isTrue();
    insert.rebind(Arrays.asList(5L, 6L, "y"));
    assertThat(insert.getOperations())
        .containsExactly(
            Insert.newBuilder()
                .namespace(NS)
                .table("t")
                .partitionKey(Key.ofInt("id", 5))
                .clusteringKey(Key.ofBigInt("seq", 6))
                .textValue("name", "y")
                .build());
    assertThat(
            parser
                .parse(
                    "UPDATE t SET price = $1 WHERE id = $2 AND seq = $3",
                    Arrays.asList(2.0, 1L, 1L))
                .isCacheable())
        .isTrue();

    // values that become part of the plan keep it from being reused
    assertThat(
            parser
                .parse("SELECT name FROM t WHERE id = 1 LIMIT $1", Collections.singletonList(2L))
                .isCacheable())
        .isFalse();
    assertThat(
            parser
                .parse("SELECT name FROM t WHERE id IN ($1, $2)", Arrays.asList(1L, 2L))
                .isCacheable())
        .isFalse();
    assertThat(parser.parse("SELECT name FROM t WHERE id = 1").isCacheable()).isTrue();
    assertThat(parser.parse("EXPLAIN SELECT name FROM t WHERE id = 1").isCacheable()).isFalse();
  }

  @Test
  public void execute_LookupJoinIntoOnePartition_ShouldBatchTheLookupsIntoScans() throws Exception {
    // 40 outer rows looked up by full key in one partition: the first batch of 16 and the next
    // 24 each become one scan with an OR of the keys, matched back to the outer rows by key
    List<Result> rows = new ArrayList<>();
    for (int i = 1; i <= 40; i++) {
      rows.add(row(1, i, "n" + i, 1.0 * i, true));
    }
    DistributedTransactionManager manager = mock(DistributedTransactionManager.class);
    TestScanners.stub(manager);
    when(manager.scan(any())).thenReturn(rows); // the stub ignores conditions: every row comes back

    QueryParser.Plan plan =
        parser.parse(
            "SELECT a.seq, b.name FROM t a JOIN t b ON b.id = a.id AND b.seq = a.seq"
                + " WHERE a.id = 1");
    List<Map<String, Object>> out = plan.execute(manager);

    assertThat(out).hasSize(40);
    for (int i = 0; i < 40; i++) {
      assertThat(out.get(i))
          .containsEntry("seq", (long) (i + 1))
          .containsEntry("name", "n" + (i + 1));
    }
    verify(manager, never()).get(any());
    org.mockito.ArgumentCaptor<Scan> scans = org.mockito.ArgumentCaptor.forClass(Scan.class);
    verify(manager, times(3)).scan(scans.capture()); // the outer scan, then two lookup scans
    assertThat(scans.getAllValues().get(1).getConjunctions()).hasSize(16);
    assertThat(scans.getAllValues().get(2).getConjunctions()).hasSize(24);
    assertThat(scans.getAllValues().get(2).getConjunctions())
        .contains(Selection.Conjunction.of(ConditionBuilder.column("seq").isEqualToBigInt(40)));

    // the same inside a transaction, where the scan goes through the transaction
    stubTable(rows);
    assertThat(plan.execute(transaction)).hasSize(40);
    verify(transaction, never()).get(any());
  }

  @Test
  public void execute_LookupJoinAcrossPartitionsOnTheManager_ShouldIssueLookupsConcurrently()
      throws Exception {
    // lookups into 40 different partitions cannot share a scan: on the manager they run 16 at a
    // time, and the output keeps the outer order
    List<Result> rows = new ArrayList<>();
    List<Result> looked = new ArrayList<>(); // row (i, i) is what outer row (1, i) looks up
    for (int i = 1; i <= 40; i++) {
      rows.add(row(1, i, "n" + i, 1.0 * i, true));
      looked.add(row(i, i, "n" + i, 1.0 * i, true));
    }
    DistributedTransactionManager manager = mock(DistributedTransactionManager.class);
    TestScanners.stub(manager);
    when(manager.scan(any())).thenReturn(rows);
    java.util.concurrent.atomic.AtomicInteger inFlight =
        new java.util.concurrent.atomic.AtomicInteger();
    java.util.concurrent.atomic.AtomicInteger maxInFlight =
        new java.util.concurrent.atomic.AtomicInteger();
    when(manager.get(any()))
        .thenAnswer(
            invocation -> {
              Get get = invocation.getArgument(0);
              long seq = get.getClusteringKey().get().getBigIntValue(0);
              maxInFlight.accumulateAndGet(inFlight.incrementAndGet(), Math::max);
              Thread.sleep(2); // a round trip
              inFlight.decrementAndGet();
              return java.util.Optional.of(looked.get((int) seq - 1));
            });

    QueryParser.Plan plan =
        parser.parse(
            "SELECT a.seq, b.name FROM t a JOIN t b ON b.id = a.seq AND b.seq = a.seq"
                + " WHERE a.id = 1");
    List<Map<String, Object>> out = plan.execute(manager);

    assertThat(out).hasSize(40);
    for (int i = 0; i < 40; i++) {
      assertThat(out.get(i))
          .containsEntry("seq", (long) (i + 1))
          .containsEntry("name", "n" + (i + 1));
    }
    verify(manager, times(40)).get(any());
    assertThat(maxInFlight.get()).as("lookups in flight at once").isGreaterThan(1);

    // inside a transaction the lookups stay sequential (a transaction is not thread-safe)
    List<Result> all = new ArrayList<>(rows);
    all.addAll(looked.subList(1, looked.size()));
    stubTable(all);
    assertThat(plan.execute(transaction)).hasSize(40);
    verify(transaction, times(40)).get(any());
  }

  @Test
  public void parse_KeywordColumns_ShouldEvaluateToSessionValues() throws Exception {
    // pgjdbc's metadata queries start with SELECT current_catalog
    assertThat(parser.parse("SELECT current_catalog AS c, current_user AS u").execute(transaction))
        .containsExactly(row("c", NS, "u", Catalog.OWNER));
  }

  @Test
  public void parse_UnquotedIdentifiers_ShouldFoldToLowerCaseLikePostgres() throws Exception {
    stubTable(Collections.singletonList(row(1, 1, "a", 1.5, true)));
    // BenchBase writes SQL in upper case against lower-case tables
    assertThat(
            parser
                .parse("SELECT NAME, Price AS P FROM T WHERE ID = 1 AND SEQ = 1")
                .execute(transaction))
        .containsExactly(row("name", "a", "p", 1.5));
  }

  @Test
  public void localDateTime_ShouldAcceptADateAlone() {
    assertThat(QueryParser.localDateTime("2026-09-01"))
        .isEqualTo(java.time.LocalDateTime.of(2026, 9, 1, 0, 0));
    assertThat(QueryParser.instant("2026-09-01"))
        .isEqualTo(
            java.time.LocalDateTime.of(2026, 9, 1, 0, 0).toInstant(java.time.ZoneOffset.UTC));
  }

  @Test
  public void parse_Between_ShouldPushBothBoundsDown() throws Exception {
    QueryParser.Plan plan =
        parser.parse("SELECT name FROM t WHERE id = 1 AND price BETWEEN 1 AND 2");
    String explain = String.join("\n", parser.explain(plan));
    assertThat(explain).doesNotContain("Filter");
    assertThat(explain).contains("price >= 1.0", "price <= 2.0");
  }

  @Test
  public void localDateTime_ShouldAcceptTheOffsetsDriversSend() {
    java.time.LocalDateTime t = java.time.LocalDateTime.of(2026, 10, 2, 10, 4, 34, 785_000_000);
    assertThat(QueryParser.localDateTime("2026-10-02 10:04:34.785+09")).isEqualTo(t);
    assertThat(QueryParser.localDateTime("2026-10-02 10:04:34.785+09:00")).isEqualTo(t);
    assertThat(QueryParser.localDateTime("2026-10-02T10:04:34.785")).isEqualTo(t);
    assertThat(QueryParser.instant("2026-10-02 10:04:34+09"))
        .isEqualTo(java.time.Instant.parse("2026-10-02T01:04:34Z"));
  }

  @Test
  public void execute_RankingWindowFunctions_ShouldComputeOverPartitions() throws Exception {
    QueryParser.Plan plan =
        parser.parse(
            "SELECT id, seq, row_number() OVER (PARTITION BY id ORDER BY price DESC) AS rn, "
                + "rank() OVER (ORDER BY price) AS r, dense_rank() OVER (ORDER BY price) AS dr, "
                + "ntile(2) OVER (ORDER BY seq) AS nt FROM t ORDER BY id, seq");
    when(transaction.scan(any()))
        .thenReturn(
            Arrays.asList(
                row(1, 1, "a", 1.0, true),
                row(1, 2, "b", 3.0, true),
                row(1, 3, "c", 3.0, true),
                row(2, 1, "d", 2.0, true)));

    assertThat(plan.execute(transaction))
        .containsExactly(
            map("id", 1, "seq", 1L, "rn", 3L, "r", 1L, "dr", 1L, "nt", 1L),
            map("id", 1, "seq", 2L, "rn", 1L, "r", 3L, "dr", 3L, "nt", 2L),
            map("id", 1, "seq", 3L, "rn", 2L, "r", 3L, "dr", 3L, "nt", 2L),
            map("id", 2, "seq", 1L, "rn", 1L, "r", 2L, "dr", 2L, "nt", 1L));
    assertThat(String.join("\n", parser.explain(plan))).contains("Window [row_number()");
  }

  @Test
  public void execute_ValueAndAggregateWindowFunctions_ShouldUseFrames() throws Exception {
    QueryParser.Plan plan =
        parser.parse(
            "SELECT seq, lag(name) OVER w AS prev, lead(name, 2, '-') OVER w AS next2, "
                + "first_value(name) OVER w AS f, last_value(name) OVER w AS l, "
                + "sum(price) OVER w AS running, count(*) OVER () AS n, "
                + "avg(price) OVER (ORDER BY seq ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS mv "
                + "FROM t WHERE id = 1 WINDOW w AS (ORDER BY seq)");
    when(transaction.scan(any()))
        .thenReturn(
            Arrays.asList(
                row(1, 1, "a", 1.0, true),
                row(1, 2, "b", 2.0, true),
                row(1, 3, "c", 3.0, true),
                row(1, 4, "d", 4.0, true)));

    assertThat(plan.execute(transaction))
        .containsExactly(
            map(
                "seq", 1L, "prev", null, "next2", "c", "f", "a", "l", "a", "running", 1.0, "n", 4L,
                "mv", 1.5),
            map(
                "seq", 2L, "prev", "a", "next2", "d", "f", "a", "l", "b", "running", 3.0, "n", 4L,
                "mv", 2.0),
            map(
                "seq", 3L, "prev", "b", "next2", "-", "f", "a", "l", "c", "running", 6.0, "n", 4L,
                "mv", 3.0),
            map(
                "seq", 4L, "prev", "c", "next2", "-", "f", "a", "l", "d", "running", 10.0, "n", 4L,
                "mv", 3.5));
  }

  @Test
  public void execute_WindowOverGroups_ShouldRankTheGroups() throws Exception {
    QueryParser.Plan plan =
        parser.parse(
            "SELECT active, count(*) AS n, rank() OVER (ORDER BY count(*) DESC) AS r, "
                + "sum(count(*)) OVER () AS total FROM t GROUP BY active ORDER BY r");
    when(transaction.scan(any()))
        .thenReturn(
            Arrays.asList(
                row(1, 1, "a", 1.0, true),
                row(1, 2, "b", 2.0, false),
                row(1, 3, "c", 3.0, true),
                row(2, 1, "d", 4.0, true)));

    assertThat(plan.execute(transaction))
        .containsExactly(
            map("active", true, "n", 3L, "r", 1L, "total", 4L),
            map("active", false, "n", 1L, "r", 2L, "total", 4L));
  }

  @Test
  public void execute_WindowFunctionInWhere_ShouldFail() throws Exception {
    QueryParser.Plan plan = parser.parse("SELECT id FROM t WHERE row_number() OVER () = 1");
    when(transaction.scan(any())).thenReturn(Collections.singletonList(row(1, 1, "a", 1.0, true)));

    assertThatThrownBy(() -> plan.execute(transaction))
        .hasMessageContaining("Window functions are allowed only");
  }

  @Test
  public void execute_RecursiveCteWithoutTable_ShouldIterateUntilTheStepIsEmpty() throws Exception {
    QueryParser.Plan plan =
        parser.parse(
            "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 4) "
                + "SELECT i, i * 10 AS t FROM n");

    assertThat(plan.execute(transaction))
        .containsExactly(
            map("i", 1L, "t", 10L),
            map("i", 2L, "t", 20L),
            map("i", 3L, "t", 30L),
            map("i", 4L, "t", 40L));
    assertThat(String.join("\n", parser.explain(plan)))
        .contains("Recursive Union ALL [i]")
        .contains("Working table n");
  }

  @Test
  public void execute_RecursiveCteOverTable_ShouldWalkTheChainAndDropRepeats() throws Exception {
    // price points at the parent id: 3 -> 2 -> 1 -> 3 is a cycle that UNION stops
    QueryParser.Plan plan =
        parser.parse(
            "WITH RECURSIVE r AS (SELECT id, price FROM t WHERE id = 3 "
                + "UNION SELECT t.id, t.price FROM r JOIN t ON t.id = CAST(r.price AS INT)) "
                + "SELECT id FROM r");
    stubTable(
        Arrays.asList(
            row(1, 1, "a", 3.0, true), row(2, 1, "b", 1.0, true), row(3, 1, "c", 2.0, true)));

    assertThat(plan.execute(transaction)).containsExactly(map("id", 3), map("id", 2), map("id", 1));
  }

  @Test
  public void execute_WithRecursiveOnAPlainUnion_ShouldNotIterate() throws Exception {
    QueryParser.Plan plan =
        parser.parse("WITH RECURSIVE u AS (SELECT 1 AS x UNION ALL SELECT 2) SELECT x FROM u");

    assertThat(plan.execute(transaction)).containsExactly(map("x", 1L), map("x", 2L));
  }

  @Test
  public void execute_DecimalLiterals_ShouldFollowPostgresNumericRules() throws Exception {
    QueryParser.Plan plan =
        parser.parse(
            "SELECT 2500 * 1.5 AS a, 7.0 / 2 AS b, 7 / 2 AS c, 1.5 + 2.25 AS d, "
                + "CAST(2.345 AS NUMERIC(10, 2)) AS e, round(2.5) AS f, -1.5 AS g, "
                + "1e3 + 0.5 AS h, 10 % 3.5 AS i, 1.5 * 2.0 AS j");

    assertThat(plan.execute(transaction))
        .containsExactly(
            map(
                "a", new java.math.BigDecimal("3750.0"),
                "b", new java.math.BigDecimal("3.5000000000000000"),
                "c", 3,
                "d", new java.math.BigDecimal("3.75"),
                "e", new java.math.BigDecimal("2.35"),
                "f", new java.math.BigDecimal("3"),
                "g", new java.math.BigDecimal("-1.5"),
                "h", new java.math.BigDecimal("1000.5"),
                "i", new java.math.BigDecimal("3.0"),
                "j", new java.math.BigDecimal("3.00")));
  }

  @Test
  public void execute_AveragesAndSums_ShouldBeNumericUnlessADoubleIsInvolved() throws Exception {
    QueryParser.Plan plan =
        parser.parse(
            "SELECT avg(seq) AS a, sum(seq) AS b, avg(price) AS c, sum(price) AS d, "
                + "price * 1.5 AS e FROM t GROUP BY price * 1.5");
    when(transaction.scan(any()))
        .thenReturn(Arrays.asList(row(1, 1, "a", 2.0, true), row(1, 2, "b", 2.0, true)));

    assertThat(plan.execute(transaction))
        .containsExactly(
            map(
                "a", new java.math.BigDecimal("1.5000000000000000"),
                "b", 3L,
                "c", 2.0,
                "d", 4.0,
                "e", 3.0));
  }

  @Test
  public void execute_DistinctOn_ShouldKeepTheFirstRowPerKeyInOrderByOrder() throws Exception {
    QueryParser.Plan plan =
        parser.parse("SELECT DISTINCT ON (id) id, seq FROM t ORDER BY id, seq DESC");
    when(transaction.scan(any()))
        .thenReturn(
            Arrays.asList(
                row(1, 1, "a", 1.0, true), row(1, 2, "b", 2.0, true), row(2, 1, "c", 3.0, true)));

    assertThat(plan.execute(transaction))
        .containsExactly(map("id", 1, "seq", 2L), map("id", 2, "seq", 1L));
    assertThatThrownBy(() -> parser.parse("SELECT DISTINCT ON (id) id, seq FROM t ORDER BY seq"))
        .hasMessageContaining("must match initial ORDER BY");
  }

  @Test
  public void execute_Rollup_ShouldGroupByEveryPrefix() throws Exception {
    QueryParser.Plan plan =
        parser.parse("SELECT id, active, count(*) AS n FROM t GROUP BY ROLLUP (id, active)");
    when(transaction.scan(any()))
        .thenReturn(
            Arrays.asList(
                row(1, 1, "a", 1.0, true), row(1, 2, "b", 2.0, true), row(2, 1, "c", 3.0, false)));

    assertThat(plan.execute(transaction))
        .containsExactlyInAnyOrder(
            map("id", 1, "active", true, "n", 2L),
            map("id", 2, "active", false, "n", 1L),
            map("id", 1, "active", null, "n", 2L),
            map("id", 2, "active", null, "n", 1L),
            map("id", null, "active", null, "n", 3L));
  }

  @Test
  public void execute_FetchFirst_ShouldLimitLikeLimit() throws Exception {
    when(transaction.scan(any()))
        .thenReturn(
            Arrays.asList(
                row(1, 1, "a", 1.0, true), row(1, 2, "b", 2.0, true), row(1, 3, "c", 3.0, true)));

    // ORDER BY a non-key column keeps the limit in memory, where the stubbed scan cannot ignore it
    assertThat(
            parser
                .parse("SELECT seq FROM t WHERE id = 1 ORDER BY name FETCH FIRST 2 ROWS ONLY")
                .execute(transaction))
        .containsExactly(map("seq", 1L), map("seq", 2L));
    assertThat(
            parser
                .parse(
                    "SELECT seq FROM t WHERE id = 1 ORDER BY name OFFSET 2 ROWS FETCH FIRST ROW ONLY")
                .execute(transaction))
        .containsExactly(map("seq", 3L));
  }

  @Test
  public void execute_EscapeString_ShouldProcessBackslashes() throws Exception {
    QueryParser.Plan plan =
        parser.parse(
            "SELECT E'a\\nb' AS s, length(E'\\x41\\101\\u0042') AS n, E'it\\'s' AS q, "
                + "'a\\nb' AS plain");

    assertThat(plan.execute(transaction))
        .containsExactly(map("s", "a\nb", "n", 3L, "q", "it's", "plain", "a\\nb"));
  }

  @Test
  public void execute_GenerateSeries_ShouldProduceRowsAndOtherTableFunctionsFail()
      throws Exception {
    assertThat(
            parser
                .parse("SELECT n * 2 AS d FROM generate_series(1, 5, 2) AS g(n)")
                .execute(transaction))
        .containsExactly(map("d", 2L), map("d", 6L), map("d", 10L));
    assertThat(parser.parse("SELECT * FROM generate_series(3, 1)").execute(transaction)).isEmpty();
    assertThatThrownBy(() -> parser.parse("SELECT * FROM json_each('{}')"))
        .hasMessageContaining("Unsupported");
    assertThatThrownBy(() -> parser.parse("CREATE TABLE x AS SELECT 1"))
        .hasMessageContaining("Unsupported CREATE TABLE form");
  }

  @Test
  public void execute_RegclassCastAndPgIndexes_ShouldAnswerFromTheCatalog() throws Exception {
    assertThat(
            parser
                .parse(
                    "SELECT c.relname FROM pg_catalog.pg_class c "
                        + "WHERE c.oid = 'public.t'::regclass AND c.relnamespace = 'ns'::regnamespace")
                .execute(transaction))
        .containsExactly(map("relname", "t"));
    assertThat(parser.parse("SELECT 'int4'::regtype = 23 AS same").execute(transaction))
        .containsExactly(map("same", true));
    assertThat(
            parser
                .parse("SELECT indexname FROM pg_indexes WHERE tablename = 't' ORDER BY indexname")
                .execute(transaction))
        .containsExactly(map("indexname", "t_name_idx"), map("indexname", "t_pkey"));
    assertThatThrownBy(() -> parser.parse("SELECT 'nosuch'::regclass").execute(transaction))
        .hasMessageContaining("does not exist");
  }

  @Test
  public void execute_Intervals_ShouldFollowPostgresArithmeticAndText() throws Exception {
    QueryParser.Plan plan =
        parser.parse(
            "SELECT interval '1 year 2 months 3 days 04:05:06.5' AS a, interval '90 minutes' AS b, "
                + "interval '1.5 days' AS c, interval '1 day' * 2.5 AS d, "
                + "-interval '1 day 01:00:00' AS e, "
                + "timestamp '2024-01-31 10:00:00' + interval '1 month' AS f, "
                + "date '2024-02-28' + interval '1 day 2 hours' AS g, "
                + "timestamp '2024-03-01 00:00:00' - timestamp '2024-01-15 12:30:00' AS h, "
                + "age(timestamp '2024-03-01 00:00:00', timestamp '2023-01-15 12:30:00') AS i, "
                + "extract(epoch from interval '1 day 00:00:01') AS j, "
                + "extract(hour from interval '1 day 05:30:00') AS k, "
                + "interval '1 day' = interval '24 hours' AS l, '36 hours'::interval AS m, "
                + "timestamp '2024-01-01 00:00:00' AT TIME ZONE 'Asia/Tokyo' AS n, "
                + "timestamptz '2024-01-01 00:00:00+00' AT TIME ZONE 'Asia/Tokyo' AS o, "
                + "interval '1 day' / 3 AS p, interval '1' day AS q");

    Map<String, Object> row = plan.execute(transaction).get(0);
    assertThat(Evaluator.text(row.get("a"))).isEqualTo("1 year 2 mons 3 days 04:05:06.5");
    assertThat(Evaluator.text(row.get("b"))).isEqualTo("01:30:00");
    assertThat(Evaluator.text(row.get("c"))).isEqualTo("1 day 12:00:00");
    assertThat(Evaluator.text(row.get("d"))).isEqualTo("2 days 12:00:00");
    assertThat(Evaluator.text(row.get("e"))).isEqualTo("-1 days -01:00:00");
    assertThat(Evaluator.text(row.get("f"))).isEqualTo("2024-02-29 10:00:00");
    assertThat(Evaluator.text(row.get("g"))).isEqualTo("2024-02-29 02:00:00");
    assertThat(Evaluator.text(row.get("h"))).isEqualTo("45 days 11:30:00");
    assertThat(Evaluator.text(row.get("i"))).isEqualTo("1 year 1 mon 16 days 11:30:00");
    assertThat(row.get("j")).isEqualTo(new java.math.BigDecimal("86401.000000"));
    assertThat(row.get("k")).isEqualTo(5L);
    assertThat(row.get("l")).isEqualTo(true);
    assertThat(Evaluator.text(row.get("m"))).isEqualTo("36:00:00");
    assertThat(Evaluator.text(row.get("n"))).isEqualTo("2023-12-31 15:00:00+00");
    assertThat(Evaluator.text(row.get("o"))).isEqualTo("2024-01-01 09:00:00");
    assertThat(Evaluator.text(row.get("p"))).isEqualTo("08:00:00");
    assertThat(Evaluator.text(row.get("q"))).isEqualTo("1 day");
  }

  @Test
  public void execute_ConstantExpressionCondition_ShouldBePushedDownWhenOpened() throws Exception {
    QueryParser.Plan plan = parser.parse("SELECT seq FROM t WHERE price > 10 - 8.5");
    List<Scan> scans = new ArrayList<>();
    when(transaction.scan(any()))
        .thenAnswer(
            invocation -> {
              scans.add(invocation.getArgument(0));
              return Collections.singletonList(row(1, 3, "c", 3.0, true));
            });

    assertThat(plan.execute(transaction)).containsExactly(map("seq", 3L));
    assertThat(scans).hasSize(1);
    assertThat(scans.get(0).getConjunctions())
        .anySatisfy(
            conjunction ->
                assertThat(conjunction.getConditions())
                    .anySatisfy(
                        c -> {
                          assertThat(c.getColumn().getName()).isEqualTo("price");
                          assertThat(c.getColumn().getValueAsObject()).isEqualTo(1.5);
                          assertThat(c.getOperator())
                              .isEqualTo(com.scalar.db.api.ConditionalExpression.Operator.GT);
                        }));
  }

  @Test
  public void execute_AnyOverArrays_ShouldBecomeMembershipAndReadByKey() throws Exception {
    stubTable(
        Arrays.asList(
            row(1, 1, "a", 1.0, true), row(2, 1, "b", 2.0, true), row(3, 1, "c", 3.0, true)));

    QueryParser.Plan plan =
        parser.parse("SELECT id FROM t WHERE id = ANY(ARRAY[1, 3]) ORDER BY id");
    assertThat(plan.execute(transaction)).containsExactly(map("id", 1), map("id", 3));
    assertThat(String.join("\n", parser.explain(plan))).contains("Lookup");
    // a bound parameter in PostgreSQL's array text, as drivers send it
    assertThat(
            parser
                .parse(
                    "SELECT id FROM t WHERE id = ANY($1) ORDER BY id",
                    Collections.singletonList("{2,3}"))
                .execute(transaction))
        .containsExactly(map("id", 2), map("id", 3));
    assertThat(
            parser
                .parse("SELECT id FROM t WHERE name = ANY('{a,\"c\"}') ORDER BY id")
                .execute(transaction))
        .containsExactly(map("id", 1), map("id", 3));
    assertThat(parser.parse("SELECT id FROM t WHERE id <> ALL(ARRAY[1, 2])").execute(transaction))
        .containsExactly(map("id", 3));
    assertThat(
            parser
                .parse("SELECT id FROM t WHERE id > ANY(ARRAY[2, 5]) OR id < ALL('{0,1}'::int[])")
                .execute(transaction))
        .containsExactly(map("id", 3));
    assertThat(parser.parse("SELECT id FROM t WHERE id = ANY(ARRAY[]::int[])").execute(transaction))
        .isEmpty();
  }

  @Test
  public void execute_ArrayValues_ShouldPrintAndUnnestLikePostgres() throws Exception {
    assertThat(
            parser
                .parse(
                    "SELECT ARRAY[1, 2] AS a, ARRAY['x', 'y z', NULL] AS b, '{1,2}'::int[] AS c, "
                        + "cardinality(ARRAY[1, 2, 3]) AS n, array_length('{}'::int[], 1) AS e")
                .execute(transaction))
        .containsExactly(
            map(
                "a", Arrays.asList(1L, 2L),
                "b", Arrays.asList("x", "y z", null),
                "c", Arrays.asList(1, 2), // int[] elements are int4
                "n", 3L,
                "e", null));
    assertThat(Evaluator.text(Arrays.asList("x", "y z", null, "a\"b")))
        .isEqualTo("{x,\"y z\",NULL,\"a\\\"b\"}");
    assertThat(
            parser
                .parse("SELECT n * 10 AS d FROM unnest(ARRAY[3, 4]) AS u(n)")
                .execute(transaction))
        .containsExactly(map("d", 30L), map("d", 40L));
    assertThat(
            parser
                .parse("SELECT * FROM unnest($1)", Collections.singletonList("{p,q}"))
                .execute(transaction))
        .containsExactly(map("unnest", "p"), map("unnest", "q"));
  }

  @Test
  public void parse_OutputOids_ShouldBeInferredForComputedAndReturningColumns() throws Exception {
    QueryParser.Plan plan =
        parser.parse(
            "SELECT count(*) AS c, avg(price) AS a, sum(id) AS s, id * 2 AS d, name || 'x' AS t, "
                + "CAST(price AS int) AS i, price > 1 AS b, now() AS n, array_agg(name) AS arr, "
                + "max(name) AS m FROM t GROUP BY id, name, price");
    assertThat(plan.outputOids()).containsExactly(20, 701, 20, 23, 25, 23, 16, 1184, 1009, 25);
    assertThat(
            parser
                .parse("INSERT INTO t (id, seq, name) VALUES (1, 1, 'a') RETURNING id, name")
                .outputOids())
        .containsExactly(23, 25);
    assertThat(parser.parse("SELECT 1 AS x UNION ALL SELECT 2").outputOids()).containsExactly(23);
  }

  @Test
  public void execute_ArrayAggAndCatalogHelpers_ShouldServeDrivers() throws Exception {
    when(transaction.scan(any()))
        .thenReturn(
            Arrays.asList(
                row(1, 1, "a", 1.0, true), row(1, 2, "b", 2.0, true), row(2, 1, "c", 3.0, true)));
    assertThat(
            parser
                .parse(
                    "SELECT id, array_agg(name ORDER BY seq DESC) AS names FROM t GROUP BY id ORDER BY id")
                .execute(transaction))
        .containsExactly(
            map("id", 1, "names", Arrays.asList("b", "a")),
            map("id", 2, "names", Collections.singletonList("c")));
    // reserved words as aliases, as Sequelize writes them, and the ORM helper functions
    assertThat(
            parser
                .parse(
                    "SELECT true AS primary, false AS unique, to_regtype('int4') AS t, "
                        + "to_regtype('hstore') AS none, to_regclass('t') IS NOT NULL AS exists, "
                        + "current_schemas(false) AS schemas")
                .execute(transaction))
        .containsExactly(
            map(
                "primary",
                true,
                "unique",
                false,
                "t",
                23L,
                "none",
                null,
                "exists",
                true,
                "schemas",
                Collections.singletonList("public")));
    assertThat(QueryParser.instant("2024-01-01 00:00:00Z"))
        .isEqualTo(java.time.Instant.parse("2024-01-01T00:00:00Z"));
  }
}
