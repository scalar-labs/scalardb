package com.scalar.db.storage.jdbc.query;

import static com.scalar.db.storage.jdbc.query.QueryUtils.enclose;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.ConditionalExpression.Operator;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.Value;
import com.scalar.db.storage.jdbc.JdbcTableMetadataManager;
import com.scalar.db.storage.jdbc.RdbEngine;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(Parameterized.class)
public class QueryBuilderTest {

  private static final String NAMESPACE = "n1";
  private static final String TABLE = "t1";
  @Parameterized.Parameter public RdbEngine rdbEngine;
  @Mock private JdbcTableMetadataManager tableMetadataManager;
  private QueryBuilder queryBuilder;

  @Parameterized.Parameters(name = "RDB={0}")
  public static Collection<RdbEngine> jdbcConnectionInfos() {
    return Arrays.asList(
        RdbEngine.MYSQL, RdbEngine.POSTGRESQL, RdbEngine.ORACLE, RdbEngine.SQL_SERVER);
  }

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);

    // Dummy metadata
    when(tableMetadataManager.getTableMetadata(any(String.class)))
        .thenReturn(
            TableMetadata.newBuilder()
                .addColumn("p1", DataType.TEXT)
                .addColumn("p2", DataType.INT)
                .addColumn("c1", DataType.TEXT)
                .addColumn("c2", DataType.TEXT)
                .addColumn("v1", DataType.TEXT)
                .addColumn("v2", DataType.TEXT)
                .addColumn("v3", DataType.TEXT)
                .addColumn("v4", DataType.TEXT)
                .addPartitionKey("p1")
                .addPartitionKey("p2")
                .addClusteringKey("c1", Scan.Ordering.Order.ASC)
                .addClusteringKey("c2", Scan.Ordering.Order.DESC)
                .addSecondaryIndex("v1")
                .addSecondaryIndex("v2")
                .build());

    queryBuilder = new QueryBuilder(tableMetadataManager, rdbEngine);
  }

  private String encloseSql(String sql) {
    return sql.replace("n1.t1", enclose("n1", rdbEngine) + "." + enclose("t1", rdbEngine))
        .replace("p1", enclose("p1", rdbEngine))
        .replace("p2", enclose("p2", rdbEngine))
        .replace("c1", enclose("c1", rdbEngine))
        .replace("c2", enclose("c2", rdbEngine))
        .replace("v1", enclose("v1", rdbEngine))
        .replace("v2", enclose("v2", rdbEngine))
        .replace("v3", enclose("v3", rdbEngine))
        .replace("v4", enclose("v4", rdbEngine));
  }

  @Test
  public void selectQueryTest() {
    assertThat(
            queryBuilder
                .select(Arrays.asList("c1", "c2"))
                .from(NAMESPACE, TABLE)
                .where(new Key(new TextValue("p1", "aaa")), Optional.empty())
                .build()
                .toString())
        .isEqualTo(encloseSql("SELECT c1,c2 FROM n1.t1 WHERE p1=?"));

    assertThat(
            queryBuilder
                .select(Collections.emptyList())
                .from(NAMESPACE, TABLE)
                .where(
                    new Key(new TextValue("p1", "aaa"), new TextValue("p2", "bbb")),
                    Optional.empty())
                .build()
                .toString())
        .isEqualTo(encloseSql("SELECT * FROM n1.t1 WHERE p1=? AND p2=?"));

    assertThat(
            queryBuilder
                .select(Arrays.asList("c1", "c2"))
                .from(NAMESPACE, TABLE)
                .where(
                    new Key(new TextValue("p1", "aaa")),
                    Optional.of(new Key(new TextValue("c1", "aaa"))))
                .build()
                .toString())
        .isEqualTo(encloseSql("SELECT c1,c2 FROM n1.t1 WHERE p1=? AND c1=?"));

    assertThat(
            queryBuilder
                .select(Arrays.asList("c1", "c2"))
                .from(NAMESPACE, TABLE)
                .where(
                    new Key(new TextValue("p1", "aaa")),
                    Optional.of(new Key(new TextValue("c1", "aaa"), new TextValue("c2", "bbb"))))
                .build()
                .toString())
        .isEqualTo(encloseSql("SELECT c1,c2 FROM n1.t1 WHERE p1=? AND c1=? AND c2=?"));

    assertThat(
            queryBuilder
                .select(Arrays.asList("c1", "c2"))
                .from(NAMESPACE, TABLE)
                .where(
                    new Key(new TextValue("p1", "aaa")),
                    Optional.of(new Key(new TextValue("c1", "aaa"))),
                    true,
                    Optional.of(new Key(new TextValue("c1", "bbb"))),
                    true)
                .build()
                .toString())
        .isEqualTo(
            encloseSql(
                "SELECT c1,c2 FROM n1.t1 WHERE p1=? AND c1>=? AND c1<=? ORDER BY c1 ASC,c2 DESC"));

    assertThat(
            queryBuilder
                .select(Collections.emptyList())
                .from(NAMESPACE, TABLE)
                .where(
                    new Key(new TextValue("p1", "aaa")),
                    Optional.of(new Key(new TextValue("c1", "aaa"))),
                    false,
                    Optional.of(new Key(new TextValue("c1", "bbb"))),
                    false)
                .build()
                .toString())
        .isEqualTo(
            encloseSql("SELECT * FROM n1.t1 WHERE p1=? AND c1>? AND c1<? ORDER BY c1 ASC,c2 DESC"));

    assertThat(
            queryBuilder
                .select(Arrays.asList("c1", "c2"))
                .from(NAMESPACE, TABLE)
                .where(
                    new Key(new TextValue("p1", "aaa")),
                    Optional.of(new Key(new TextValue("c1", "aaa"), new TextValue("c2", "aaa"))),
                    true,
                    Optional.of(new Key(new TextValue("c1", "aaa"), new TextValue("c2", "bbb"))),
                    false)
                .build()
                .toString())
        .isEqualTo(
            encloseSql(
                "SELECT c1,c2 FROM n1.t1 WHERE p1=? AND c1=? AND c2>=? AND c2<? "
                    + "ORDER BY c1 ASC,c2 DESC"));

    assertThat(
            queryBuilder
                .select(Arrays.asList("c1", "c2"))
                .from(NAMESPACE, TABLE)
                .where(
                    new Key(new TextValue("p1", "aaa")),
                    Optional.of(new Key(new TextValue("c1", "aaa"))),
                    true,
                    Optional.of(new Key(new TextValue("c1", "bbb"))),
                    true)
                .orderBy(
                    Collections.singletonList(new Scan.Ordering("c1", Scan.Ordering.Order.ASC)))
                .build()
                .toString())
        .isEqualTo(
            encloseSql(
                "SELECT c1,c2 FROM n1.t1 WHERE p1=? AND c1>=? AND c1<=? ORDER BY c1 ASC,c2 DESC"));

    assertThat(
            queryBuilder
                .select(Arrays.asList("c1", "c2"))
                .from(NAMESPACE, TABLE)
                .where(
                    new Key(new TextValue("p1", "aaa")),
                    Optional.of(new Key(new TextValue("c1", "aaa"))),
                    true,
                    Optional.of(new Key(new TextValue("c1", "bbb"))),
                    true)
                .orderBy(
                    Arrays.asList(
                        new Scan.Ordering("c1", Scan.Ordering.Order.ASC),
                        new Scan.Ordering("c2", Scan.Ordering.Order.DESC)))
                .build()
                .toString())
        .isEqualTo(
            encloseSql(
                "SELECT c1,c2 FROM n1.t1 WHERE p1=? AND c1>=? AND c1<=? ORDER BY c1 ASC,c2 DESC"));

    assertThat(
            queryBuilder
                .select(Arrays.asList("c1", "c2"))
                .from(NAMESPACE, TABLE)
                .where(
                    new Key(new TextValue("p1", "aaa")),
                    Optional.of(new Key(new TextValue("c1", "aaa"))),
                    true,
                    Optional.of(new Key(new TextValue("c1", "bbb"))),
                    true)
                .orderBy(
                    Collections.singletonList(new Scan.Ordering("c1", Scan.Ordering.Order.DESC)))
                .build()
                .toString())
        .isEqualTo(
            encloseSql(
                "SELECT c1,c2 FROM n1.t1 WHERE p1=? AND c1>=? AND c1<=? ORDER BY c1 DESC,c2 ASC"));

    assertThat(
            queryBuilder
                .select(Arrays.asList("c1", "c2"))
                .from(NAMESPACE, TABLE)
                .where(
                    new Key(new TextValue("p1", "aaa")),
                    Optional.of(new Key(new TextValue("c1", "aaa"))),
                    true,
                    Optional.of(new Key(new TextValue("c1", "bbb"))),
                    true)
                .orderBy(
                    Arrays.asList(
                        new Scan.Ordering("c1", Scan.Ordering.Order.DESC),
                        new Scan.Ordering("c2", Scan.Ordering.Order.ASC)))
                .build()
                .toString())
        .isEqualTo(
            encloseSql(
                "SELECT c1,c2 FROM n1.t1 WHERE p1=? AND c1>=? AND c1<=? ORDER BY c1 DESC,c2 ASC"));

    String expectedQuery;
    switch (rdbEngine) {
      case MYSQL:
      case POSTGRESQL:
        expectedQuery =
            "SELECT c1,c2 FROM n1.t1 WHERE p1=? AND c1>=? AND c1<=? "
                + "ORDER BY c1 ASC,c2 DESC LIMIT 10";
        break;
      case ORACLE:
        expectedQuery =
            "SELECT c1,c2 FROM n1.t1 WHERE p1=? AND c1>=? AND c1<=? "
                + "ORDER BY c1 ASC,c2 DESC FETCH FIRST 10 ROWS ONLY";
        break;
      case SQL_SERVER:
      default:
        expectedQuery =
            "SELECT c1,c2 FROM n1.t1 WHERE p1=? AND c1>=? AND c1<=? "
                + "ORDER BY c1 ASC,c2 DESC OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY";
        break;
    }
    assertThat(
            queryBuilder
                .select(Arrays.asList("c1", "c2"))
                .from(NAMESPACE, TABLE)
                .where(
                    new Key(new TextValue("p1", "aaa")),
                    Optional.of(new Key(new TextValue("c1", "aaa"))),
                    true,
                    Optional.of(new Key(new TextValue("c1", "bbb"))),
                    true)
                .limit(10)
                .build()
                .toString())
        .isEqualTo(encloseSql(expectedQuery));
  }

  @Test
  public void selectQueryWithIndexedColumnTest() {
    assertThat(
            queryBuilder
                .select(Arrays.asList("c1", "c2"))
                .from(NAMESPACE, TABLE)
                .where(new Key(new TextValue("v1", "aaa")), Optional.empty())
                .build()
                .toString())
        .isEqualTo(encloseSql("SELECT c1,c2 FROM n1.t1 WHERE v1=?"));

    assertThat(
            queryBuilder
                .select(Arrays.asList("c1", "c2"))
                .from(NAMESPACE, TABLE)
                .where(
                    new Key(new TextValue("v1", "aaa")),
                    Optional.empty(),
                    false,
                    Optional.empty(),
                    false)
                .build()
                .toString())
        .isEqualTo(encloseSql("SELECT c1,c2 FROM n1.t1 WHERE v1=?"));

    assertThat(
            queryBuilder
                .select(Arrays.asList("c1", "c2"))
                .from(NAMESPACE, TABLE)
                .where(new Key(new TextValue("v2", "aaa")), Optional.empty())
                .build()
                .toString())
        .isEqualTo(encloseSql("SELECT c1,c2 FROM n1.t1 WHERE v2=?"));

    assertThat(
            queryBuilder
                .select(Arrays.asList("c1", "c2"))
                .from(NAMESPACE, TABLE)
                .where(
                    new Key(new TextValue("v2", "aaa")),
                    Optional.empty(),
                    false,
                    Optional.empty(),
                    false)
                .build()
                .toString())
        .isEqualTo(encloseSql("SELECT c1,c2 FROM n1.t1 WHERE v2=?"));
  }

  @Test
  public void insertQueryTest() {
    Map<String, Value<?>> values = new HashMap<>();
    values.put("v1", new TextValue("aaa"));
    values.put("v2", new TextValue("bbb"));
    values.put("v3", new TextValue("ddd"));

    assertThat(
            queryBuilder
                .insertInto(NAMESPACE, TABLE)
                .values(new Key(new TextValue("p1", "aaa")), Optional.empty(), values)
                .build()
                .toString())
        .isEqualTo(encloseSql("INSERT INTO n1.t1 (p1,v1,v2,v3) VALUES (?,?,?,?)"));

    assertThat(
            queryBuilder
                .insertInto(NAMESPACE, TABLE)
                .values(
                    new Key(new TextValue("p1", "aaa")),
                    Optional.of(new Key(new TextValue("c1", "bbb"))),
                    values)
                .build()
                .toString())
        .isEqualTo(encloseSql("INSERT INTO n1.t1 (p1,c1,v1,v2,v3) VALUES (?,?,?,?,?)"));

    values.put("v4", new TextValue("eee"));

    assertThat(
            queryBuilder
                .insertInto(NAMESPACE, TABLE)
                .values(
                    new Key(new TextValue("p1", "aaa"), new TextValue("p2", "ccc")),
                    Optional.of(new Key(new TextValue("c1", "bbb"), new TextValue("c2", "ddd"))),
                    values)
                .build()
                .toString())
        .isEqualTo(
            encloseSql("INSERT INTO n1.t1 (p1,p2,c1,c2,v1,v2,v3,v4) VALUES (?,?,?,?,?,?,?,?)"));
  }

  @Test
  public void updateQueryTest() {
    Map<String, Value<?>> values = new HashMap<>();
    values.put("v1", new TextValue("aaa"));
    values.put("v2", new TextValue("bbb"));
    values.put("v3", new TextValue("ddd"));

    assertThat(
            queryBuilder
                .update(NAMESPACE, TABLE)
                .set(values)
                .where(new Key(new TextValue("p1", "aaa")), Optional.empty())
                .build()
                .toString())
        .isEqualTo(encloseSql("UPDATE n1.t1 SET v1=?,v2=?,v3=? WHERE p1=?"));

    assertThat(
            queryBuilder
                .update(NAMESPACE, TABLE)
                .set(values)
                .where(
                    new Key(new TextValue("p1", "aaa")),
                    Optional.of(new Key(new TextValue("c1", "bbb"))))
                .build()
                .toString())
        .isEqualTo(encloseSql("UPDATE n1.t1 SET v1=?,v2=?,v3=? WHERE p1=? AND c1=?"));

    assertThat(
            queryBuilder
                .update(NAMESPACE, TABLE)
                .set(values)
                .where(
                    new Key(new TextValue("p1", "aaa"), new TextValue("p2", "ccc")),
                    Optional.of(new Key(new TextValue("c1", "bbb"), new TextValue("c2", "ddd"))))
                .build()
                .toString())
        .isEqualTo(
            encloseSql("UPDATE n1.t1 SET v1=?,v2=?,v3=? WHERE p1=? AND p2=? AND c1=? AND c2=?"));

    assertThat(
            queryBuilder
                .update(NAMESPACE, TABLE)
                .set(values)
                .where(
                    new Key(new TextValue("p1", "aaa")),
                    Optional.of(new Key(new TextValue("c1", "bbb"))),
                    Collections.singletonList(
                        new ConditionalExpression("v1", new TextValue("ccc"), Operator.EQ)))
                .build()
                .toString())
        .isEqualTo(encloseSql("UPDATE n1.t1 SET v1=?,v2=?,v3=? WHERE p1=? AND c1=? AND v1=?"));

    assertThat(
            queryBuilder
                .update(NAMESPACE, TABLE)
                .set(values)
                .where(
                    new Key(new TextValue("p1", "aaa")),
                    Optional.of(new Key(new TextValue("c1", "bbb"))),
                    Arrays.asList(
                        new ConditionalExpression("v1", new TextValue("ccc"), Operator.NE),
                        new ConditionalExpression("v2", new TextValue("ddd"), Operator.GT),
                        new ConditionalExpression("v3", new TextValue("eee"), Operator.LTE)))
                .build()
                .toString())
        .isEqualTo(
            encloseSql(
                "UPDATE n1.t1 SET v1=?,v2=?,v3=? WHERE p1=? AND c1=? AND v1<>? AND v2>? AND v3<=?"));
  }

  @Test
  public void deleteQueryTest() {
    assertThat(
            queryBuilder
                .deleteFrom(NAMESPACE, TABLE)
                .where(new Key(new TextValue("p1", "aaa")), Optional.empty())
                .build()
                .toString())
        .isEqualTo(encloseSql("DELETE FROM n1.t1 WHERE p1=?"));

    assertThat(
            queryBuilder
                .deleteFrom(NAMESPACE, TABLE)
                .where(
                    new Key(new TextValue("p1", "aaa")),
                    Optional.of(new Key(new TextValue("c1", "bbb"))))
                .build()
                .toString())
        .isEqualTo(encloseSql("DELETE FROM n1.t1 WHERE p1=? AND c1=?"));

    assertThat(
            queryBuilder
                .deleteFrom(NAMESPACE, TABLE)
                .where(
                    new Key(new TextValue("p1", "aaa"), new TextValue("p2", "ccc")),
                    Optional.of(new Key(new TextValue("c1", "bbb"), new TextValue("c2", "ddd"))))
                .build()
                .toString())
        .isEqualTo(encloseSql("DELETE FROM n1.t1 WHERE p1=? AND p2=? AND c1=? AND c2=?"));

    assertThat(
            queryBuilder
                .deleteFrom(NAMESPACE, TABLE)
                .where(
                    new Key(new TextValue("p1", "aaa")),
                    Optional.of(new Key(new TextValue("c1", "bbb"))),
                    Collections.singletonList(
                        new ConditionalExpression("v1", new TextValue("ccc"), Operator.EQ)))
                .build()
                .toString())
        .isEqualTo(encloseSql("DELETE FROM n1.t1 WHERE p1=? AND c1=? AND v1=?"));

    assertThat(
            queryBuilder
                .deleteFrom(NAMESPACE, TABLE)
                .where(
                    new Key(new TextValue("p1", "aaa")),
                    Optional.of(new Key(new TextValue("c1", "bbb"))),
                    Arrays.asList(
                        new ConditionalExpression("v1", new TextValue("ccc"), Operator.NE),
                        new ConditionalExpression("v2", new TextValue("ddd"), Operator.GTE),
                        new ConditionalExpression("v3", new TextValue("eee"), Operator.LT)))
                .build()
                .toString())
        .isEqualTo(
            encloseSql("DELETE FROM n1.t1 WHERE p1=? AND c1=? AND v1<>? AND v2>=? AND v3<?"));
  }

  @Test
  public void upsertQueryTest() {
    Map<String, Value<?>> values = new HashMap<>();
    values.put("v1", new TextValue("aaa"));
    values.put("v2", new TextValue("bbb"));
    values.put("v3", new TextValue("ddd"));

    String expectedQuery;

    switch (rdbEngine) {
      case MYSQL:
        expectedQuery =
            "INSERT INTO n1.t1 (p1,v1,v2,v3) VALUES (?,?,?,?)"
                + " ON DUPLICATE KEY UPDATE v1=?,v2=?,v3=?";
        break;
      case POSTGRESQL:
        expectedQuery =
            "INSERT INTO n1.t1 (p1,v1,v2,v3) VALUES (?,?,?,?) "
                + "ON CONFLICT (p1) DO UPDATE SET v1=?,v2=?,v3=?";
        break;
      case ORACLE:
        expectedQuery =
            "MERGE INTO n1.t1 t1 USING (SELECT ? p1 FROM DUAL) t2 ON (t1.p1=t2.p1) "
                + "WHEN MATCHED THEN UPDATE SET v1=?,v2=?,v3=? "
                + "WHEN NOT MATCHED THEN INSERT (p1,v1,v2,v3) VALUES (?,?,?,?)";
        break;
      case SQL_SERVER:
      default:
        expectedQuery =
            "MERGE n1.t1 t1 USING (SELECT ? p1) t2 ON (t1.p1=t2.p1) "
                + "WHEN MATCHED THEN UPDATE SET v1=?,v2=?,v3=? "
                + "WHEN NOT MATCHED THEN INSERT (p1,v1,v2,v3) VALUES (?,?,?,?);";
        break;
    }
    assertThat(
            queryBuilder
                .upsertInto(NAMESPACE, TABLE)
                .values(new Key(new TextValue("p1", "aaa")), Optional.empty(), values)
                .build()
                .toString())
        .isEqualTo(encloseSql(expectedQuery));

    switch (rdbEngine) {
      case MYSQL:
        expectedQuery =
            "INSERT INTO n1.t1 (p1,c1,v1,v2,v3) VALUES (?,?,?,?,?)"
                + " ON DUPLICATE KEY UPDATE v1=?,v2=?,v3=?";
        break;
      case POSTGRESQL:
        expectedQuery =
            "INSERT INTO n1.t1 (p1,c1,v1,v2,v3) VALUES (?,?,?,?,?) "
                + "ON CONFLICT (p1,c1) DO UPDATE SET v1=?,v2=?,v3=?";
        break;
      case ORACLE:
        expectedQuery =
            "MERGE INTO n1.t1 t1 USING (SELECT ? p1,? c1 FROM DUAL) t2 "
                + "ON (t1.p1=t2.p1 AND t1.c1=t2.c1) "
                + "WHEN MATCHED THEN UPDATE SET v1=?,v2=?,v3=? "
                + "WHEN NOT MATCHED THEN INSERT (p1,c1,v1,v2,v3) VALUES (?,?,?,?,?)";
        break;
      case SQL_SERVER:
      default:
        expectedQuery =
            "MERGE n1.t1 t1 USING (SELECT ? p1,? c1) t2 "
                + "ON (t1.p1=t2.p1 AND t1.c1=t2.c1) "
                + "WHEN MATCHED THEN UPDATE SET v1=?,v2=?,v3=? "
                + "WHEN NOT MATCHED THEN INSERT (p1,c1,v1,v2,v3) VALUES (?,?,?,?,?);";
        break;
    }
    assertThat(
            queryBuilder
                .upsertInto(NAMESPACE, TABLE)
                .values(
                    new Key(new TextValue("p1", "aaa")),
                    Optional.of(new Key(new TextValue("c1", "bbb"))),
                    values)
                .build()
                .toString())
        .isEqualTo(encloseSql(expectedQuery));

    values.put("v4", new TextValue("eee"));

    switch (rdbEngine) {
      case MYSQL:
        expectedQuery =
            "INSERT INTO n1.t1 (p1,p2,c1,c2,v1,v2,v3,v4) VALUES (?,?,?,?,?,?,?,?)"
                + " ON DUPLICATE KEY UPDATE v1=?,v2=?,v3=?,v4=?";
        break;
      case POSTGRESQL:
        expectedQuery =
            "INSERT INTO n1.t1 (p1,p2,c1,c2,v1,v2,v3,v4) VALUES (?,?,?,?,?,?,?,?) "
                + "ON CONFLICT (p1,p2,c1,c2) DO UPDATE SET v1=?,v2=?,v3=?,v4=?";
        break;
      case ORACLE:
        expectedQuery =
            "MERGE INTO n1.t1 t1 USING (SELECT ? p1,? p2,? c1,? c2 FROM DUAL) t2 "
                + "ON (t1.p1=t2.p1 AND t1.p2=t2.p2 AND t1.c1=t2.c1 AND t1.c2=t2.c2) "
                + "WHEN MATCHED THEN UPDATE SET v1=?,v2=?,v3=?,v4=? "
                + "WHEN NOT MATCHED THEN INSERT (p1,p2,c1,c2,v1,v2,v3,v4) "
                + "VALUES (?,?,?,?,?,?,?,?)";
        break;
      case SQL_SERVER:
      default:
        expectedQuery =
            "MERGE n1.t1 t1 USING (SELECT ? p1,? p2,? c1,? c2) t2 "
                + "ON (t1.p1=t2.p1 AND t1.p2=t2.p2 AND t1.c1=t2.c1 AND t1.c2=t2.c2) "
                + "WHEN MATCHED THEN UPDATE SET v1=?,v2=?,v3=?,v4=? "
                + "WHEN NOT MATCHED THEN INSERT (p1,p2,c1,c2,v1,v2,v3,v4) VALUES (?,?,?,?,?,?,?,?);";
        break;
    }
    assertThat(
            queryBuilder
                .upsertInto(NAMESPACE, TABLE)
                .values(
                    new Key(new TextValue("p1", "aaa"), new TextValue("p2", "ccc")),
                    Optional.of(new Key(new TextValue("c1", "bbb"), new TextValue("c2", "ddd"))),
                    values)
                .build()
                .toString())
        .isEqualTo(encloseSql(expectedQuery));
  }

  @Test
  public void upsertQueryWithoutValuesTest() {
    String expectedQuery;

    switch (rdbEngine) {
      case MYSQL:
        expectedQuery = "INSERT IGNORE INTO n1.t1 (p1) VALUES (?)";
        break;
      case POSTGRESQL:
        expectedQuery = "INSERT INTO n1.t1 (p1) VALUES (?) ON CONFLICT (p1) DO NOTHING";
        break;
      case ORACLE:
        expectedQuery =
            "MERGE INTO n1.t1 t1 USING (SELECT ? p1 FROM DUAL) t2 ON (t1.p1=t2.p1) "
                + "WHEN NOT MATCHED THEN INSERT (p1) VALUES (?)";
        break;
      case SQL_SERVER:
      default:
        expectedQuery =
            "MERGE n1.t1 t1 USING (SELECT ? p1) t2 ON (t1.p1=t2.p1) "
                + "WHEN NOT MATCHED THEN INSERT (p1) VALUES (?);";
        break;
    }
    assertThat(
            queryBuilder
                .upsertInto(NAMESPACE, TABLE)
                .values(
                    new Key(new TextValue("p1", "aaa")), Optional.empty(), Collections.emptyMap())
                .build()
                .toString())
        .isEqualTo(encloseSql(expectedQuery));

    switch (rdbEngine) {
      case MYSQL:
        expectedQuery = "INSERT IGNORE INTO n1.t1 (p1,c1) VALUES (?,?)";
        break;
      case POSTGRESQL:
        expectedQuery = "INSERT INTO n1.t1 (p1,c1) VALUES (?,?) ON CONFLICT (p1,c1) DO NOTHING";
        break;
      case ORACLE:
        expectedQuery =
            "MERGE INTO n1.t1 t1 USING (SELECT ? p1,? c1 FROM DUAL) t2 "
                + "ON (t1.p1=t2.p1 AND t1.c1=t2.c1) "
                + "WHEN NOT MATCHED THEN INSERT (p1,c1) VALUES (?,?)";
        break;
      case SQL_SERVER:
      default:
        expectedQuery =
            "MERGE n1.t1 t1 USING (SELECT ? p1,? c1) t2 "
                + "ON (t1.p1=t2.p1 AND t1.c1=t2.c1) "
                + "WHEN NOT MATCHED THEN INSERT (p1,c1) VALUES (?,?);";
        break;
    }
    assertThat(
            queryBuilder
                .upsertInto(NAMESPACE, TABLE)
                .values(
                    new Key(new TextValue("p1", "aaa")),
                    Optional.of(new Key(new TextValue("c1", "bbb"))),
                    Collections.emptyMap())
                .build()
                .toString())
        .isEqualTo(encloseSql(expectedQuery));

    switch (rdbEngine) {
      case MYSQL:
        expectedQuery = "INSERT IGNORE INTO n1.t1 (p1,p2,c1,c2) VALUES (?,?,?,?)";
        break;
      case POSTGRESQL:
        expectedQuery =
            "INSERT INTO n1.t1 (p1,p2,c1,c2) VALUES (?,?,?,?) "
                + "ON CONFLICT (p1,p2,c1,c2) DO NOTHING";
        break;
      case ORACLE:
        expectedQuery =
            "MERGE INTO n1.t1 t1 USING (SELECT ? p1,? p2,? c1,? c2 FROM DUAL) t2 "
                + "ON (t1.p1=t2.p1 AND t1.p2=t2.p2 AND t1.c1=t2.c1 AND t1.c2=t2.c2) "
                + "WHEN NOT MATCHED THEN INSERT (p1,p2,c1,c2) VALUES (?,?,?,?)";
        break;
      case SQL_SERVER:
      default:
        expectedQuery =
            "MERGE n1.t1 t1 USING (SELECT ? p1,? p2,? c1,? c2) t2 "
                + "ON (t1.p1=t2.p1 AND t1.p2=t2.p2 AND t1.c1=t2.c1 AND t1.c2=t2.c2) "
                + "WHEN NOT MATCHED THEN INSERT (p1,p2,c1,c2) VALUES (?,?,?,?);";
        break;
    }
    assertThat(
            queryBuilder
                .upsertInto(NAMESPACE, TABLE)
                .values(
                    new Key(new TextValue("p1", "aaa"), new TextValue("p2", "ccc")),
                    Optional.of(new Key(new TextValue("c1", "bbb"), new TextValue("c2", "ddd"))),
                    Collections.emptyMap())
                .build()
                .toString())
        .isEqualTo(encloseSql(expectedQuery));
  }
}
