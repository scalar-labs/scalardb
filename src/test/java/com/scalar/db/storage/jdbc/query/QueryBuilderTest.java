package com.scalar.db.storage.jdbc.query;

import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.ConditionalExpression.Operator;
import com.scalar.db.api.Scan;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.Value;
import com.scalar.db.storage.jdbc.RdbEngine;
import com.scalar.db.storage.jdbc.metadata.DataType;
import com.scalar.db.storage.jdbc.metadata.JdbcTableMetadata;
import com.scalar.db.storage.jdbc.metadata.TableMetadataManager;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

public class QueryBuilderTest {

  private static final String TABLE_FULL_NAME = "s1.t1";

  @Mock private TableMetadataManager tableMetadataManager;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);

    // Dummy metadata
    JdbcTableMetadata dummyTableMetadata =
        new JdbcTableMetadata(
            TABLE_FULL_NAME,
            new HashMap<String, DataType>() {
              {
                put("p1", DataType.TEXT);
                put("p2", DataType.TEXT);
                put("c1", DataType.TEXT);
                put("c2", DataType.TEXT);
                put("v1", DataType.TEXT);
                put("v2", DataType.TEXT);
                put("v3", DataType.TEXT);
              }
            },
            Arrays.asList("p1", "p2"),
            Arrays.asList("c1", "c2"),
            new HashMap<String, Scan.Ordering.Order>() {
              {
                put("c1", Scan.Ordering.Order.ASC);
                put("c2", Scan.Ordering.Order.DESC);
              }
            },
            new HashSet<>());

    when(tableMetadataManager.getTableMetadata(any(String.class))).thenReturn(dummyTableMetadata);
  }

  @Test
  public void simpleSelectQueryTest() {
    QueryBuilder queryBuilder = new QueryBuilder(tableMetadataManager, RdbEngine.MY_SQL);

    assertThat(
            queryBuilder
                .select(Arrays.asList("c1", "c2"))
                .from(TABLE_FULL_NAME)
                .where(new Key(new TextValue("p1", "aaa")), Optional.empty())
                .build()
                .toString())
        .isEqualTo("SELECT c1,c2 FROM s1.t1 WHERE p1=?");

    assertThat(
            queryBuilder
                .select(Collections.emptyList())
                .from(TABLE_FULL_NAME)
                .where(
                    new Key(new TextValue("p1", "aaa"), new TextValue("p2", "bbb")),
                    Optional.empty())
                .build()
                .toString())
        .isEqualTo("SELECT * FROM s1.t1 WHERE p1=? AND p2=?");

    assertThat(
            queryBuilder
                .select(Arrays.asList("c1", "c2"))
                .from(TABLE_FULL_NAME)
                .where(
                    new Key(new TextValue("p1", "aaa")),
                    Optional.of(new Key(new TextValue("c1", "aaa"))))
                .build()
                .toString())
        .isEqualTo("SELECT c1,c2 FROM s1.t1 WHERE p1=? AND c1=?");

    assertThat(
            queryBuilder
                .select(Arrays.asList("c1", "c2"))
                .from(TABLE_FULL_NAME)
                .where(
                    new Key(new TextValue("p1", "aaa")),
                    Optional.of(new Key(new TextValue("c1", "aaa"), new TextValue("c2", "bbb"))))
                .build()
                .toString())
        .isEqualTo("SELECT c1,c2 FROM s1.t1 WHERE p1=? AND c1=? AND c2=?");

    assertThat(
            queryBuilder
                .select(Arrays.asList("c1", "c2"))
                .from(TABLE_FULL_NAME)
                .where(
                    new Key(new TextValue("p1", "aaa")),
                    Optional.of(new Key(new TextValue("c1", "aaa"))),
                    true,
                    Optional.of(new Key(new TextValue("c1", "bbb"))),
                    true)
                .build()
                .toString())
        .isEqualTo(
            "SELECT c1,c2 FROM s1.t1 WHERE p1=? AND c1>=? AND c1<=? ORDER BY c1 ASC,c2 DESC");

    assertThat(
            queryBuilder
                .select(Collections.emptyList())
                .from(TABLE_FULL_NAME)
                .where(
                    new Key(new TextValue("p1", "aaa")),
                    Optional.of(new Key(new TextValue("c1", "aaa"))),
                    false,
                    Optional.of(new Key(new TextValue("c1", "bbb"))),
                    false)
                .build()
                .toString())
        .isEqualTo("SELECT * FROM s1.t1 WHERE p1=? AND c1>? AND c1<? ORDER BY c1 ASC,c2 DESC");

    assertThat(
            queryBuilder
                .select(Arrays.asList("c1", "c2"))
                .from(TABLE_FULL_NAME)
                .where(
                    new Key(new TextValue("p1", "aaa")),
                    Optional.of(new Key(new TextValue("c1", "aaa"), new TextValue("c2", "aaa"))),
                    true,
                    Optional.of(new Key(new TextValue("c1", "aaa"), new TextValue("c2", "bbb"))),
                    false)
                .build()
                .toString())
        .isEqualTo(
            "SELECT c1,c2 FROM s1.t1 WHERE p1=? AND c1=? AND c2>=? AND c2<? "
                + "ORDER BY c1 ASC,c2 DESC");

    assertThat(
            queryBuilder
                .select(Arrays.asList("c1", "c2"))
                .from(TABLE_FULL_NAME)
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
            "SELECT c1,c2 FROM s1.t1 WHERE p1=? AND c1>=? AND c1<=? ORDER BY c1 ASC,c2 DESC");

    assertThat(
            queryBuilder
                .select(Arrays.asList("c1", "c2"))
                .from(TABLE_FULL_NAME)
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
            "SELECT c1,c2 FROM s1.t1 WHERE p1=? AND c1>=? AND c1<=? ORDER BY c1 ASC,c2 DESC");

    assertThat(
            queryBuilder
                .select(Arrays.asList("c1", "c2"))
                .from(TABLE_FULL_NAME)
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
            "SELECT c1,c2 FROM s1.t1 WHERE p1=? AND c1>=? AND c1<=? ORDER BY c1 DESC,c2 ASC");

    assertThat(
            queryBuilder
                .select(Arrays.asList("c1", "c2"))
                .from(TABLE_FULL_NAME)
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
            "SELECT c1,c2 FROM s1.t1 WHERE p1=? AND c1>=? AND c1<=? ORDER BY c1 DESC,c2 ASC");
  }

  @Test
  public void selectQueryWithLimitForMySQLAndPostgreSQLTest() {
    QueryBuilder queryBuilder = new QueryBuilder(tableMetadataManager, RdbEngine.MY_SQL);
    assertThat(
            queryBuilder
                .select(Arrays.asList("c1", "c2"))
                .from(TABLE_FULL_NAME)
                .where(
                    new Key(new TextValue("p1", "aaa")),
                    Optional.of(new Key(new TextValue("c1", "aaa"))),
                    true,
                    Optional.of(new Key(new TextValue("c1", "bbb"))),
                    true)
                .limit(10)
                .build()
                .toString())
        .isEqualTo(
            "SELECT c1,c2 FROM s1.t1 WHERE p1=? AND c1>=? AND c1<=? "
                + "ORDER BY c1 ASC,c2 DESC LIMIT 10");
  }

  @Test
  public void selectQueryWithLimitForOracleTest() {
    QueryBuilder queryBuilder = new QueryBuilder(tableMetadataManager, RdbEngine.ORACLE);
    assertThat(
            queryBuilder
                .select(Arrays.asList("c1", "c2"))
                .from(TABLE_FULL_NAME)
                .where(
                    new Key(new TextValue("p1", "aaa")),
                    Optional.of(new Key(new TextValue("c1", "aaa"))),
                    true,
                    Optional.of(new Key(new TextValue("c1", "bbb"))),
                    true)
                .limit(10)
                .build()
                .toString())
        .isEqualTo(
            "SELECT * FROM (SELECT c1,c2 FROM s1.t1 WHERE p1=? AND c1>=? AND c1<=? "
                + "ORDER BY c1 ASC,c2 DESC) WHERE ROWNUM <= 10");
  }

  @Test
  public void selectQueryWithLimitForSQLServerTest() {
    QueryBuilder queryBuilder = new QueryBuilder(tableMetadataManager, RdbEngine.SQL_SERVER);
    assertThat(
            queryBuilder
                .select(Arrays.asList("c1", "c2"))
                .from(TABLE_FULL_NAME)
                .where(
                    new Key(new TextValue("p1", "aaa")),
                    Optional.of(new Key(new TextValue("c1", "aaa"))),
                    true,
                    Optional.of(new Key(new TextValue("c1", "bbb"))),
                    true)
                .limit(10)
                .build()
                .toString())
        .isEqualTo(
            "SELECT c1,c2 FROM s1.t1 WHERE p1=? AND c1>=? AND c1<=? "
                + "ORDER BY c1 ASC,c2 DESC OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY");
  }

  @Test
  public void insertQueryTest() {
    QueryBuilder queryBuilder = new QueryBuilder(tableMetadataManager, RdbEngine.MY_SQL);

    Map<String, Value> values = new HashMap<>();
    values.put("v1", new TextValue("aaa"));
    values.put("v2", new TextValue("bbb"));
    values.put("v3", new TextValue("ddd"));

    assertThat(
            queryBuilder
                .insertInto(TABLE_FULL_NAME)
                .values(new Key(new TextValue("p1", "aaa")), Optional.empty(), values)
                .build()
                .toString())
        .isEqualTo("INSERT INTO s1.t1 (p1,v1,v2,v3) VALUES(?,?,?,?)");

    assertThat(
            queryBuilder
                .insertInto(TABLE_FULL_NAME)
                .values(
                    new Key(new TextValue("p1", "aaa")),
                    Optional.of(new Key(new TextValue("c1", "bbb"))),
                    values)
                .build()
                .toString())
        .isEqualTo("INSERT INTO s1.t1 (p1,c1,v1,v2,v3) VALUES(?,?,?,?,?)");

    values.put("v4", new TextValue("eee"));

    assertThat(
            queryBuilder
                .insertInto(TABLE_FULL_NAME)
                .values(
                    new Key(new TextValue("p1", "aaa"), new TextValue("p2", "ccc")),
                    Optional.of(new Key(new TextValue("c1", "bbb"), new TextValue("c2", "ddd"))),
                    values)
                .build()
                .toString())
        .isEqualTo("INSERT INTO s1.t1 (p1,p2,c1,c2,v1,v2,v3,v4) VALUES(?,?,?,?,?,?,?,?)");
  }

  @Test
  public void updateQueryTest() {
    QueryBuilder queryBuilder = new QueryBuilder(tableMetadataManager, RdbEngine.MY_SQL);

    Map<String, Value> values = new HashMap<>();
    values.put("v1", new TextValue("aaa"));
    values.put("v2", new TextValue("bbb"));
    values.put("v3", new TextValue("ddd"));

    assertThat(
            queryBuilder
                .update(TABLE_FULL_NAME)
                .set(values)
                .where(new Key(new TextValue("p1", "aaa")), Optional.empty())
                .build()
                .toString())
        .isEqualTo("UPDATE s1.t1 SET v1=?,v2=?,v3=? WHERE p1=?");

    assertThat(
            queryBuilder
                .update(TABLE_FULL_NAME)
                .set(values)
                .where(
                    new Key(new TextValue("p1", "aaa")),
                    Optional.of(new Key(new TextValue("c1", "bbb"))))
                .build()
                .toString())
        .isEqualTo("UPDATE s1.t1 SET v1=?,v2=?,v3=? WHERE p1=? AND c1=?");

    assertThat(
            queryBuilder
                .update(TABLE_FULL_NAME)
                .set(values)
                .where(
                    new Key(new TextValue("p1", "aaa"), new TextValue("p2", "ccc")),
                    Optional.of(new Key(new TextValue("c1", "bbb"), new TextValue("c2", "ddd"))))
                .build()
                .toString())
        .isEqualTo("UPDATE s1.t1 SET v1=?,v2=?,v3=? WHERE p1=? AND p2=? AND c1=? AND c2=?");

    assertThat(
            queryBuilder
                .update(TABLE_FULL_NAME)
                .set(values)
                .where(
                    new Key(new TextValue("p1", "aaa")),
                    Optional.of(new Key(new TextValue("c1", "bbb"))),
                    Collections.singletonList(
                        new ConditionalExpression("v1", new TextValue("ccc"), Operator.EQ)))
                .build()
                .toString())
        .isEqualTo("UPDATE s1.t1 SET v1=?,v2=?,v3=? WHERE p1=? AND c1=? AND v1=?");

    assertThat(
            queryBuilder
                .update(TABLE_FULL_NAME)
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
            "UPDATE s1.t1 SET v1=?,v2=?,v3=? WHERE p1=? AND c1=? AND v1<>? AND v2>? AND v3<=?");
  }

  @Test
  public void deleteQueryTest() {
    QueryBuilder queryBuilder = new QueryBuilder(tableMetadataManager, RdbEngine.MY_SQL);

    assertThat(
            queryBuilder
                .deleteFrom(TABLE_FULL_NAME)
                .where(new Key(new TextValue("p1", "aaa")), Optional.empty())
                .build()
                .toString())
        .isEqualTo("DELETE FROM s1.t1 WHERE p1=?");

    assertThat(
            queryBuilder
                .deleteFrom(TABLE_FULL_NAME)
                .where(
                    new Key(new TextValue("p1", "aaa")),
                    Optional.of(new Key(new TextValue("c1", "bbb"))))
                .build()
                .toString())
        .isEqualTo("DELETE FROM s1.t1 WHERE p1=? AND c1=?");

    assertThat(
            queryBuilder
                .deleteFrom(TABLE_FULL_NAME)
                .where(
                    new Key(new TextValue("p1", "aaa"), new TextValue("p2", "ccc")),
                    Optional.of(new Key(new TextValue("c1", "bbb"), new TextValue("c2", "ddd"))))
                .build()
                .toString())
        .isEqualTo("DELETE FROM s1.t1 WHERE p1=? AND p2=? AND c1=? AND c2=?");

    assertThat(
            queryBuilder
                .deleteFrom(TABLE_FULL_NAME)
                .where(
                    new Key(new TextValue("p1", "aaa")),
                    Optional.of(new Key(new TextValue("c1", "bbb"))),
                    Collections.singletonList(
                        new ConditionalExpression("v1", new TextValue("ccc"), Operator.EQ)))
                .build()
                .toString())
        .isEqualTo("DELETE FROM s1.t1 WHERE p1=? AND c1=? AND v1=?");

    assertThat(
            queryBuilder
                .deleteFrom(TABLE_FULL_NAME)
                .where(
                    new Key(new TextValue("p1", "aaa")),
                    Optional.of(new Key(new TextValue("c1", "bbb"))),
                    Arrays.asList(
                        new ConditionalExpression("v1", new TextValue("ccc"), Operator.NE),
                        new ConditionalExpression("v2", new TextValue("ddd"), Operator.GTE),
                        new ConditionalExpression("v3", new TextValue("eee"), Operator.LT)))
                .build()
                .toString())
        .isEqualTo("DELETE FROM s1.t1 WHERE p1=? AND c1=? AND v1<>? AND v2>=? AND v3<?");
  }

  @Test
  public void upsertQueryForMySQLTest() {
    QueryBuilder queryBuilder = new QueryBuilder(tableMetadataManager, RdbEngine.MY_SQL);

    Map<String, Value> values = new HashMap<>();
    values.put("v1", new TextValue("aaa"));
    values.put("v2", new TextValue("bbb"));
    values.put("v3", new TextValue("ddd"));

    assertThat(
            queryBuilder
                .upsertInto(TABLE_FULL_NAME)
                .values(new Key(new TextValue("p1", "aaa")), Optional.empty(), values)
                .build()
                .toString())
        .isEqualTo(
            "INSERT INTO s1.t1 (p1,v1,v2,v3) VALUES(?,?,?,?)"
                + " ON DUPLICATE KEY UPDATE v1=?,v2=?,v3=?");

    assertThat(
            queryBuilder
                .upsertInto(TABLE_FULL_NAME)
                .values(
                    new Key(new TextValue("p1", "aaa")),
                    Optional.of(new Key(new TextValue("c1", "bbb"))),
                    values)
                .build()
                .toString())
        .isEqualTo(
            "INSERT INTO s1.t1 (p1,c1,v1,v2,v3) VALUES(?,?,?,?,?)"
                + " ON DUPLICATE KEY UPDATE v1=?,v2=?,v3=?");

    values.put("v4", new TextValue("eee"));

    assertThat(
            queryBuilder
                .upsertInto(TABLE_FULL_NAME)
                .values(
                    new Key(new TextValue("p1", "aaa"), new TextValue("p2", "ccc")),
                    Optional.of(new Key(new TextValue("c1", "bbb"), new TextValue("c2", "ddd"))),
                    values)
                .build()
                .toString())
        .isEqualTo(
            "INSERT INTO s1.t1 (p1,p2,c1,c2,v1,v2,v3,v4) VALUES(?,?,?,?,?,?,?,?)"
                + " ON DUPLICATE KEY UPDATE v1=?,v2=?,v3=?,v4=?");
  }

  @Test
  public void upsertQueryForPostgreSQLTest() {
    QueryBuilder queryBuilder = new QueryBuilder(tableMetadataManager, RdbEngine.POSTGRE_SQL);

    Map<String, Value> values = new HashMap<>();
    values.put("v1", new TextValue("aaa"));
    values.put("v2", new TextValue("bbb"));
    values.put("v3", new TextValue("ddd"));

    assertThat(
            queryBuilder
                .upsertInto(TABLE_FULL_NAME)
                .values(new Key(new TextValue("p1", "aaa")), Optional.empty(), values)
                .build()
                .toString())
        .isEqualTo(
            "INSERT INTO s1.t1 (p1,v1,v2,v3) VALUES(?,?,?,?) "
                + "ON CONFLICT (p1) DO UPDATE SET v1=?,v2=?,v3=?");

    assertThat(
            queryBuilder
                .upsertInto(TABLE_FULL_NAME)
                .values(
                    new Key(new TextValue("p1", "aaa")),
                    Optional.of(new Key(new TextValue("c1", "bbb"))),
                    values)
                .build()
                .toString())
        .isEqualTo(
            "INSERT INTO s1.t1 (p1,c1,v1,v2,v3) VALUES(?,?,?,?,?) "
                + "ON CONFLICT (p1,c1) DO UPDATE SET v1=?,v2=?,v3=?");

    values.put("v4", new TextValue("eee"));

    assertThat(
            queryBuilder
                .upsertInto(TABLE_FULL_NAME)
                .values(
                    new Key(new TextValue("p1", "aaa"), new TextValue("p2", "ccc")),
                    Optional.of(new Key(new TextValue("c1", "bbb"), new TextValue("c2", "ddd"))),
                    values)
                .build()
                .toString())
        .isEqualTo(
            "INSERT INTO s1.t1 (p1,p2,c1,c2,v1,v2,v3,v4) VALUES(?,?,?,?,?,?,?,?) "
                + "ON CONFLICT (p1,p2,c1,c2) DO UPDATE SET v1=?,v2=?,v3=?,v4=?");
  }

  @Test
  public void upsertQueryForOracleAndSQLServerTest() {
    QueryBuilder queryBuilder = new QueryBuilder(tableMetadataManager, RdbEngine.ORACLE);

    Map<String, Value> values = new HashMap<>();
    values.put("v1", new TextValue("aaa"));
    values.put("v2", new TextValue("bbb"));
    values.put("v3", new TextValue("ddd"));

    assertThat(
            queryBuilder
                .upsertInto(TABLE_FULL_NAME)
                .values(new Key(new TextValue("p1", "aaa")), Optional.empty(), values)
                .build()
                .toString())
        .isEqualTo(
            "MERGE INTO s1.t1 t1 USING (SELECT ? p1 FROM DUAL) t2 ON (t1.p1=t2.p1) "
                + "WHEN MATCHED THEN UPDATE SET v1=?,v2=?,v3=? "
                + "WHEN NOT MATCHED THEN INSERT (p1,v1,v2,v3) VALUES(?,?,?,?)");

    assertThat(
            queryBuilder
                .upsertInto(TABLE_FULL_NAME)
                .values(
                    new Key(new TextValue("p1", "aaa")),
                    Optional.of(new Key(new TextValue("c1", "bbb"))),
                    values)
                .build()
                .toString())
        .isEqualTo(
            "MERGE INTO s1.t1 t1 USING (SELECT ? p1,? c1 FROM DUAL) t2 "
                + "ON (t1.p1=t2.p1 AND t1.c1=t2.c1) WHEN MATCHED THEN UPDATE SET v1=?,v2=?,v3=? "
                + "WHEN NOT MATCHED THEN INSERT (p1,c1,v1,v2,v3) VALUES(?,?,?,?,?)");

    values.put("v4", new TextValue("eee"));

    assertThat(
            queryBuilder
                .upsertInto(TABLE_FULL_NAME)
                .values(
                    new Key(new TextValue("p1", "aaa"), new TextValue("p2", "ccc")),
                    Optional.of(new Key(new TextValue("c1", "bbb"), new TextValue("c2", "ddd"))),
                    values)
                .build()
                .toString())
        .isEqualTo(
            "MERGE INTO s1.t1 t1 USING (SELECT ? p1,? p2,? c1,? c2 FROM DUAL) t2 "
                + "ON (t1.p1=t2.p1 AND t1.p2=t2.p2 AND t1.c1=t2.c1 AND t1.c2=t2.c2) "
                + "WHEN MATCHED THEN UPDATE SET v1=?,v2=?,v3=?,v4=? "
                + "WHEN NOT MATCHED THEN INSERT (p1,p2,c1,c2,v1,v2,v3,v4) "
                + "VALUES(?,?,?,?,?,?,?,?)");
  }
}
