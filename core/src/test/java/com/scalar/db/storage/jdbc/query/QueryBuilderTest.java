package com.scalar.db.storage.jdbc.query;

import static com.scalar.db.storage.jdbc.query.QueryUtils.enclose;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.ConditionalExpression.Operator;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import com.scalar.db.io.TextValue;
import com.scalar.db.storage.jdbc.RdbEngine;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

public class QueryBuilderTest {

  private static final String NAMESPACE = "n1";
  private static final String TABLE = "t1";
  private static final TableMetadata TABLE_METADATA =
      TableMetadata.newBuilder()
          .addColumn("p1", DataType.TEXT)
          .addColumn("p2", DataType.INT)
          .addColumn("c1", DataType.TEXT)
          .addColumn("c2", DataType.TEXT)
          .addColumn("v1", DataType.TEXT)
          .addColumn("v2", DataType.TEXT)
          .addColumn("v3", DataType.TEXT)
          .addColumn("v4", DataType.TEXT)
          .addColumn("v5", DataType.TEXT)
          .addPartitionKey("p1")
          .addPartitionKey("p2")
          .addClusteringKey("c1", Scan.Ordering.Order.ASC)
          .addClusteringKey("c2", Scan.Ordering.Order.DESC)
          .addSecondaryIndex("v1")
          .addSecondaryIndex("v2")
          .build();

  @ParameterizedTest
  @EnumSource(RdbEngine.class)
  public void selectQueryTest(RdbEngine rdbEngine) throws SQLException {
    QueryBuilder queryBuilder = new QueryBuilder(rdbEngine);

    SelectQuery query;
    PreparedStatement preparedStatement;

    preparedStatement = mock(PreparedStatement.class);
    query =
        queryBuilder
            .select(Arrays.asList("c1", "c2"))
            .from(NAMESPACE, TABLE, TABLE_METADATA)
            .where(new Key("p1", "p1Value"), Optional.empty())
            .build();
    assertThat(query.sql()).isEqualTo(encloseSql("SELECT c1,c2 FROM n1.t1 WHERE p1=?", rdbEngine));
    query.bind(preparedStatement);
    verify(preparedStatement).setString(1, "p1Value");

    preparedStatement = mock(PreparedStatement.class);
    query =
        queryBuilder
            .select(Collections.emptyList())
            .from(NAMESPACE, TABLE, TABLE_METADATA)
            .where(new Key("p1", "p1Value", "p2", "p2Value"), Optional.empty())
            .build();
    assertThat(query.sql())
        .isEqualTo(encloseSql("SELECT * FROM n1.t1 WHERE p1=? AND p2=?", rdbEngine));
    query.bind(preparedStatement);
    verify(preparedStatement).setString(1, "p1Value");
    verify(preparedStatement).setString(2, "p2Value");

    preparedStatement = mock(PreparedStatement.class);
    query =
        queryBuilder
            .select(Arrays.asList("c1", "c2"))
            .from(NAMESPACE, TABLE, TABLE_METADATA)
            .where(new Key("p1", "p1Value"), Optional.of(new Key("c1", "c1Value")))
            .build();
    assertThat(query.sql())
        .isEqualTo(encloseSql("SELECT c1,c2 FROM n1.t1 WHERE p1=? AND c1=?", rdbEngine));
    query.bind(preparedStatement);
    verify(preparedStatement).setString(1, "p1Value");
    verify(preparedStatement).setString(2, "c1Value");

    preparedStatement = mock(PreparedStatement.class);
    query =
        queryBuilder
            .select(Arrays.asList("c1", "c2"))
            .from(NAMESPACE, TABLE, TABLE_METADATA)
            .where(new Key("p1", "p1Value"), Optional.of(new Key("c1", "c1Value", "c2", "c2Value")))
            .build();
    assertThat(query.sql())
        .isEqualTo(encloseSql("SELECT c1,c2 FROM n1.t1 WHERE p1=? AND c1=? AND c2=?", rdbEngine));
    query.bind(preparedStatement);
    verify(preparedStatement).setString(1, "p1Value");
    verify(preparedStatement).setString(2, "c1Value");
    verify(preparedStatement).setString(3, "c2Value");

    preparedStatement = mock(PreparedStatement.class);
    query =
        queryBuilder
            .select(Arrays.asList("c1", "c2"))
            .from(NAMESPACE, TABLE, TABLE_METADATA)
            .where(
                new Key("p1", "p1Value"),
                Optional.of(new Key("c1", "c1StartValue")),
                true,
                Optional.of(new Key("c1", "c1EndValue")),
                true)
            .build();
    assertThat(query.sql())
        .isEqualTo(
            encloseSql(
                "SELECT c1,c2 FROM n1.t1 WHERE p1=? AND c1>=? AND c1<=? ORDER BY c1 ASC,c2 DESC",
                rdbEngine));
    query.bind(preparedStatement);
    verify(preparedStatement).setString(1, "p1Value");
    verify(preparedStatement).setString(2, "c1StartValue");
    verify(preparedStatement).setString(3, "c1EndValue");

    preparedStatement = mock(PreparedStatement.class);
    query =
        queryBuilder
            .select(Collections.emptyList())
            .from(NAMESPACE, TABLE, TABLE_METADATA)
            .where(
                new Key("p1", "p1Value"),
                Optional.of(new Key("c1", "c1StartValue")),
                false,
                Optional.of(new Key("c1", "c1EndValue")),
                false)
            .build();
    assertThat(query.sql())
        .isEqualTo(
            encloseSql(
                "SELECT * FROM n1.t1 WHERE p1=? AND c1>? AND c1<? ORDER BY c1 ASC,c2 DESC",
                rdbEngine));
    query.bind(preparedStatement);
    verify(preparedStatement).setString(1, "p1Value");
    verify(preparedStatement).setString(2, "c1StartValue");
    verify(preparedStatement).setString(3, "c1EndValue");

    preparedStatement = mock(PreparedStatement.class);
    query =
        queryBuilder
            .select(Arrays.asList("c1", "c2"))
            .from(NAMESPACE, TABLE, TABLE_METADATA)
            .where(
                new Key("p1", "p1Value"),
                Optional.of(new Key("c1", "c1Value", "c2", "c2StartValue")),
                true,
                Optional.of(new Key("c1", "c1Value", "c2", "c2EndValue")),
                false)
            .build();
    assertThat(query.sql())
        .isEqualTo(
            encloseSql(
                "SELECT c1,c2 FROM n1.t1 WHERE p1=? AND c1=? AND c2>=? AND c2<? "
                    + "ORDER BY c1 ASC,c2 DESC",
                rdbEngine));
    query.bind(preparedStatement);
    verify(preparedStatement).setString(1, "p1Value");
    verify(preparedStatement).setString(2, "c1Value");
    verify(preparedStatement).setString(3, "c2StartValue");
    verify(preparedStatement).setString(4, "c2EndValue");

    preparedStatement = mock(PreparedStatement.class);
    query =
        queryBuilder
            .select(Arrays.asList("c1", "c2"))
            .from(NAMESPACE, TABLE, TABLE_METADATA)
            .where(
                new Key("p1", "p1Value"),
                Optional.of(new Key("c1", "c1StartValue")),
                true,
                Optional.of(new Key("c1", "c1EndValue")),
                true)
            .orderBy(Collections.singletonList(new Scan.Ordering("c1", Scan.Ordering.Order.ASC)))
            .build();
    assertThat(query.sql())
        .isEqualTo(
            encloseSql(
                "SELECT c1,c2 FROM n1.t1 WHERE p1=? AND c1>=? AND c1<=? ORDER BY c1 ASC,c2 DESC",
                rdbEngine));
    query.bind(preparedStatement);
    verify(preparedStatement).setString(1, "p1Value");
    verify(preparedStatement).setString(2, "c1StartValue");
    verify(preparedStatement).setString(3, "c1EndValue");

    preparedStatement = mock(PreparedStatement.class);
    query =
        queryBuilder
            .select(Arrays.asList("c1", "c2"))
            .from(NAMESPACE, TABLE, TABLE_METADATA)
            .where(
                new Key("p1", "p1Value"),
                Optional.of(new Key("c1", "c1StartValue")),
                true,
                Optional.of(new Key("c1", "c1EndValue")),
                true)
            .orderBy(
                Arrays.asList(
                    new Scan.Ordering("c1", Scan.Ordering.Order.ASC),
                    new Scan.Ordering("c2", Scan.Ordering.Order.DESC)))
            .build();
    assertThat(query.sql())
        .isEqualTo(
            encloseSql(
                "SELECT c1,c2 FROM n1.t1 WHERE p1=? AND c1>=? AND c1<=? ORDER BY c1 ASC,c2 DESC",
                rdbEngine));
    query.bind(preparedStatement);
    verify(preparedStatement).setString(1, "p1Value");
    verify(preparedStatement).setString(2, "c1StartValue");
    verify(preparedStatement).setString(3, "c1EndValue");

    preparedStatement = mock(PreparedStatement.class);
    query =
        queryBuilder
            .select(Arrays.asList("c1", "c2"))
            .from(NAMESPACE, TABLE, TABLE_METADATA)
            .where(
                new Key("p1", "p1Value"),
                Optional.of(new Key("c1", "c1StartValue")),
                true,
                Optional.of(new Key("c1", "c1EndValue")),
                true)
            .orderBy(Collections.singletonList(new Scan.Ordering("c1", Scan.Ordering.Order.DESC)))
            .build();
    assertThat(query.sql())
        .isEqualTo(
            encloseSql(
                "SELECT c1,c2 FROM n1.t1 WHERE p1=? AND c1>=? AND c1<=? ORDER BY c1 DESC,c2 ASC",
                rdbEngine));
    query.bind(preparedStatement);
    verify(preparedStatement).setString(1, "p1Value");
    verify(preparedStatement).setString(2, "c1StartValue");
    verify(preparedStatement).setString(3, "c1EndValue");

    preparedStatement = mock(PreparedStatement.class);
    query =
        queryBuilder
            .select(Arrays.asList("c1", "c2"))
            .from(NAMESPACE, TABLE, TABLE_METADATA)
            .where(
                new Key("p1", "p1Value"),
                Optional.of(new Key("c1", "c1StartValue")),
                true,
                Optional.of(new Key("c1", "c1EndValue")),
                true)
            .orderBy(
                Arrays.asList(
                    new Scan.Ordering("c1", Scan.Ordering.Order.DESC),
                    new Scan.Ordering("c2", Scan.Ordering.Order.ASC)))
            .build();
    assertThat(query.sql())
        .isEqualTo(
            encloseSql(
                "SELECT c1,c2 FROM n1.t1 WHERE p1=? AND c1>=? AND c1<=? ORDER BY c1 DESC,c2 ASC",
                rdbEngine));
    query.bind(preparedStatement);
    verify(preparedStatement).setString(1, "p1Value");
    verify(preparedStatement).setString(2, "c1StartValue");
    verify(preparedStatement).setString(3, "c1EndValue");

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
            "SELECT TOP 10 c1,c2 FROM n1.t1 WHERE p1=? AND c1>=? AND c1<=? "
                + "ORDER BY c1 ASC,c2 DESC";
        break;
    }
    preparedStatement = mock(PreparedStatement.class);
    query =
        queryBuilder
            .select(Arrays.asList("c1", "c2"))
            .from(NAMESPACE, TABLE, TABLE_METADATA)
            .where(
                new Key("p1", "p1Value"),
                Optional.of(new Key("c1", "c1StartValue")),
                true,
                Optional.of(new Key("c1", "c1EndValue")),
                true)
            .limit(10)
            .build();
    assertThat(query.sql()).isEqualTo(encloseSql(expectedQuery, rdbEngine));
    query.bind(preparedStatement);
    verify(preparedStatement).setString(1, "p1Value");
    verify(preparedStatement).setString(2, "c1StartValue");
    verify(preparedStatement).setString(3, "c1EndValue");
  }

  @ParameterizedTest
  @EnumSource(RdbEngine.class)
  public void selectQueryWithIndexedColumnTest(RdbEngine rdbEngine) throws SQLException {
    QueryBuilder queryBuilder = new QueryBuilder(rdbEngine);

    SelectQuery query;
    PreparedStatement preparedStatement;

    preparedStatement = mock(PreparedStatement.class);
    query =
        queryBuilder
            .select(Arrays.asList("c1", "c2"))
            .from(NAMESPACE, TABLE, TABLE_METADATA)
            .where(new Key("v1", "v1Value"), Optional.empty())
            .build();
    assertThat(query.sql()).isEqualTo(encloseSql("SELECT c1,c2 FROM n1.t1 WHERE v1=?", rdbEngine));
    query.bind(preparedStatement);
    verify(preparedStatement).setString(1, "v1Value");

    preparedStatement = mock(PreparedStatement.class);
    query =
        queryBuilder
            .select(Arrays.asList("c1", "c2"))
            .from(NAMESPACE, TABLE, TABLE_METADATA)
            .where(new Key("v1", "v1Value"), Optional.empty(), false, Optional.empty(), false)
            .build();
    assertThat(query.sql()).isEqualTo(encloseSql("SELECT c1,c2 FROM n1.t1 WHERE v1=?", rdbEngine));
    query.bind(preparedStatement);
    verify(preparedStatement).setString(1, "v1Value");

    preparedStatement = mock(PreparedStatement.class);
    query =
        queryBuilder
            .select(Arrays.asList("c1", "c2"))
            .from(NAMESPACE, TABLE, TABLE_METADATA)
            .where(new Key("v2", "v2Value"), Optional.empty())
            .build();
    assertThat(query.sql()).isEqualTo(encloseSql("SELECT c1,c2 FROM n1.t1 WHERE v2=?", rdbEngine));
    query.bind(preparedStatement);
    verify(preparedStatement).setString(1, "v2Value");

    preparedStatement = mock(PreparedStatement.class);
    query =
        queryBuilder
            .select(Arrays.asList("c1", "c2"))
            .from(NAMESPACE, TABLE, TABLE_METADATA)
            .where(new Key("v2", "v2Value"), Optional.empty(), false, Optional.empty(), false)
            .build();
    assertThat(query.sql()).isEqualTo(encloseSql("SELECT c1,c2 FROM n1.t1 WHERE v2=?", rdbEngine));
    query.bind(preparedStatement);
    verify(preparedStatement).setString(1, "v2Value");
  }

  @ParameterizedTest
  @EnumSource(RdbEngine.class)
  public void selectQueryWithTableWithoutClusteringKeyTest(RdbEngine rdbEngine)
      throws SQLException {
    QueryBuilder queryBuilder = new QueryBuilder(rdbEngine);

    TableMetadata tableMetadataWithoutClusteringKey =
        TableMetadata.newBuilder()
            .addColumn("p1", DataType.TEXT)
            .addColumn("p2", DataType.INT)
            .addColumn("v1", DataType.TEXT)
            .addColumn("v2", DataType.TEXT)
            .addColumn("v3", DataType.TEXT)
            .addPartitionKey("p1")
            .addPartitionKey("p2")
            .build();

    SelectQuery query;
    PreparedStatement preparedStatement;

    preparedStatement = mock(PreparedStatement.class);
    query =
        queryBuilder
            .select(Arrays.asList("c1", "c2"))
            .from(NAMESPACE, TABLE, tableMetadataWithoutClusteringKey)
            .where(Key.of("p1", "p1Value", "p2", "p2Value"), Optional.empty())
            .build();
    assertThat(query.sql())
        .isEqualTo(encloseSql("SELECT c1,c2 FROM n1.t1 WHERE p1=? AND p2=?", rdbEngine));
    query.bind(preparedStatement);
    verify(preparedStatement).setString(1, "p1Value");
    verify(preparedStatement).setString(2, "p2Value");

    preparedStatement = mock(PreparedStatement.class);
    query =
        queryBuilder
            .select(Arrays.asList("c1", "c2"))
            .from(NAMESPACE, TABLE, tableMetadataWithoutClusteringKey)
            .where(
                Key.of("p1", "p1Value", "p2", "p2Value"),
                Optional.empty(),
                false,
                Optional.empty(),
                false)
            .build();
    assertThat(query.sql())
        .isEqualTo(encloseSql("SELECT c1,c2 FROM n1.t1 WHERE p1=? AND p2=?", rdbEngine));
    query.bind(preparedStatement);
    verify(preparedStatement).setString(1, "p1Value");
    verify(preparedStatement).setString(2, "p2Value");
  }

  @ParameterizedTest
  @EnumSource(RdbEngine.class)
  public void insertQueryTest(RdbEngine rdbEngine) throws SQLException {
    QueryBuilder queryBuilder = new QueryBuilder(rdbEngine);

    InsertQuery query;
    PreparedStatement preparedStatement;

    Map<String, Column<?>> columns = new HashMap<>();
    columns.put("v1", TextColumn.of("v1", "v1Value"));
    columns.put("v2", TextColumn.of("v2", "v2Value"));
    columns.put("v3", TextColumn.of("v3", "v3Value"));

    preparedStatement = mock(PreparedStatement.class);
    query =
        queryBuilder
            .insertInto(NAMESPACE, TABLE, TABLE_METADATA)
            .values(new Key("p1", "p1Value"), Optional.empty(), columns)
            .build();
    assertThat(query.sql())
        .isEqualTo(encloseSql("INSERT INTO n1.t1 (p1,v1,v2,v3) VALUES (?,?,?,?)", rdbEngine));
    query.bind(preparedStatement);
    verify(preparedStatement).setString(1, "p1Value");
    verify(preparedStatement).setString(2, "v1Value");
    verify(preparedStatement).setString(3, "v2Value");
    verify(preparedStatement).setString(4, "v3Value");

    preparedStatement = mock(PreparedStatement.class);
    query =
        queryBuilder
            .insertInto(NAMESPACE, TABLE, TABLE_METADATA)
            .values(new Key("p1", "p1Value"), Optional.of(new Key("c1", "c1Value")), columns)
            .build();
    assertThat(query.sql())
        .isEqualTo(encloseSql("INSERT INTO n1.t1 (p1,c1,v1,v2,v3) VALUES (?,?,?,?,?)", rdbEngine));
    query.bind(preparedStatement);
    verify(preparedStatement).setString(1, "p1Value");
    verify(preparedStatement).setString(2, "c1Value");
    verify(preparedStatement).setString(3, "v1Value");
    verify(preparedStatement).setString(4, "v2Value");
    verify(preparedStatement).setString(5, "v3Value");

    columns.put("v4", TextColumn.of("v4", "v4Value"));
    preparedStatement = mock(PreparedStatement.class);
    query =
        queryBuilder
            .insertInto(NAMESPACE, TABLE, TABLE_METADATA)
            .values(
                new Key("p1", "p1Value", "p2", "p2Value"),
                Optional.of(new Key("c1", "c1Value", "c2", "c2Value")),
                columns)
            .build();
    assertThat(query.sql())
        .isEqualTo(
            encloseSql(
                "INSERT INTO n1.t1 (p1,p2,c1,c2,v1,v2,v3,v4) VALUES (?,?,?,?,?,?,?,?)", rdbEngine));
    query.bind(preparedStatement);
    verify(preparedStatement).setString(1, "p1Value");
    verify(preparedStatement).setString(2, "p2Value");
    verify(preparedStatement).setString(3, "c1Value");
    verify(preparedStatement).setString(4, "c2Value");
    verify(preparedStatement).setString(5, "v1Value");
    verify(preparedStatement).setString(6, "v2Value");
    verify(preparedStatement).setString(7, "v3Value");
    verify(preparedStatement).setString(8, "v4Value");

    columns.put("v5", TextColumn.ofNull("v5"));
    preparedStatement = mock(PreparedStatement.class);
    query =
        queryBuilder
            .insertInto(NAMESPACE, TABLE, TABLE_METADATA)
            .values(
                new Key("p1", "p1Value", "p2", "p2Value"),
                Optional.of(new Key("c1", "c1Value", "c2", "c2Value")),
                columns)
            .build();
    assertThat(query.sql())
        .isEqualTo(
            encloseSql(
                "INSERT INTO n1.t1 (p1,p2,c1,c2,v1,v2,v3,v4,v5) VALUES (?,?,?,?,?,?,?,?,?)",
                rdbEngine));
    query.bind(preparedStatement);
    verify(preparedStatement).setString(1, "p1Value");
    verify(preparedStatement).setString(2, "p2Value");
    verify(preparedStatement).setString(3, "c1Value");
    verify(preparedStatement).setString(4, "c2Value");
    verify(preparedStatement).setString(5, "v1Value");
    verify(preparedStatement).setString(6, "v2Value");
    verify(preparedStatement).setString(7, "v3Value");
    verify(preparedStatement).setString(8, "v4Value");
    verify(preparedStatement).setNull(9, Types.VARCHAR);
  }

  @ParameterizedTest
  @EnumSource(RdbEngine.class)
  public void updateQueryTest(RdbEngine rdbEngine) throws SQLException {
    QueryBuilder queryBuilder = new QueryBuilder(rdbEngine);

    UpdateQuery query;
    PreparedStatement preparedStatement;

    Map<String, Column<?>> columns = new HashMap<>();
    columns.put("v1", TextColumn.of("v1", "v1Value"));
    columns.put("v2", TextColumn.of("v2", "v2Value"));
    columns.put("v3", TextColumn.of("v3", "v3Value"));

    preparedStatement = mock(PreparedStatement.class);
    query =
        queryBuilder
            .update(NAMESPACE, TABLE, TABLE_METADATA)
            .set(columns)
            .where(new Key("p1", "p1Value"), Optional.empty())
            .build();
    assertThat(query.sql())
        .isEqualTo(encloseSql("UPDATE n1.t1 SET v1=?,v2=?,v3=? WHERE p1=?", rdbEngine));
    query.bind(preparedStatement);
    verify(preparedStatement).setString(1, "v1Value");
    verify(preparedStatement).setString(2, "v2Value");
    verify(preparedStatement).setString(3, "v3Value");
    verify(preparedStatement).setString(4, "p1Value");

    preparedStatement = mock(PreparedStatement.class);
    query =
        queryBuilder
            .update(NAMESPACE, TABLE, TABLE_METADATA)
            .set(columns)
            .where(new Key("p1", "p1Value"), Optional.of(new Key("c1", "c1Value")))
            .build();
    assertThat(query.sql())
        .isEqualTo(encloseSql("UPDATE n1.t1 SET v1=?,v2=?,v3=? WHERE p1=? AND c1=?", rdbEngine));
    query.bind(preparedStatement);
    verify(preparedStatement).setString(1, "v1Value");
    verify(preparedStatement).setString(2, "v2Value");
    verify(preparedStatement).setString(3, "v3Value");
    verify(preparedStatement).setString(4, "p1Value");
    verify(preparedStatement).setString(5, "c1Value");

    preparedStatement = mock(PreparedStatement.class);
    query =
        queryBuilder
            .update(NAMESPACE, TABLE, TABLE_METADATA)
            .set(columns)
            .where(
                new Key("p1", "p1Value", "p2", "p2Value"),
                Optional.of(new Key("c1", "c1Value", "c2", "c2Value")))
            .build();
    assertThat(query.sql())
        .isEqualTo(
            encloseSql(
                "UPDATE n1.t1 SET v1=?,v2=?,v3=? WHERE p1=? AND p2=? AND c1=? AND c2=?",
                rdbEngine));
    query.bind(preparedStatement);
    verify(preparedStatement).setString(1, "v1Value");
    verify(preparedStatement).setString(2, "v2Value");
    verify(preparedStatement).setString(3, "v3Value");
    verify(preparedStatement).setString(4, "p1Value");
    verify(preparedStatement).setString(5, "p2Value");
    verify(preparedStatement).setString(6, "c1Value");
    verify(preparedStatement).setString(7, "c2Value");

    preparedStatement = mock(PreparedStatement.class);
    query =
        queryBuilder
            .update(NAMESPACE, TABLE, TABLE_METADATA)
            .set(columns)
            .where(
                new Key("p1", "p1Value"),
                Optional.of(new Key("c1", "c1Value")),
                Collections.singletonList(
                    new ConditionalExpression(
                        "v1", new TextValue("v1ConditionValue"), Operator.EQ)))
            .build();
    assertThat(query.sql())
        .isEqualTo(
            encloseSql("UPDATE n1.t1 SET v1=?,v2=?,v3=? WHERE p1=? AND c1=? AND v1=?", rdbEngine));
    query.bind(preparedStatement);
    verify(preparedStatement).setString(1, "v1Value");
    verify(preparedStatement).setString(2, "v2Value");
    verify(preparedStatement).setString(3, "v3Value");
    verify(preparedStatement).setString(4, "p1Value");
    verify(preparedStatement).setString(5, "c1Value");
    verify(preparedStatement).setString(6, "v1ConditionValue");

    preparedStatement = mock(PreparedStatement.class);
    query =
        queryBuilder
            .update(NAMESPACE, TABLE, TABLE_METADATA)
            .set(columns)
            .where(
                new Key("p1", "p1Value"),
                Optional.of(new Key("c1", "c1Value")),
                Arrays.asList(
                    new ConditionalExpression("v1", new TextValue("v1ConditionValue"), Operator.NE),
                    new ConditionalExpression("v2", new TextValue("v2ConditionValue"), Operator.GT),
                    new ConditionalExpression(
                        "v3", new TextValue("v3ConditionValue"), Operator.LTE)))
            .build();
    assertThat(query.sql())
        .isEqualTo(
            encloseSql(
                "UPDATE n1.t1 SET v1=?,v2=?,v3=? WHERE p1=? AND c1=? AND v1<>? AND v2>? AND v3<=?",
                rdbEngine));
    query.bind(preparedStatement);
    verify(preparedStatement).setString(1, "v1Value");
    verify(preparedStatement).setString(2, "v2Value");
    verify(preparedStatement).setString(3, "v3Value");
    verify(preparedStatement).setString(4, "p1Value");
    verify(preparedStatement).setString(5, "c1Value");
    verify(preparedStatement).setString(6, "v1ConditionValue");
    verify(preparedStatement).setString(7, "v2ConditionValue");
    verify(preparedStatement).setString(8, "v3ConditionValue");

    preparedStatement = mock(PreparedStatement.class);
    query =
        queryBuilder
            .update(NAMESPACE, TABLE, TABLE_METADATA)
            .set(columns)
            .where(
                new Key("p1", "p1Value"),
                Optional.of(new Key("c1", "c1Value")),
                Arrays.asList(
                    ConditionBuilder.column("v1").isNullText(),
                    ConditionBuilder.column("v2").isNotNullText()))
            .build();
    assertThat(query.sql())
        .isEqualTo(
            encloseSql(
                "UPDATE n1.t1 SET v1=?,v2=?,v3=? WHERE p1=? AND c1=? AND v1 IS NULL AND v2 IS NOT NULL",
                rdbEngine));
    query.bind(preparedStatement);
    verify(preparedStatement).setString(1, "v1Value");
    verify(preparedStatement).setString(2, "v2Value");
    verify(preparedStatement).setString(3, "v3Value");
    verify(preparedStatement).setString(4, "p1Value");
    verify(preparedStatement).setString(5, "c1Value");

    columns.put("v4", TextColumn.ofNull("v4"));
    preparedStatement = mock(PreparedStatement.class);
    query =
        queryBuilder
            .update(NAMESPACE, TABLE, TABLE_METADATA)
            .set(columns)
            .where(new Key("p1", "p1Value"), Optional.of(new Key("c1", "c1Value")))
            .build();
    assertThat(query.sql())
        .isEqualTo(
            encloseSql("UPDATE n1.t1 SET v1=?,v2=?,v3=?,v4=? WHERE p1=? AND c1=?", rdbEngine));
    query.bind(preparedStatement);
    verify(preparedStatement).setString(1, "v1Value");
    verify(preparedStatement).setString(2, "v2Value");
    verify(preparedStatement).setString(3, "v3Value");
    verify(preparedStatement).setNull(4, Types.VARCHAR);
    verify(preparedStatement).setString(5, "p1Value");
    verify(preparedStatement).setString(6, "c1Value");
  }

  @ParameterizedTest
  @EnumSource(RdbEngine.class)
  public void deleteQueryTest(RdbEngine rdbEngine) throws SQLException {
    QueryBuilder queryBuilder = new QueryBuilder(rdbEngine);

    DeleteQuery query;
    PreparedStatement preparedStatement;

    preparedStatement = mock(PreparedStatement.class);
    query =
        queryBuilder
            .deleteFrom(NAMESPACE, TABLE, TABLE_METADATA)
            .where(new Key("p1", "p1Value"), Optional.empty())
            .build();
    assertThat(query.sql()).isEqualTo(encloseSql("DELETE FROM n1.t1 WHERE p1=?", rdbEngine));
    query.bind(preparedStatement);
    verify(preparedStatement).setString(1, "p1Value");

    preparedStatement = mock(PreparedStatement.class);
    query =
        queryBuilder
            .deleteFrom(NAMESPACE, TABLE, TABLE_METADATA)
            .where(new Key("p1", "p1Value"), Optional.of(new Key("c1", "c1Value")))
            .build();
    assertThat(query.sql())
        .isEqualTo(encloseSql("DELETE FROM n1.t1 WHERE p1=? AND c1=?", rdbEngine));
    query.bind(preparedStatement);
    verify(preparedStatement).setString(1, "p1Value");
    verify(preparedStatement).setString(2, "c1Value");

    preparedStatement = mock(PreparedStatement.class);
    query =
        queryBuilder
            .deleteFrom(NAMESPACE, TABLE, TABLE_METADATA)
            .where(
                new Key("p1", "p1Value", "p2", "p2Value"),
                Optional.of(new Key("c1", "c1Value", "c2", "c2Value")))
            .build();
    assertThat(query.sql())
        .isEqualTo(
            encloseSql("DELETE FROM n1.t1 WHERE p1=? AND p2=? AND c1=? AND c2=?", rdbEngine));
    query.bind(preparedStatement);
    verify(preparedStatement).setString(1, "p1Value");
    verify(preparedStatement).setString(2, "p2Value");
    verify(preparedStatement).setString(3, "c1Value");
    verify(preparedStatement).setString(4, "c2Value");

    preparedStatement = mock(PreparedStatement.class);
    query =
        queryBuilder
            .deleteFrom(NAMESPACE, TABLE, TABLE_METADATA)
            .where(
                new Key("p1", "p1Value"),
                Optional.of(new Key("c1", "c1Value")),
                Collections.singletonList(
                    new ConditionalExpression(
                        "v1", new TextValue("v1ConditionValue"), Operator.EQ)))
            .build();
    assertThat(query.sql())
        .isEqualTo(encloseSql("DELETE FROM n1.t1 WHERE p1=? AND c1=? AND v1=?", rdbEngine));
    query.bind(preparedStatement);
    verify(preparedStatement).setString(1, "p1Value");
    verify(preparedStatement).setString(2, "c1Value");
    verify(preparedStatement).setString(3, "v1ConditionValue");

    preparedStatement = mock(PreparedStatement.class);
    query =
        queryBuilder
            .deleteFrom(NAMESPACE, TABLE, TABLE_METADATA)
            .where(
                new Key("p1", "p1Value"),
                Optional.of(new Key("c1", "c1Value")),
                Arrays.asList(
                    new ConditionalExpression("v1", new TextValue("v1ConditionValue"), Operator.NE),
                    new ConditionalExpression(
                        "v2", new TextValue("v2ConditionValue"), Operator.GTE),
                    new ConditionalExpression(
                        "v3", new TextValue("v3ConditionValue"), Operator.LT)))
            .build();
    assertThat(query.sql())
        .isEqualTo(
            encloseSql(
                "DELETE FROM n1.t1 WHERE p1=? AND c1=? AND v1<>? AND v2>=? AND v3<?", rdbEngine));
    query.bind(preparedStatement);
    verify(preparedStatement).setString(1, "p1Value");
    verify(preparedStatement).setString(2, "c1Value");
    verify(preparedStatement).setString(3, "v1ConditionValue");
    verify(preparedStatement).setString(4, "v2ConditionValue");
    verify(preparedStatement).setString(5, "v3ConditionValue");

    preparedStatement = mock(PreparedStatement.class);
    query =
        queryBuilder
            .deleteFrom(NAMESPACE, TABLE, TABLE_METADATA)
            .where(
                new Key("p1", "p1Value"),
                Optional.of(new Key("c1", "c1Value")),
                Arrays.asList(
                    ConditionBuilder.column("v1").isNullText(),
                    ConditionBuilder.column("v2").isNotNullText()))
            .build();
    assertThat(query.sql())
        .isEqualTo(
            encloseSql(
                "DELETE FROM n1.t1 WHERE p1=? AND c1=? AND v1 IS NULL AND v2 IS NOT NULL",
                rdbEngine));
    query.bind(preparedStatement);
    verify(preparedStatement).setString(1, "p1Value");
    verify(preparedStatement).setString(2, "c1Value");
  }

  @ParameterizedTest
  @EnumSource(RdbEngine.class)
  public void upsertQueryTest(RdbEngine rdbEngine) throws SQLException {
    QueryBuilder queryBuilder = new QueryBuilder(rdbEngine);

    String expectedQuery;
    UpsertQuery query;
    PreparedStatement preparedStatement;

    Map<String, Column<?>> columns = new HashMap<>();
    columns.put("v1", TextColumn.of("v1", "v1Value"));
    columns.put("v2", TextColumn.of("v2", "v2Value"));
    columns.put("v3", TextColumn.of("v3", "v3Value"));

    preparedStatement = mock(PreparedStatement.class);
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
    query =
        queryBuilder
            .upsertInto(NAMESPACE, TABLE, TABLE_METADATA)
            .values(new Key("p1", "p1Value"), Optional.empty(), columns)
            .build();
    assertThat(query.sql()).isEqualTo(encloseSql(expectedQuery, rdbEngine));
    query.bind(preparedStatement);
    switch (rdbEngine) {
      case MYSQL:
      case POSTGRESQL:
        verify(preparedStatement).setString(1, "p1Value");
        verify(preparedStatement).setString(2, "v1Value");
        verify(preparedStatement).setString(3, "v2Value");
        verify(preparedStatement).setString(4, "v3Value");
        verify(preparedStatement).setString(5, "v1Value");
        verify(preparedStatement).setString(6, "v2Value");
        verify(preparedStatement).setString(7, "v3Value");
        break;
      case ORACLE:
      case SQL_SERVER:
        verify(preparedStatement).setString(1, "p1Value");
        verify(preparedStatement).setString(2, "v1Value");
        verify(preparedStatement).setString(3, "v2Value");
        verify(preparedStatement).setString(4, "v3Value");
        verify(preparedStatement).setString(5, "p1Value");
        verify(preparedStatement).setString(6, "v1Value");
        verify(preparedStatement).setString(7, "v2Value");
        verify(preparedStatement).setString(8, "v3Value");
        break;
    }

    preparedStatement = mock(PreparedStatement.class);
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
    query =
        queryBuilder
            .upsertInto(NAMESPACE, TABLE, TABLE_METADATA)
            .values(new Key("p1", "p1Value"), Optional.of(new Key("c1", "c1Value")), columns)
            .build();
    assertThat(query.sql()).isEqualTo(encloseSql(expectedQuery, rdbEngine));
    query.bind(preparedStatement);
    switch (rdbEngine) {
      case MYSQL:
      case POSTGRESQL:
        verify(preparedStatement).setString(1, "p1Value");
        verify(preparedStatement).setString(2, "c1Value");
        verify(preparedStatement).setString(3, "v1Value");
        verify(preparedStatement).setString(4, "v2Value");
        verify(preparedStatement).setString(5, "v3Value");
        verify(preparedStatement).setString(6, "v1Value");
        verify(preparedStatement).setString(7, "v2Value");
        verify(preparedStatement).setString(8, "v3Value");
        break;
      case ORACLE:
      case SQL_SERVER:
        verify(preparedStatement).setString(1, "p1Value");
        verify(preparedStatement).setString(2, "c1Value");
        verify(preparedStatement).setString(3, "v1Value");
        verify(preparedStatement).setString(4, "v2Value");
        verify(preparedStatement).setString(5, "v3Value");
        verify(preparedStatement).setString(6, "p1Value");
        verify(preparedStatement).setString(7, "c1Value");
        verify(preparedStatement).setString(8, "v1Value");
        verify(preparedStatement).setString(9, "v2Value");
        verify(preparedStatement).setString(10, "v3Value");
        break;
    }

    columns.put("v4", TextColumn.of("v4", "v4Value"));
    preparedStatement = mock(PreparedStatement.class);
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
    query =
        queryBuilder
            .upsertInto(NAMESPACE, TABLE, TABLE_METADATA)
            .values(
                new Key("p1", "p1Value", "p2", "p2Value"),
                Optional.of(new Key("c1", "c1Value", "c2", "c2Value")),
                columns)
            .build();
    assertThat(query.sql()).isEqualTo(encloseSql(expectedQuery, rdbEngine));
    query.bind(preparedStatement);
    switch (rdbEngine) {
      case MYSQL:
      case POSTGRESQL:
        verify(preparedStatement).setString(1, "p1Value");
        verify(preparedStatement).setString(2, "p2Value");
        verify(preparedStatement).setString(3, "c1Value");
        verify(preparedStatement).setString(4, "c2Value");
        verify(preparedStatement).setString(5, "v1Value");
        verify(preparedStatement).setString(6, "v2Value");
        verify(preparedStatement).setString(7, "v3Value");
        verify(preparedStatement).setString(8, "v4Value");
        verify(preparedStatement).setString(9, "v1Value");
        verify(preparedStatement).setString(10, "v2Value");
        verify(preparedStatement).setString(11, "v3Value");
        verify(preparedStatement).setString(12, "v4Value");
        break;
      case ORACLE:
      case SQL_SERVER:
        verify(preparedStatement).setString(1, "p1Value");
        verify(preparedStatement).setString(2, "p2Value");
        verify(preparedStatement).setString(3, "c1Value");
        verify(preparedStatement).setString(4, "c2Value");
        verify(preparedStatement).setString(5, "v1Value");
        verify(preparedStatement).setString(6, "v2Value");
        verify(preparedStatement).setString(7, "v3Value");
        verify(preparedStatement).setString(8, "v4Value");
        verify(preparedStatement).setString(9, "p1Value");
        verify(preparedStatement).setString(10, "p2Value");
        verify(preparedStatement).setString(11, "c1Value");
        verify(preparedStatement).setString(12, "c2Value");
        verify(preparedStatement).setString(13, "v1Value");
        verify(preparedStatement).setString(14, "v2Value");
        verify(preparedStatement).setString(15, "v3Value");
        verify(preparedStatement).setString(16, "v4Value");
        break;
    }

    columns.put("v5", TextColumn.ofNull("v5"));
    preparedStatement = mock(PreparedStatement.class);
    switch (rdbEngine) {
      case MYSQL:
        expectedQuery =
            "INSERT INTO n1.t1 (p1,p2,c1,c2,v1,v2,v3,v4,v5) VALUES (?,?,?,?,?,?,?,?,?)"
                + " ON DUPLICATE KEY UPDATE v1=?,v2=?,v3=?,v4=?,v5=?";
        break;
      case POSTGRESQL:
        expectedQuery =
            "INSERT INTO n1.t1 (p1,p2,c1,c2,v1,v2,v3,v4,v5) VALUES (?,?,?,?,?,?,?,?,?) "
                + "ON CONFLICT (p1,p2,c1,c2) DO UPDATE SET v1=?,v2=?,v3=?,v4=?,v5=?";
        break;
      case ORACLE:
        expectedQuery =
            "MERGE INTO n1.t1 t1 USING (SELECT ? p1,? p2,? c1,? c2 FROM DUAL) t2 "
                + "ON (t1.p1=t2.p1 AND t1.p2=t2.p2 AND t1.c1=t2.c1 AND t1.c2=t2.c2) "
                + "WHEN MATCHED THEN UPDATE SET v1=?,v2=?,v3=?,v4=?,v5=? "
                + "WHEN NOT MATCHED THEN INSERT (p1,p2,c1,c2,v1,v2,v3,v4,v5) "
                + "VALUES (?,?,?,?,?,?,?,?,?)";
        break;
      case SQL_SERVER:
      default:
        expectedQuery =
            "MERGE n1.t1 t1 USING (SELECT ? p1,? p2,? c1,? c2) t2 "
                + "ON (t1.p1=t2.p1 AND t1.p2=t2.p2 AND t1.c1=t2.c1 AND t1.c2=t2.c2) "
                + "WHEN MATCHED THEN UPDATE SET v1=?,v2=?,v3=?,v4=?,v5=? "
                + "WHEN NOT MATCHED THEN INSERT (p1,p2,c1,c2,v1,v2,v3,v4,v5) "
                + "VALUES (?,?,?,?,?,?,?,?,?);";
        break;
    }
    query =
        queryBuilder
            .upsertInto(NAMESPACE, TABLE, TABLE_METADATA)
            .values(
                new Key("p1", "p1Value", "p2", "p2Value"),
                Optional.of(new Key("c1", "c1Value", "c2", "c2Value")),
                columns)
            .build();
    assertThat(query.sql()).isEqualTo(encloseSql(expectedQuery, rdbEngine));
    query.bind(preparedStatement);
    switch (rdbEngine) {
      case MYSQL:
      case POSTGRESQL:
        verify(preparedStatement).setString(1, "p1Value");
        verify(preparedStatement).setString(2, "p2Value");
        verify(preparedStatement).setString(3, "c1Value");
        verify(preparedStatement).setString(4, "c2Value");
        verify(preparedStatement).setString(5, "v1Value");
        verify(preparedStatement).setString(6, "v2Value");
        verify(preparedStatement).setString(7, "v3Value");
        verify(preparedStatement).setString(8, "v4Value");
        verify(preparedStatement).setNull(9, Types.VARCHAR);
        verify(preparedStatement).setString(10, "v1Value");
        verify(preparedStatement).setString(11, "v2Value");
        verify(preparedStatement).setString(12, "v3Value");
        verify(preparedStatement).setString(13, "v4Value");
        verify(preparedStatement).setNull(14, Types.VARCHAR);
        break;
      case ORACLE:
      case SQL_SERVER:
        verify(preparedStatement).setString(1, "p1Value");
        verify(preparedStatement).setString(2, "p2Value");
        verify(preparedStatement).setString(3, "c1Value");
        verify(preparedStatement).setString(4, "c2Value");
        verify(preparedStatement).setString(5, "v1Value");
        verify(preparedStatement).setString(6, "v2Value");
        verify(preparedStatement).setString(7, "v3Value");
        verify(preparedStatement).setString(8, "v4Value");
        verify(preparedStatement).setNull(9, Types.VARCHAR);
        verify(preparedStatement).setString(10, "p1Value");
        verify(preparedStatement).setString(11, "p2Value");
        verify(preparedStatement).setString(12, "c1Value");
        verify(preparedStatement).setString(13, "c2Value");
        verify(preparedStatement).setString(14, "v1Value");
        verify(preparedStatement).setString(15, "v2Value");
        verify(preparedStatement).setString(16, "v3Value");
        verify(preparedStatement).setString(17, "v4Value");
        verify(preparedStatement).setNull(18, Types.VARCHAR);
        break;
    }
  }

  @ParameterizedTest
  @EnumSource(RdbEngine.class)
  public void upsertQueryWithoutValuesTest(RdbEngine rdbEngine) throws SQLException {
    QueryBuilder queryBuilder = new QueryBuilder(rdbEngine);

    String expectedQuery;
    UpsertQuery query;
    PreparedStatement preparedStatement;

    preparedStatement = mock(PreparedStatement.class);
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
    query =
        queryBuilder
            .upsertInto(NAMESPACE, TABLE, TABLE_METADATA)
            .values(new Key("p1", "p1Value"), Optional.empty(), Collections.emptyMap())
            .build();
    assertThat(query.sql()).isEqualTo(encloseSql(expectedQuery, rdbEngine));
    query.bind(preparedStatement);
    switch (rdbEngine) {
      case MYSQL:
      case POSTGRESQL:
        verify(preparedStatement).setString(1, "p1Value");
        break;
      case ORACLE:
      case SQL_SERVER:
        verify(preparedStatement).setString(1, "p1Value");
        verify(preparedStatement).setString(2, "p1Value");
        break;
    }

    preparedStatement = mock(PreparedStatement.class);
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
    query =
        queryBuilder
            .upsertInto(NAMESPACE, TABLE, TABLE_METADATA)
            .values(
                new Key("p1", "p1Value"),
                Optional.of(new Key("c1", "c1Value")),
                Collections.emptyMap())
            .build();
    assertThat(query.sql()).isEqualTo(encloseSql(expectedQuery, rdbEngine));
    query.bind(preparedStatement);
    switch (rdbEngine) {
      case MYSQL:
      case POSTGRESQL:
        verify(preparedStatement).setString(1, "p1Value");
        verify(preparedStatement).setString(2, "c1Value");
        break;
      case ORACLE:
      case SQL_SERVER:
        verify(preparedStatement).setString(1, "p1Value");
        verify(preparedStatement).setString(2, "c1Value");
        verify(preparedStatement).setString(3, "p1Value");
        verify(preparedStatement).setString(4, "c1Value");
        break;
    }

    preparedStatement = mock(PreparedStatement.class);
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
    query =
        queryBuilder
            .upsertInto(NAMESPACE, TABLE, TABLE_METADATA)
            .values(
                new Key("p1", "p1Value", "p2", "p2Value"),
                Optional.of(new Key("c1", "c1Value", "c2", "c2Value")),
                Collections.emptyMap())
            .build();
    assertThat(query.sql()).isEqualTo(encloseSql(expectedQuery, rdbEngine));
    query.bind(preparedStatement);
    switch (rdbEngine) {
      case MYSQL:
      case POSTGRESQL:
        verify(preparedStatement).setString(1, "p1Value");
        verify(preparedStatement).setString(2, "p2Value");
        verify(preparedStatement).setString(3, "c1Value");
        verify(preparedStatement).setString(4, "c2Value");
        break;
      case ORACLE:
      case SQL_SERVER:
        verify(preparedStatement).setString(1, "p1Value");
        verify(preparedStatement).setString(2, "p2Value");
        verify(preparedStatement).setString(3, "c1Value");
        verify(preparedStatement).setString(4, "c2Value");
        verify(preparedStatement).setString(5, "p1Value");
        verify(preparedStatement).setString(6, "p2Value");
        verify(preparedStatement).setString(7, "c1Value");
        verify(preparedStatement).setString(8, "c2Value");
        break;
    }
  }

  private String encloseSql(String sql, RdbEngine rdbEngine) {
    return sql.replace("n1.t1", enclose("n1", rdbEngine) + "." + enclose("t1", rdbEngine))
        .replace("p1", enclose("p1", rdbEngine))
        .replace("p2", enclose("p2", rdbEngine))
        .replace("c1", enclose("c1", rdbEngine))
        .replace("c2", enclose("c2", rdbEngine))
        .replace("v1", enclose("v1", rdbEngine))
        .replace("v2", enclose("v2", rdbEngine))
        .replace("v3", enclose("v3", rdbEngine))
        .replace("v4", enclose("v4", rdbEngine))
        .replace("v5", enclose("v5", rdbEngine));
  }
}
