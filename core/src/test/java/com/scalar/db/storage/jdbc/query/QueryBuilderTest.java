package com.scalar.db.storage.jdbc.query;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableSet;
import com.scalar.db.api.ConditionBuilder;
import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.ConditionalExpression.Operator;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Selection.Conjunction;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import com.scalar.db.io.TextValue;
import com.scalar.db.storage.jdbc.RdbEngine;
import com.scalar.db.storage.jdbc.RdbEngineStrategy;
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

  private static final TableMetadata CROSS_PARTITION_TABLE_METADATA =
      TableMetadata.newBuilder()
          .addColumn("p1", DataType.TEXT)
          .addColumn("p2", DataType.INT)
          .addColumn("v1", DataType.INT)
          .addColumn("v2", DataType.INT)
          .addColumn("v3", DataType.INT)
          .addColumn("v4", DataType.INT)
          .addColumn("v5", DataType.INT)
          .addPartitionKey("p1")
          .addPartitionKey("p2")
          .build();

  @ParameterizedTest
  @EnumSource(RdbEngine.class)
  public void selectQueryTest(RdbEngine rdbEngineType) throws SQLException {
    RdbEngineStrategy rdbEngine = RdbEngine.createRdbEngineStrategy(rdbEngineType);
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

    String expectedQuery = "";
    switch (rdbEngineType) {
      case MYSQL:
      case POSTGRESQL:
      case YUGABYTE:
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
        expectedQuery =
            "SELECT TOP 10 c1,c2 FROM n1.t1 WHERE p1=? AND c1>=? AND c1<=? "
                + "ORDER BY c1 ASC,c2 DESC";
        break;
      case SQLITE:
        expectedQuery =
            "SELECT c1,c2 FROM \"n1$t1\" WHERE p1=? AND c1>=? AND c1<=? "
                + "ORDER BY c1 ASC,c2 DESC LIMIT 10";
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
  public void selectQueryWithConjunctionsTest(RdbEngine rdbEngineType) throws SQLException {
    RdbEngineStrategy rdbEngine = RdbEngine.createRdbEngineStrategy(rdbEngineType);
    QueryBuilder queryBuilder = new QueryBuilder(rdbEngine);

    SelectQuery query;
    PreparedStatement preparedStatement;

    query =
        queryBuilder
            .select(Collections.emptyList())
            .from(NAMESPACE, TABLE, CROSS_PARTITION_TABLE_METADATA)
            .where(ImmutableSet.of())
            .build();
    assertThat(query.sql()).isEqualTo(encloseSql("SELECT * FROM n1.t1", rdbEngine));

    query =
        queryBuilder
            .select(Collections.emptyList())
            .from(NAMESPACE, TABLE, CROSS_PARTITION_TABLE_METADATA)
            .where(ImmutableSet.of())
            .orderBy(
                Arrays.asList(
                    new Scan.Ordering("v1", Scan.Ordering.Order.ASC),
                    new Scan.Ordering("v2", Scan.Ordering.Order.DESC)))
            .build();
    assertThat(query.sql())
        .isEqualTo(encloseSql("SELECT * FROM n1.t1 ORDER BY v1 ASC,v2 DESC", rdbEngine));

    query =
        queryBuilder
            .select(Arrays.asList("v1", "v2"))
            .from(NAMESPACE, TABLE, CROSS_PARTITION_TABLE_METADATA)
            .where(ImmutableSet.of())
            .build();
    assertThat(query.sql()).isEqualTo(encloseSql("SELECT v1,v2 FROM n1.t1", rdbEngine));

    preparedStatement = mock(PreparedStatement.class);
    query =
        queryBuilder
            .select(Collections.emptyList())
            .from(NAMESPACE, TABLE, CROSS_PARTITION_TABLE_METADATA)
            .where(ImmutableSet.of(Conjunction.of(ConditionBuilder.column("v1").isEqualToInt(1))))
            .build();
    assertThat(query.sql()).isEqualTo(encloseSql("SELECT * FROM n1.t1 WHERE v1=?", rdbEngine));
    query.bind(preparedStatement);
    verify(preparedStatement).setInt(1, 1);

    preparedStatement = mock(PreparedStatement.class);
    query =
        queryBuilder
            .select(Collections.emptyList())
            .from(NAMESPACE, TABLE, CROSS_PARTITION_TABLE_METADATA)
            .where(
                ImmutableSet.of(
                    Conjunction.of(
                        ConditionBuilder.column("v1").isEqualToInt(1),
                        ConditionBuilder.column("v2").isEqualToInt(2))))
            .build();
    assertThat(query.sql())
        .isEqualTo(encloseSql("SELECT * FROM n1.t1 WHERE v1=? AND v2=?", rdbEngine));
    query.bind(preparedStatement);
    verify(preparedStatement).setInt(1, 1);
    verify(preparedStatement).setInt(2, 2);

    preparedStatement = mock(PreparedStatement.class);
    query =
        queryBuilder
            .select(Collections.emptyList())
            .from(NAMESPACE, TABLE, CROSS_PARTITION_TABLE_METADATA)
            .where(
                ImmutableSet.of(
                    Conjunction.of(ConditionBuilder.column("v1").isEqualToInt(1)),
                    Conjunction.of(ConditionBuilder.column("v2").isEqualToInt(2))))
            .build();
    assertThat(query.sql())
        .isEqualTo(encloseSql("SELECT * FROM n1.t1 WHERE v1=? OR v2=?", rdbEngine));
    query.bind(preparedStatement);
    verify(preparedStatement).setInt(1, 1);
    verify(preparedStatement).setInt(2, 2);

    preparedStatement = mock(PreparedStatement.class);
    query =
        queryBuilder
            .select(Collections.emptyList())
            .from(NAMESPACE, TABLE, CROSS_PARTITION_TABLE_METADATA)
            .where(
                ImmutableSet.of(
                    Conjunction.of(
                        ConditionBuilder.column("v1").isEqualToInt(1),
                        ConditionBuilder.column("v2").isEqualToInt(2)),
                    Conjunction.of(
                        ConditionBuilder.column("v3").isEqualToInt(3),
                        ConditionBuilder.column("v4").isEqualToInt(4))))
            .build();
    assertThat(query.sql())
        .isEqualTo(
            encloseSql("SELECT * FROM n1.t1 WHERE v1=? AND v2=? OR v3=? AND v4=?", rdbEngine));
    query.bind(preparedStatement);
    verify(preparedStatement).setInt(1, 1);
    verify(preparedStatement).setInt(2, 2);
    verify(preparedStatement).setInt(3, 3);
    verify(preparedStatement).setInt(4, 4);

    preparedStatement = mock(PreparedStatement.class);
    query =
        queryBuilder
            .select(Collections.emptyList())
            .from(NAMESPACE, TABLE, CROSS_PARTITION_TABLE_METADATA)
            .where(
                ImmutableSet.of(
                    Conjunction.of(
                        ConditionBuilder.column("v1").isGreaterThanInt(1),
                        ConditionBuilder.column("v1").isLessThanInt(2),
                        ConditionBuilder.column("v2").isGreaterThanOrEqualToInt(3),
                        ConditionBuilder.column("v2").isLessThanOrEqualToInt(4),
                        ConditionBuilder.column("v2").isNotEqualToInt(5))))
            .build();
    assertThat(query.sql())
        .isEqualTo(
            encloseSql(
                "SELECT * FROM n1.t1 WHERE v1>? AND v1<? AND v2>=? AND v2<=? AND v2!=?",
                rdbEngine));
    query.bind(preparedStatement);
    verify(preparedStatement).setInt(1, 1);
    verify(preparedStatement).setInt(2, 2);
    verify(preparedStatement).setInt(3, 3);
    verify(preparedStatement).setInt(4, 4);
    verify(preparedStatement).setInt(5, 5);

    query =
        queryBuilder
            .select(Collections.emptyList())
            .from(NAMESPACE, TABLE, CROSS_PARTITION_TABLE_METADATA)
            .where(
                ImmutableSet.of(
                    Conjunction.of(
                        ConditionBuilder.column("v1").isNullInt(),
                        ConditionBuilder.column("v2").isNotNullInt())))
            .build();
    assertThat(query.sql())
        .isEqualTo(
            encloseSql("SELECT * FROM n1.t1 WHERE v1 IS NULL AND v2 IS NOT NULL", rdbEngine));
  }

  @ParameterizedTest
  @EnumSource(
      value = RdbEngine.class,
      names = {"MYSQL"},
      mode = EnumSource.Mode.INCLUDE)
  public void selectQueryWithLikeConditionsTestForMySql(RdbEngine rdbEngineType)
      throws SQLException {
    RdbEngineStrategy rdbEngine = RdbEngine.createRdbEngineStrategy(rdbEngineType);
    QueryBuilder queryBuilder = new QueryBuilder(rdbEngine);

    SelectQuery query;
    PreparedStatement preparedStatement;

    preparedStatement = mock(PreparedStatement.class);
    query =
        queryBuilder
            .select(Collections.emptyList())
            .from(NAMESPACE, TABLE, CROSS_PARTITION_TABLE_METADATA)
            .where(
                ImmutableSet.of(
                    Conjunction.of(
                        ConditionBuilder.column("c1").isLikeText("%text"),
                        ConditionBuilder.column("c2").isLikeText("text+%%", "+"),
                        ConditionBuilder.column("v1").isLikeText("text\\%", ""),
                        ConditionBuilder.column("v2").isNotLikeText("%text"),
                        ConditionBuilder.column("v3").isNotLikeText("text+%%", "+"),
                        ConditionBuilder.column("v4").isNotLikeText("text\\%", ""))))
            .build();
    assertThat(query.sql())
        .isEqualTo(
            encloseSql(
                "SELECT * FROM n1.t1 WHERE"
                    + " c1 LIKE ? ESCAPE ? AND c2 LIKE ? ESCAPE ? AND v1 LIKE ? ESCAPE ?"
                    + " AND v2 NOT LIKE ? ESCAPE ? AND v3 NOT LIKE ? ESCAPE ? AND v4 NOT LIKE ? ESCAPE ?",
                rdbEngine));
    query.bind(preparedStatement);
    verify(preparedStatement).setString(1, "%text");
    verify(preparedStatement).setString(2, "\\");
    verify(preparedStatement).setString(3, "text+%%");
    verify(preparedStatement).setString(4, "+");
    verify(preparedStatement).setString(5, "text\\\\%");
    verify(preparedStatement).setString(6, "\\");
    verify(preparedStatement).setString(7, "%text");
    verify(preparedStatement).setString(8, "\\");
    verify(preparedStatement).setString(9, "text+%%");
    verify(preparedStatement).setString(10, "+");
    verify(preparedStatement).setString(11, "text\\\\%");
    verify(preparedStatement).setString(12, "\\");
  }

  @ParameterizedTest
  @EnumSource(
      value = RdbEngine.class,
      names = {"POSTGRESQL", "YUGABYTE"},
      mode = EnumSource.Mode.INCLUDE)
  public void selectQueryWithLikeConditionsTestForPostgres(RdbEngine rdbEngineType)
      throws SQLException {
    RdbEngineStrategy rdbEngine = RdbEngine.createRdbEngineStrategy(rdbEngineType);
    QueryBuilder queryBuilder = new QueryBuilder(rdbEngine);

    SelectQuery query;
    PreparedStatement preparedStatement;

    preparedStatement = mock(PreparedStatement.class);
    query =
        queryBuilder
            .select(Collections.emptyList())
            .from(NAMESPACE, TABLE, CROSS_PARTITION_TABLE_METADATA)
            .where(
                ImmutableSet.of(
                    Conjunction.of(
                        ConditionBuilder.column("c1").isLikeText("%text"),
                        ConditionBuilder.column("c2").isLikeText("text+%%", "+"),
                        ConditionBuilder.column("v1").isLikeText("text\\%", ""),
                        ConditionBuilder.column("v2").isNotLikeText("%text"),
                        ConditionBuilder.column("v3").isNotLikeText("text+%%", "+"),
                        ConditionBuilder.column("v4").isNotLikeText("text\\%", ""))))
            .build();
    assertThat(query.sql())
        .isEqualTo(
            encloseSql(
                "SELECT * FROM n1.t1 WHERE"
                    + " c1 LIKE ? ESCAPE ? AND c2 LIKE ? ESCAPE ? AND v1 LIKE ? ESCAPE ?"
                    + " AND v2 NOT LIKE ? ESCAPE ? AND v3 NOT LIKE ? ESCAPE ? AND v4 NOT LIKE ? ESCAPE ?",
                rdbEngine));
    query.bind(preparedStatement);
    verify(preparedStatement).setString(1, "%text");
    verify(preparedStatement).setString(2, "\\");
    verify(preparedStatement).setString(3, "text+%%");
    verify(preparedStatement).setString(4, "+");
    verify(preparedStatement).setString(5, "text\\%");
    verify(preparedStatement).setString(6, "");
    verify(preparedStatement).setString(7, "%text");
    verify(preparedStatement).setString(8, "\\");
    verify(preparedStatement).setString(9, "text+%%");
    verify(preparedStatement).setString(10, "+");
    verify(preparedStatement).setString(11, "text\\%");
    verify(preparedStatement).setString(12, "");
  }

  @ParameterizedTest
  @EnumSource(
      value = RdbEngine.class,
      names = {"ORACLE", "SQLITE"},
      mode = EnumSource.Mode.INCLUDE)
  public void selectQueryWithLikeConditionsTestForOracleAndSqlLite(RdbEngine rdbEngineType)
      throws SQLException {
    RdbEngineStrategy rdbEngine = RdbEngine.createRdbEngineStrategy(rdbEngineType);
    QueryBuilder queryBuilder = new QueryBuilder(rdbEngine);

    SelectQuery query;
    PreparedStatement preparedStatement;

    preparedStatement = mock(PreparedStatement.class);
    query =
        queryBuilder
            .select(Collections.emptyList())
            .from(NAMESPACE, TABLE, CROSS_PARTITION_TABLE_METADATA)
            .where(
                ImmutableSet.of(
                    Conjunction.of(
                        ConditionBuilder.column("c1").isLikeText("%text"),
                        ConditionBuilder.column("c2").isLikeText("text+%%", "+"),
                        ConditionBuilder.column("v1").isLikeText("text\\%", ""),
                        ConditionBuilder.column("v2").isNotLikeText("%text"),
                        ConditionBuilder.column("v3").isNotLikeText("text+%%", "+"),
                        ConditionBuilder.column("v4").isNotLikeText("text\\%", ""))))
            .build();
    assertThat(query.sql())
        .isEqualTo(
            encloseSql(
                "SELECT * FROM n1.t1 WHERE"
                    + " c1 LIKE ? ESCAPE ? AND c2 LIKE ? ESCAPE ? AND v1 LIKE ?"
                    + " AND v2 NOT LIKE ? ESCAPE ? AND v3 NOT LIKE ? ESCAPE ? AND v4 NOT LIKE ?",
                rdbEngine));
    query.bind(preparedStatement);
    verify(preparedStatement).setString(1, "%text");
    verify(preparedStatement).setString(2, "\\");
    verify(preparedStatement).setString(3, "text+%%");
    verify(preparedStatement).setString(4, "+");
    verify(preparedStatement).setString(5, "text\\%");
    verify(preparedStatement).setString(6, "%text");
    verify(preparedStatement).setString(7, "\\");
    verify(preparedStatement).setString(8, "text+%%");
    verify(preparedStatement).setString(9, "+");
    verify(preparedStatement).setString(10, "text\\%");
  }

  @ParameterizedTest
  @EnumSource(
      value = RdbEngine.class,
      names = {"SQL_SERVER"},
      mode = EnumSource.Mode.INCLUDE)
  public void selectQueryWithLikeConditionsTestForSQLServer(RdbEngine rdbEngineType)
      throws SQLException {
    RdbEngineStrategy rdbEngine = RdbEngine.createRdbEngineStrategy(rdbEngineType);
    QueryBuilder queryBuilder = new QueryBuilder(rdbEngine);

    SelectQuery query;
    PreparedStatement preparedStatement;

    preparedStatement = mock(PreparedStatement.class);
    query =
        queryBuilder
            .select(Collections.emptyList())
            .from(NAMESPACE, TABLE, CROSS_PARTITION_TABLE_METADATA)
            .where(
                ImmutableSet.of(
                    Conjunction.of(
                        ConditionBuilder.column("c1").isLikeText("%text[%]"),
                        ConditionBuilder.column("c2").isLikeText("[%]text+%%", "+"),
                        ConditionBuilder.column("v1").isLikeText("[%]text\\%", ""),
                        ConditionBuilder.column("v2").isNotLikeText("%text[%]"),
                        ConditionBuilder.column("v3").isNotLikeText("[%]text+%%", "+"),
                        ConditionBuilder.column("v4").isNotLikeText("[%]text\\%", ""))))
            .build();
    assertThat(query.sql())
        .isEqualTo(
            encloseSql(
                "SELECT * FROM n1.t1 WHERE"
                    + " c1 LIKE ? ESCAPE ? AND c2 LIKE ? ESCAPE ? AND v1 LIKE ? ESCAPE ?"
                    + " AND v2 NOT LIKE ? ESCAPE ? AND v3 NOT LIKE ? ESCAPE ? AND v4 NOT LIKE ? ESCAPE ?",
                rdbEngine));
    query.bind(preparedStatement);
    verify(preparedStatement).setString(1, "%text\\[%\\]");
    verify(preparedStatement).setString(2, "\\");
    verify(preparedStatement).setString(3, "+[%+]text+%%");
    verify(preparedStatement).setString(4, "+");
    verify(preparedStatement).setString(5, "\\[%\\]text\\\\%");
    verify(preparedStatement).setString(6, "\\");
    verify(preparedStatement).setString(7, "%text\\[%\\]");
    verify(preparedStatement).setString(8, "\\");
    verify(preparedStatement).setString(9, "+[%+]text+%%");
    verify(preparedStatement).setString(10, "+");
    verify(preparedStatement).setString(11, "\\[%\\]text\\\\%");
    verify(preparedStatement).setString(12, "\\");
  }

  @ParameterizedTest
  @EnumSource(RdbEngine.class)
  public void selectQueryWithIndexedColumnTest(RdbEngine rdbEngineType) throws SQLException {
    RdbEngineStrategy rdbEngine = RdbEngine.createRdbEngineStrategy(rdbEngineType);
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
  public void selectQueryWithTableWithoutClusteringKeyTest(RdbEngine rdbEngineType)
      throws SQLException {
    RdbEngineStrategy rdbEngine = RdbEngine.createRdbEngineStrategy(rdbEngineType);
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
  public void insertQueryTest(RdbEngine rdbEngineType) throws SQLException {
    RdbEngineStrategy rdbEngine = RdbEngine.createRdbEngineStrategy(rdbEngineType);
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
  public void updateQueryTest(RdbEngine rdbEngineType) throws SQLException {
    RdbEngineStrategy rdbEngine = RdbEngine.createRdbEngineStrategy(rdbEngineType);
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
  public void deleteQueryTest(RdbEngine rdbEngineType) throws SQLException {
    RdbEngineStrategy rdbEngine = RdbEngine.createRdbEngineStrategy(rdbEngineType);
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
  public void upsertQueryTest(RdbEngine rdbEngineType) throws SQLException {
    RdbEngineStrategy rdbEngine = RdbEngine.createRdbEngineStrategy(rdbEngineType);
    QueryBuilder queryBuilder = new QueryBuilder(rdbEngine);

    String expectedQuery = "";
    UpsertQuery query;
    PreparedStatement preparedStatement;

    Map<String, Column<?>> columns = new HashMap<>();
    columns.put("v1", TextColumn.of("v1", "v1Value"));
    columns.put("v2", TextColumn.of("v2", "v2Value"));
    columns.put("v3", TextColumn.of("v3", "v3Value"));

    preparedStatement = mock(PreparedStatement.class);
    switch (rdbEngineType) {
      case MYSQL:
        expectedQuery =
            "INSERT INTO n1.t1 (p1,v1,v2,v3) VALUES (?,?,?,?)"
                + " ON DUPLICATE KEY UPDATE v1=?,v2=?,v3=?";
        break;
      case POSTGRESQL:
      case YUGABYTE:
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
        expectedQuery =
            "MERGE n1.t1 t1 USING (SELECT ? p1) t2 ON (t1.p1=t2.p1) "
                + "WHEN MATCHED THEN UPDATE SET v1=?,v2=?,v3=? "
                + "WHEN NOT MATCHED THEN INSERT (p1,v1,v2,v3) VALUES (?,?,?,?);";
        break;
      case SQLITE:
        expectedQuery =
            "INSERT INTO \"n1$t1\" (p1,v1,v2,v3) VALUES (?,?,?,?) "
                + "ON CONFLICT (p1) DO UPDATE SET v1=?,v2=?,v3=?";
        break;
    }
    query =
        queryBuilder
            .upsertInto(NAMESPACE, TABLE, TABLE_METADATA)
            .values(new Key("p1", "p1Value"), Optional.empty(), columns)
            .build();
    assertThat(query.sql()).isEqualTo(encloseSql(expectedQuery, rdbEngine));
    query.bind(preparedStatement);
    switch (rdbEngineType) {
      case MYSQL:
      case POSTGRESQL:
      case SQLITE:
      case YUGABYTE:
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
    switch (rdbEngineType) {
      case MYSQL:
        expectedQuery =
            "INSERT INTO n1.t1 (p1,c1,v1,v2,v3) VALUES (?,?,?,?,?)"
                + " ON DUPLICATE KEY UPDATE v1=?,v2=?,v3=?";
        break;
      case POSTGRESQL:
      case YUGABYTE:
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
        expectedQuery =
            "MERGE n1.t1 t1 USING (SELECT ? p1,? c1) t2 "
                + "ON (t1.p1=t2.p1 AND t1.c1=t2.c1) "
                + "WHEN MATCHED THEN UPDATE SET v1=?,v2=?,v3=? "
                + "WHEN NOT MATCHED THEN INSERT (p1,c1,v1,v2,v3) VALUES (?,?,?,?,?);";
        break;
      case SQLITE:
        expectedQuery =
            "INSERT INTO \"n1$t1\" (p1,c1,v1,v2,v3) VALUES (?,?,?,?,?) "
                + "ON CONFLICT (p1,c1) DO UPDATE SET v1=?,v2=?,v3=?";
        break;
    }
    query =
        queryBuilder
            .upsertInto(NAMESPACE, TABLE, TABLE_METADATA)
            .values(new Key("p1", "p1Value"), Optional.of(new Key("c1", "c1Value")), columns)
            .build();
    assertThat(query.sql()).isEqualTo(encloseSql(expectedQuery, rdbEngine));
    query.bind(preparedStatement);
    switch (rdbEngineType) {
      case MYSQL:
      case POSTGRESQL:
      case SQLITE:
      case YUGABYTE:
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
    switch (rdbEngineType) {
      case MYSQL:
        expectedQuery =
            "INSERT INTO n1.t1 (p1,p2,c1,c2,v1,v2,v3,v4) VALUES (?,?,?,?,?,?,?,?)"
                + " ON DUPLICATE KEY UPDATE v1=?,v2=?,v3=?,v4=?";
        break;
      case POSTGRESQL:
      case YUGABYTE:
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
        expectedQuery =
            "MERGE n1.t1 t1 USING (SELECT ? p1,? p2,? c1,? c2) t2 "
                + "ON (t1.p1=t2.p1 AND t1.p2=t2.p2 AND t1.c1=t2.c1 AND t1.c2=t2.c2) "
                + "WHEN MATCHED THEN UPDATE SET v1=?,v2=?,v3=?,v4=? "
                + "WHEN NOT MATCHED THEN INSERT (p1,p2,c1,c2,v1,v2,v3,v4) VALUES (?,?,?,?,?,?,?,?);";
        break;
      case SQLITE:
        expectedQuery =
            "INSERT INTO \"n1$t1\" (p1,p2,c1,c2,v1,v2,v3,v4) VALUES (?,?,?,?,?,?,?,?) "
                + "ON CONFLICT (p1,p2,c1,c2) DO UPDATE SET v1=?,v2=?,v3=?,v4=?";
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
    switch (rdbEngineType) {
      case MYSQL:
      case POSTGRESQL:
      case SQLITE:
      case YUGABYTE:
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
    switch (rdbEngineType) {
      case MYSQL:
        expectedQuery =
            "INSERT INTO n1.t1 (p1,p2,c1,c2,v1,v2,v3,v4,v5) VALUES (?,?,?,?,?,?,?,?,?)"
                + " ON DUPLICATE KEY UPDATE v1=?,v2=?,v3=?,v4=?,v5=?";
        break;
      case POSTGRESQL:
      case YUGABYTE:
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
        expectedQuery =
            "MERGE n1.t1 t1 USING (SELECT ? p1,? p2,? c1,? c2) t2 "
                + "ON (t1.p1=t2.p1 AND t1.p2=t2.p2 AND t1.c1=t2.c1 AND t1.c2=t2.c2) "
                + "WHEN MATCHED THEN UPDATE SET v1=?,v2=?,v3=?,v4=?,v5=? "
                + "WHEN NOT MATCHED THEN INSERT (p1,p2,c1,c2,v1,v2,v3,v4,v5) "
                + "VALUES (?,?,?,?,?,?,?,?,?);";
        break;
      case SQLITE:
        expectedQuery =
            "INSERT INTO \"n1$t1\" (p1,p2,c1,c2,v1,v2,v3,v4,v5) VALUES (?,?,?,?,?,?,?,?,?) "
                + "ON CONFLICT (p1,p2,c1,c2) DO UPDATE SET v1=?,v2=?,v3=?,v4=?,v5=?";
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
    switch (rdbEngineType) {
      case MYSQL:
      case POSTGRESQL:
      case SQLITE:
      case YUGABYTE:
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
  public void upsertQueryWithoutValuesTest(RdbEngine rdbEngineType) throws SQLException {
    RdbEngineStrategy rdbEngine = RdbEngine.createRdbEngineStrategy(rdbEngineType);
    QueryBuilder queryBuilder = new QueryBuilder(rdbEngine);

    String expectedQuery = "";
    UpsertQuery query;
    PreparedStatement preparedStatement;

    preparedStatement = mock(PreparedStatement.class);
    switch (rdbEngineType) {
      case MYSQL:
        expectedQuery = "INSERT IGNORE INTO n1.t1 (p1) VALUES (?)";
        break;
      case POSTGRESQL:
      case YUGABYTE:
        expectedQuery = "INSERT INTO n1.t1 (p1) VALUES (?) ON CONFLICT (p1) DO NOTHING";
        break;
      case ORACLE:
        expectedQuery =
            "MERGE INTO n1.t1 t1 USING (SELECT ? p1 FROM DUAL) t2 ON (t1.p1=t2.p1) "
                + "WHEN NOT MATCHED THEN INSERT (p1) VALUES (?)";
        break;
      case SQL_SERVER:
        expectedQuery =
            "MERGE n1.t1 t1 USING (SELECT ? p1) t2 ON (t1.p1=t2.p1) "
                + "WHEN NOT MATCHED THEN INSERT (p1) VALUES (?);";
        break;
      case SQLITE:
        expectedQuery = "INSERT INTO \"n1$t1\" (p1) VALUES (?) ON CONFLICT (p1) DO NOTHING";
        break;
    }
    query =
        queryBuilder
            .upsertInto(NAMESPACE, TABLE, TABLE_METADATA)
            .values(new Key("p1", "p1Value"), Optional.empty(), Collections.emptyMap())
            .build();
    assertThat(query.sql()).isEqualTo(encloseSql(expectedQuery, rdbEngine));
    query.bind(preparedStatement);
    switch (rdbEngineType) {
      case MYSQL:
      case POSTGRESQL:
      case SQLITE:
      case YUGABYTE:
        verify(preparedStatement).setString(1, "p1Value");
        break;
      case ORACLE:
      case SQL_SERVER:
        verify(preparedStatement).setString(1, "p1Value");
        verify(preparedStatement).setString(2, "p1Value");
        break;
    }

    preparedStatement = mock(PreparedStatement.class);
    switch (rdbEngineType) {
      case MYSQL:
        expectedQuery = "INSERT IGNORE INTO n1.t1 (p1,c1) VALUES (?,?)";
        break;
      case POSTGRESQL:
      case YUGABYTE:
        expectedQuery = "INSERT INTO n1.t1 (p1,c1) VALUES (?,?) ON CONFLICT (p1,c1) DO NOTHING";
        break;
      case ORACLE:
        expectedQuery =
            "MERGE INTO n1.t1 t1 USING (SELECT ? p1,? c1 FROM DUAL) t2 "
                + "ON (t1.p1=t2.p1 AND t1.c1=t2.c1) "
                + "WHEN NOT MATCHED THEN INSERT (p1,c1) VALUES (?,?)";
        break;
      case SQL_SERVER:
        expectedQuery =
            "MERGE n1.t1 t1 USING (SELECT ? p1,? c1) t2 "
                + "ON (t1.p1=t2.p1 AND t1.c1=t2.c1) "
                + "WHEN NOT MATCHED THEN INSERT (p1,c1) VALUES (?,?);";
        break;
      case SQLITE:
        expectedQuery = "INSERT INTO \"n1$t1\" (p1,c1) VALUES (?,?) ON CONFLICT (p1,c1) DO NOTHING";
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
    switch (rdbEngineType) {
      case MYSQL:
      case POSTGRESQL:
      case SQLITE:
      case YUGABYTE:
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
    switch (rdbEngineType) {
      case MYSQL:
        expectedQuery = "INSERT IGNORE INTO n1.t1 (p1,p2,c1,c2) VALUES (?,?,?,?)";
        break;
      case POSTGRESQL:
      case YUGABYTE:
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
        expectedQuery =
            "MERGE n1.t1 t1 USING (SELECT ? p1,? p2,? c1,? c2) t2 "
                + "ON (t1.p1=t2.p1 AND t1.p2=t2.p2 AND t1.c1=t2.c1 AND t1.c2=t2.c2) "
                + "WHEN NOT MATCHED THEN INSERT (p1,p2,c1,c2) VALUES (?,?,?,?);";
        break;
      case SQLITE:
        expectedQuery =
            "INSERT INTO \"n1$t1\" (p1,p2,c1,c2) VALUES (?,?,?,?) "
                + "ON CONFLICT (p1,p2,c1,c2) DO NOTHING";
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
    switch (rdbEngineType) {
      case MYSQL:
      case POSTGRESQL:
      case SQLITE:
      case YUGABYTE:
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

  private String encloseSql(String sql, RdbEngineStrategy rdbEngine) {
    return sql.replace("n1.t1", rdbEngine.encloseFullTableName("n1", "t1"))
        .replace("p1", rdbEngine.enclose("p1"))
        .replace("p2", rdbEngine.enclose("p2"))
        .replace("c1", rdbEngine.enclose("c1"))
        .replace("c2", rdbEngine.enclose("c2"))
        .replace("v1", rdbEngine.enclose("v1"))
        .replace("v2", rdbEngine.enclose("v2"))
        .replace("v3", rdbEngine.enclose("v3"))
        .replace("v4", rdbEngine.enclose("v4"))
        .replace("v5", rdbEngine.enclose("v5"));
  }
}
