package com.scalar.db.sql.builder;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.scalar.db.sql.Assignment;
import com.scalar.db.sql.ClusteringOrder;
import com.scalar.db.sql.ClusteringOrdering;
import com.scalar.db.sql.DataType;
import com.scalar.db.sql.Predicate;
import com.scalar.db.sql.Projection;
import com.scalar.db.sql.Value;
import com.scalar.db.sql.statement.CreateCoordinatorTableStatement;
import com.scalar.db.sql.statement.CreateIndexStatement;
import com.scalar.db.sql.statement.CreateNamespaceStatement;
import com.scalar.db.sql.statement.CreateTableStatement;
import com.scalar.db.sql.statement.DeleteStatement;
import com.scalar.db.sql.statement.DropCoordinatorTableStatement;
import com.scalar.db.sql.statement.DropIndexStatement;
import com.scalar.db.sql.statement.DropNamespaceStatement;
import com.scalar.db.sql.statement.DropTableStatement;
import com.scalar.db.sql.statement.InsertStatement;
import com.scalar.db.sql.statement.SelectStatement;
import com.scalar.db.sql.statement.TruncateCoordinatorTableStatement;
import com.scalar.db.sql.statement.TruncateTableStatement;
import com.scalar.db.sql.statement.UpdateStatement;
import java.util.Arrays;
import org.junit.Test;

public class StatementBuilderTest {

  @Test
  public void createCoordinatorTable_ShouldBuildProperStatement() {
    // Arrange

    // Act
    CreateCoordinatorTableStatement statement1 =
        StatementBuilder.createCoordinatorTable()
            .withOption("opt1", "val1")
            .withOptions(ImmutableMap.of("opt2", "val2", "opt3", "val3"))
            .build();
    CreateCoordinatorTableStatement statement2 =
        StatementBuilder.createCoordinatorTable()
            .ifNotExists()
            .withOption("opt1", "val1")
            .withOptions(ImmutableMap.of("opt2", "val2", "opt3", "val3"))
            .build();
    CreateCoordinatorTableStatement statement3 =
        StatementBuilder.createCoordinatorTable()
            .ifNotExists(false)
            .withOption("opt1", "val1")
            .withOptions(ImmutableMap.of("opt2", "val2", "opt3", "val3"))
            .build();

    // Assert
    assertThat(statement1)
        .isEqualTo(
            CreateCoordinatorTableStatement.of(
                false, ImmutableMap.of("opt1", "val1", "opt2", "val2", "opt3", "val3")));
    assertThat(statement2)
        .isEqualTo(
            CreateCoordinatorTableStatement.of(
                true, ImmutableMap.of("opt1", "val1", "opt2", "val2", "opt3", "val3")));
    assertThat(statement3)
        .isEqualTo(
            CreateCoordinatorTableStatement.of(
                false, ImmutableMap.of("opt1", "val1", "opt2", "val2", "opt3", "val3")));
  }

  @Test
  public void createIndex_ShouldBuildProperStatement() {
    // Arrange

    // Act
    CreateIndexStatement statement1 =
        StatementBuilder.createIndex()
            .onTable("ns", "tbl")
            .column("col")
            .withOption("opt1", "val1")
            .withOptions(ImmutableMap.of("opt2", "val2", "opt3", "val3"))
            .build();
    CreateIndexStatement statement2 =
        StatementBuilder.createIndex()
            .ifNotExists()
            .onTable("ns", "tbl")
            .column("col")
            .withOption("opt1", "val1")
            .withOptions(ImmutableMap.of("opt2", "val2", "opt3", "val3"))
            .build();
    CreateIndexStatement statement3 =
        StatementBuilder.createIndex()
            .ifNotExists(false)
            .onTable("ns", "tbl")
            .column("col")
            .withOption("opt1", "val1")
            .withOptions(ImmutableMap.of("opt2", "val2", "opt3", "val3"))
            .build();

    // Assert
    assertThat(statement1)
        .isEqualTo(
            CreateIndexStatement.of(
                "ns",
                "tbl",
                "col",
                false,
                ImmutableMap.of("opt1", "val1", "opt2", "val2", "opt3", "val3")));
    assertThat(statement2)
        .isEqualTo(
            CreateIndexStatement.of(
                "ns",
                "tbl",
                "col",
                true,
                ImmutableMap.of("opt1", "val1", "opt2", "val2", "opt3", "val3")));
    assertThat(statement3)
        .isEqualTo(
            CreateIndexStatement.of(
                "ns",
                "tbl",
                "col",
                false,
                ImmutableMap.of("opt1", "val1", "opt2", "val2", "opt3", "val3")));
  }

  @Test
  public void createNamespace_ShouldBuildProperStatement() {
    // Arrange

    // Act
    CreateNamespaceStatement statement1 =
        StatementBuilder.createNamespace("ns")
            .withOption("opt1", "val1")
            .withOptions(ImmutableMap.of("opt2", "val2", "opt3", "val3"))
            .build();
    CreateNamespaceStatement statement2 =
        StatementBuilder.createNamespace("ns")
            .ifNotExists()
            .withOption("opt1", "val1")
            .withOptions(ImmutableMap.of("opt2", "val2", "opt3", "val3"))
            .build();
    CreateNamespaceStatement statement3 =
        StatementBuilder.createNamespace("ns")
            .ifNotExists(false)
            .withOption("opt1", "val1")
            .withOptions(ImmutableMap.of("opt2", "val2", "opt3", "val3"))
            .build();

    // Assert
    assertThat(statement1)
        .isEqualTo(
            CreateNamespaceStatement.of(
                "ns", false, ImmutableMap.of("opt1", "val1", "opt2", "val2", "opt3", "val3")));
    assertThat(statement2)
        .isEqualTo(
            CreateNamespaceStatement.of(
                "ns", true, ImmutableMap.of("opt1", "val1", "opt2", "val2", "opt3", "val3")));
    assertThat(statement3)
        .isEqualTo(
            CreateNamespaceStatement.of(
                "ns", false, ImmutableMap.of("opt1", "val1", "opt2", "val2", "opt3", "val3")));
  }

  @Test
  public void createTable_ShouldBuildProperStatement() {
    // Arrange

    // Act
    CreateTableStatement statement1 =
        StatementBuilder.createTable("ns", "tbl")
            .withPartitionKey("p1", DataType.INT)
            .withPartitionKey(ImmutableMap.of("p2", DataType.TEXT, "p3", DataType.BIGINT))
            .withClusteringKey("c1", DataType.TEXT)
            .withClusteringKey(ImmutableMap.of("c2", DataType.INT, "c3", DataType.FLOAT))
            .withColumn("col1", DataType.BOOLEAN)
            .withColumns(ImmutableMap.of("col2", DataType.BLOB, "col3", DataType.DOUBLE))
            .withClusteringOrder("c1", ClusteringOrder.DESC)
            .withClusteringOrders(
                ImmutableMap.of("c2", ClusteringOrder.ASC, "c3", ClusteringOrder.DESC))
            .withIndex("col1")
            .withIndexes(ImmutableSet.of("col2", "col3"))
            .withOption("opt1", "val1")
            .withOptions(ImmutableMap.of("opt2", "val2", "opt3", "val3"))
            .build();
    CreateTableStatement statement2 =
        StatementBuilder.createTable("ns", "tbl")
            .ifNotExists()
            .withPartitionKey(ImmutableMap.of("p1", DataType.TEXT, "p2", DataType.BIGINT))
            .withPartitionKey("p3", DataType.INT)
            .withClusteringKey("c1", DataType.TEXT)
            .withClusteringKey(ImmutableMap.of("c2", DataType.INT, "c3", DataType.FLOAT))
            .withColumn("col1", DataType.BOOLEAN)
            .withColumns(ImmutableMap.of("col2", DataType.BLOB, "col3", DataType.DOUBLE))
            .withClusteringOrder("c1", ClusteringOrder.DESC)
            .withClusteringOrders(
                ImmutableMap.of("c2", ClusteringOrder.ASC, "c3", ClusteringOrder.DESC))
            .withIndex("col1")
            .withIndexes(ImmutableSet.of("col2", "col3"))
            .withOption("opt1", "val1")
            .withOptions(ImmutableMap.of("opt2", "val2", "opt3", "val3"))
            .build();
    CreateTableStatement statement3 =
        StatementBuilder.createTable("ns", "tbl")
            .ifNotExists(false)
            .withPartitionKey(ImmutableMap.of("p1", DataType.TEXT, "p2", DataType.BIGINT))
            .withPartitionKey("p3", DataType.INT)
            .withClusteringKey("c1", DataType.TEXT)
            .withClusteringKey(ImmutableMap.of("c2", DataType.INT, "c3", DataType.FLOAT))
            .withColumn("col1", DataType.BOOLEAN)
            .withColumns(ImmutableMap.of("col2", DataType.BLOB, "col3", DataType.DOUBLE))
            .withClusteringOrder("c1", ClusteringOrder.DESC)
            .withClusteringOrders(
                ImmutableMap.of("c2", ClusteringOrder.ASC, "c3", ClusteringOrder.DESC))
            .withIndex("col1")
            .withIndexes(ImmutableSet.of("col2", "col3"))
            .withOption("opt1", "val1")
            .withOptions(ImmutableMap.of("opt2", "val2", "opt3", "val3"))
            .build();

    // Assert
    assertThat(statement1)
        .isEqualTo(
            CreateTableStatement.of(
                "ns",
                "tbl",
                false,
                ImmutableMap.<String, DataType>builder()
                    .put("p1", DataType.INT)
                    .put("p2", DataType.TEXT)
                    .put("p3", DataType.BIGINT)
                    .put("c1", DataType.TEXT)
                    .put("c2", DataType.INT)
                    .put("c3", DataType.FLOAT)
                    .put("col1", DataType.BOOLEAN)
                    .put("col2", DataType.BLOB)
                    .put("col3", DataType.DOUBLE)
                    .build(),
                ImmutableSet.of("p1", "p2", "p3"),
                ImmutableSet.of("c1", "c2", "c3"),
                ImmutableMap.of(
                    "c1",
                    ClusteringOrder.DESC,
                    "c2",
                    ClusteringOrder.ASC,
                    "c3",
                    ClusteringOrder.DESC),
                ImmutableSet.of("col1", "col2", "col3"),
                ImmutableMap.of("opt1", "val1", "opt2", "val2", "opt3", "val3")));
    assertThat(statement2)
        .isEqualTo(
            CreateTableStatement.of(
                "ns",
                "tbl",
                true,
                ImmutableMap.<String, DataType>builder()
                    .put("p1", DataType.TEXT)
                    .put("p2", DataType.BIGINT)
                    .put("p3", DataType.INT)
                    .put("c1", DataType.TEXT)
                    .put("c2", DataType.INT)
                    .put("c3", DataType.FLOAT)
                    .put("col1", DataType.BOOLEAN)
                    .put("col2", DataType.BLOB)
                    .put("col3", DataType.DOUBLE)
                    .build(),
                ImmutableSet.of("p1", "p2", "p3"),
                ImmutableSet.of("c1", "c2", "c3"),
                ImmutableMap.of(
                    "c1",
                    ClusteringOrder.DESC,
                    "c2",
                    ClusteringOrder.ASC,
                    "c3",
                    ClusteringOrder.DESC),
                ImmutableSet.of("col1", "col2", "col3"),
                ImmutableMap.of("opt1", "val1", "opt2", "val2", "opt3", "val3")));
    assertThat(statement3)
        .isEqualTo(
            CreateTableStatement.of(
                "ns",
                "tbl",
                false,
                ImmutableMap.<String, DataType>builder()
                    .put("p1", DataType.TEXT)
                    .put("p2", DataType.BIGINT)
                    .put("p3", DataType.INT)
                    .put("c1", DataType.TEXT)
                    .put("c2", DataType.INT)
                    .put("c3", DataType.FLOAT)
                    .put("col1", DataType.BOOLEAN)
                    .put("col2", DataType.BLOB)
                    .put("col3", DataType.DOUBLE)
                    .build(),
                ImmutableSet.of("p1", "p2", "p3"),
                ImmutableSet.of("c1", "c2", "c3"),
                ImmutableMap.of(
                    "c1",
                    ClusteringOrder.DESC,
                    "c2",
                    ClusteringOrder.ASC,
                    "c3",
                    ClusteringOrder.DESC),
                ImmutableSet.of("col1", "col2", "col3"),
                ImmutableMap.of("opt1", "val1", "opt2", "val2", "opt3", "val3")));
  }

  @Test
  public void deleteFrom_ShouldBuildProperStatement() {
    // Arrange

    // Act
    DeleteStatement statement1 =
        StatementBuilder.deleteFrom("ns1", "tbl1")
            .where(Predicate.column("col1").isEqualTo(Value.ofInt(10)))
            .and(Predicate.column("col2").isEqualTo(Value.ofText("aaa")))
            .build();

    DeleteStatement statement2 =
        StatementBuilder.deleteFrom("ns2", "tbl2")
            .where(
                Arrays.asList(
                    Predicate.column("col1").isEqualTo(Value.ofInt(20)),
                    Predicate.column("col2").isEqualTo(Value.ofText("bbb"))))
            .build();

    // Assert
    assertThat(statement1)
        .isEqualTo(
            DeleteStatement.of(
                "ns1",
                "tbl1",
                ImmutableList.of(
                    Predicate.column("col1").isEqualTo(Value.ofInt(10)),
                    Predicate.column("col2").isEqualTo(Value.ofText("aaa")))));

    assertThat(statement2)
        .isEqualTo(
            DeleteStatement.of(
                "ns2",
                "tbl2",
                ImmutableList.of(
                    Predicate.column("col1").isEqualTo(Value.ofInt(20)),
                    Predicate.column("col2").isEqualTo(Value.ofText("bbb")))));
  }

  @Test
  public void dropCoordinatorTable_ShouldBuildProperStatement() {
    // Arrange

    // Act
    DropCoordinatorTableStatement statement1 = StatementBuilder.dropCoordinatorTable().build();
    DropCoordinatorTableStatement statement2 =
        StatementBuilder.dropCoordinatorTable().ifExists().build();
    DropCoordinatorTableStatement statement3 =
        StatementBuilder.dropCoordinatorTable().ifExists(false).build();

    // Assert
    assertThat(statement1).isEqualTo(DropCoordinatorTableStatement.of(false));
    assertThat(statement2).isEqualTo(DropCoordinatorTableStatement.of(true));
    assertThat(statement3).isEqualTo(DropCoordinatorTableStatement.of(false));
  }

  @Test
  public void dropIndex_ShouldBuildProperStatement() {
    // Arrange

    // Act
    DropIndexStatement statement1 =
        StatementBuilder.dropIndex().onTable("ns", "tbl").column("col").build();
    DropIndexStatement statement2 =
        StatementBuilder.dropIndex().ifExists().onTable("ns", "tbl").column("col").build();
    DropIndexStatement statement3 =
        StatementBuilder.dropIndex().ifExists(false).onTable("ns", "tbl").column("col").build();

    // Assert
    assertThat(statement1).isEqualTo(DropIndexStatement.of("ns", "tbl", "col", false));
    assertThat(statement2).isEqualTo(DropIndexStatement.of("ns", "tbl", "col", true));
    assertThat(statement3).isEqualTo(DropIndexStatement.of("ns", "tbl", "col", false));
  }

  @Test
  public void dropNamespace_ShouldBuildProperStatement() {
    // Arrange

    // Act
    DropNamespaceStatement statement1 = StatementBuilder.dropNamespace("ns").build();
    DropNamespaceStatement statement2 = StatementBuilder.dropNamespace("ns").ifExists().build();
    DropNamespaceStatement statement3 = StatementBuilder.dropNamespace("ns").cascade().build();
    DropNamespaceStatement statement4 =
        StatementBuilder.dropNamespace("ns").ifExists(false).cascade(false).build();

    // Assert
    assertThat(statement1).isEqualTo(DropNamespaceStatement.of("ns", false, false));
    assertThat(statement2).isEqualTo(DropNamespaceStatement.of("ns", true, false));
    assertThat(statement3).isEqualTo(DropNamespaceStatement.of("ns", false, true));
    assertThat(statement4).isEqualTo(DropNamespaceStatement.of("ns", false, false));
  }

  @Test
  public void dropTable_ShouldBuildProperStatement() {
    // Arrange

    // Act
    DropTableStatement statement1 = StatementBuilder.dropTable("ns", "tbl").build();
    DropTableStatement statement2 = StatementBuilder.dropTable("ns", "tbl").ifExists().build();
    DropTableStatement statement3 = StatementBuilder.dropTable("ns", "tbl").ifExists(false).build();

    // Assert
    assertThat(statement1).isEqualTo(DropTableStatement.of("ns", "tbl", false));
    assertThat(statement2).isEqualTo(DropTableStatement.of("ns", "tbl", true));
    assertThat(statement3).isEqualTo(DropTableStatement.of("ns", "tbl", false));
  }

  @Test
  public void insertInto_ShouldBuildProperStatement() {
    // Arrange

    // Act
    InsertStatement statement1 =
        StatementBuilder.insertInto("ns1", "tbl1")
            .values(
                Assignment.column("col1").value(Value.ofInt(10)),
                Assignment.column("col2").value(Value.ofText("aaa")))
            .build();

    InsertStatement statement2 =
        StatementBuilder.insertInto("ns2", "tbl2")
            .values(
                Arrays.asList(
                    Assignment.column("col1").value(Value.ofInt(20)),
                    Assignment.column("col2").value(Value.ofText("bbb"))))
            .build();

    // Assert
    assertThat(statement1)
        .isEqualTo(
            InsertStatement.of(
                "ns1",
                "tbl1",
                ImmutableList.of(
                    Assignment.column("col1").value(Value.ofInt(10)),
                    Assignment.column("col2").value(Value.ofText("aaa")))));

    assertThat(statement2)
        .isEqualTo(
            InsertStatement.of(
                "ns2",
                "tbl2",
                ImmutableList.of(
                    Assignment.column("col1").value(Value.ofInt(20)),
                    Assignment.column("col2").value(Value.ofText("bbb")))));
  }

  @Test
  public void select_ShouldBuildProperStatement() {
    // Arrange

    // Act
    SelectStatement statement1 =
        StatementBuilder.select("col1", "col2", "col3")
            .from("ns1", "tbl1")
            .where(Predicate.column("col1").isEqualTo(Value.ofInt(10)))
            .and(Predicate.column("col2").isGreaterThan(Value.ofText("aaa")))
            .and(Predicate.column("col2").isLessThan(Value.ofText("ccc")))
            .orderBy(
                ClusteringOrdering.column("col2").desc(), ClusteringOrdering.column("col3").desc())
            .limit(10)
            .build();

    SelectStatement statement2 =
        StatementBuilder.select(Projection.column("col2"), Projection.column("col3"))
            .from("ns2", "tbl2")
            .where(
                Arrays.asList(
                    Predicate.column("col1").isEqualTo(Value.ofInt(20)),
                    Predicate.column("col2").isGreaterThan(Value.ofText("ddd")),
                    Predicate.column("col2").isLessThan(Value.ofText("fff"))))
            .orderBy(
                Arrays.asList(
                    ClusteringOrdering.column("col2").desc(),
                    ClusteringOrdering.column("col3").asc()))
            .limit(10)
            .build();

    SelectStatement statement3 =
        StatementBuilder.select(
                Arrays.asList(Projection.column("col2").as("a"), Projection.column("col3").as("b")))
            .from("ns3", "tbl3")
            .where(
                Arrays.asList(
                    Predicate.column("col1").isEqualTo(Value.ofInt(20)),
                    Predicate.column("col2").isEqualTo(Value.ofText("aaa"))))
            .orderBy(
                Arrays.asList(
                    ClusteringOrdering.column("col2").desc(),
                    ClusteringOrdering.column("col3").desc()))
            .limit(10)
            .build();

    // Assert
    assertThat(statement1)
        .isEqualTo(
            SelectStatement.of(
                "ns1",
                "tbl1",
                ImmutableList.of(
                    Projection.column("col1"),
                    Projection.column("col2"),
                    Projection.column("col3")),
                ImmutableList.of(
                    Predicate.column("col1").isEqualTo(Value.ofInt(10)),
                    Predicate.column("col2").isGreaterThan(Value.ofText("aaa")),
                    Predicate.column("col2").isLessThan(Value.ofText("ccc"))),
                ImmutableList.of(
                    ClusteringOrdering.column("col2").desc(),
                    ClusteringOrdering.column("col3").desc()),
                10));

    assertThat(statement2)
        .isEqualTo(
            SelectStatement.of(
                "ns2",
                "tbl2",
                ImmutableList.of(Projection.column("col2"), Projection.column("col3")),
                ImmutableList.of(
                    Predicate.column("col1").isEqualTo(Value.ofInt(20)),
                    Predicate.column("col2").isGreaterThan(Value.ofText("ddd")),
                    Predicate.column("col2").isLessThan(Value.ofText("fff"))),
                ImmutableList.of(
                    ClusteringOrdering.column("col2").desc(),
                    ClusteringOrdering.column("col3").asc()),
                10));

    assertThat(statement3)
        .isEqualTo(
            SelectStatement.of(
                "ns3",
                "tbl3",
                ImmutableList.of(
                    Projection.column("col2").as("a"), Projection.column("col3").as("b")),
                ImmutableList.of(
                    Predicate.column("col1").isEqualTo(Value.ofInt(20)),
                    Predicate.column("col2").isEqualTo(Value.ofText("aaa"))),
                ImmutableList.of(
                    ClusteringOrdering.column("col2").desc(),
                    ClusteringOrdering.column("col3").desc()),
                10));
  }

  @Test
  public void truncateCoordinatorTable_ShouldBuildProperStatement() {
    // Arrange

    // Act
    TruncateCoordinatorTableStatement statement =
        StatementBuilder.truncateCoordinatorTable().build();

    // Assert
    assertThat(statement).isEqualTo(TruncateCoordinatorTableStatement.of());
  }

  @Test
  public void truncateTable_ShouldBuildProperStatement() {
    // Arrange

    // Act
    TruncateTableStatement statement = StatementBuilder.truncateTable("ns", "tbl").build();

    // Assert
    assertThat(statement).isEqualTo(TruncateTableStatement.of("ns", "tbl"));
  }

  @Test
  public void update_ShouldBuildProperStatement() {
    // Arrange

    // Act
    UpdateStatement statement1 =
        StatementBuilder.update("ns1", "tbl1")
            .set(
                Assignment.column("col3").value(Value.ofInt(10)),
                Assignment.column("col4").value(Value.ofText("aaa")))
            .where(Predicate.column("col1").isEqualTo(Value.ofInt(20)))
            .and(Predicate.column("col2").isEqualTo(Value.ofText("bbb")))
            .build();

    UpdateStatement statement2 =
        StatementBuilder.update("ns2", "tbl2")
            .set(
                Arrays.asList(
                    Assignment.column("col3").value(Value.ofInt(20)),
                    Assignment.column("col4").value(Value.ofText("bbb"))))
            .where(
                Arrays.asList(
                    Predicate.column("col1").isEqualTo(Value.ofInt(40)),
                    Predicate.column("col2").isEqualTo(Value.ofText("ccc"))))
            .build();

    // Assert
    assertThat(statement1)
        .isEqualTo(
            UpdateStatement.of(
                "ns1",
                "tbl1",
                ImmutableList.of(
                    Assignment.column("col3").value(Value.ofInt(10)),
                    Assignment.column("col4").value(Value.ofText("aaa"))),
                ImmutableList.of(
                    Predicate.column("col1").isEqualTo(Value.ofInt(20)),
                    Predicate.column("col2").isEqualTo(Value.ofText("bbb")))));

    assertThat(statement2)
        .isEqualTo(
            UpdateStatement.of(
                "ns2",
                "tbl2",
                ImmutableList.of(
                    Assignment.column("col3").value(Value.ofInt(20)),
                    Assignment.column("col4").value(Value.ofText("bbb"))),
                ImmutableList.of(
                    Predicate.column("col1").isEqualTo(Value.ofInt(40)),
                    Predicate.column("col2").isEqualTo(Value.ofText("ccc")))));
  }
}
