package com.scalar.db.sql.statement;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.scalar.db.sql.BindMarker;
import com.scalar.db.sql.ClusteringOrdering;
import com.scalar.db.sql.Predicate;
import com.scalar.db.sql.Projection;
import com.scalar.db.sql.Value;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.Test;

@SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_INFERRED")
public class SelectStatementTest {

  @Test
  public void bind_positionalValuesGiven_ShouldBindProperly() {
    // Arrange
    SelectStatement statement1 =
        SelectStatement.of(
            "ns",
            "table",
            ImmutableList.of(
                Projection.column("col1"), Projection.column("col2"), Projection.column("col3")),
            ImmutableList.of(
                Predicate.column("col1").isEqualTo(BindMarker.of()),
                Predicate.column("col2").isGreaterThan(BindMarker.of()),
                Predicate.column("col2").isLessThanOrEqualTo(BindMarker.of())),
            ImmutableList.of(ClusteringOrdering.column("col2").desc()),
            BindMarker.of());
    List<Value> positionalValues1 =
        Arrays.asList(Value.ofInt(10), Value.ofText("aaa"), Value.ofText("bbb"), Value.ofInt(100));

    SelectStatement statement2 =
        SelectStatement.of(
            "ns",
            "table",
            ImmutableList.of(
                Projection.column("col1"), Projection.column("col2"), Projection.column("col3")),
            ImmutableList.of(
                Predicate.column("col1").isEqualTo(BindMarker.of()),
                Predicate.column("col2").isGreaterThan(BindMarker.of()),
                Predicate.column("col2").isLessThanOrEqualTo(BindMarker.of())),
            ImmutableList.of(ClusteringOrdering.column("col2").desc()),
            BindMarker.of());
    List<Value> positionalValues2 = Arrays.asList(Value.ofInt(10), Value.ofText("aaa"));

    SelectStatement statement3 =
        SelectStatement.of(
            "ns",
            "table",
            ImmutableList.of(
                Projection.column("col1"), Projection.column("col2"), Projection.column("col3")),
            ImmutableList.of(
                Predicate.column("col1").isEqualTo(BindMarker.of()),
                Predicate.column("col2").isGreaterThan(BindMarker.of()),
                Predicate.column("col2").isLessThanOrEqualTo(BindMarker.of())),
            ImmutableList.of(ClusteringOrdering.column("col2").desc()),
            BindMarker.of());
    List<Value> positionalValues3 =
        Arrays.asList(
            Value.ofInt(10),
            Value.ofText("aaa"),
            Value.ofText("bbb"),
            Value.ofInt(100),
            Value.ofText("ccc"));

    // Act
    SelectStatement actual1 = statement1.bind(positionalValues1);
    SelectStatement actual2 = statement2.bind(positionalValues2);
    SelectStatement actual3 = statement3.bind(positionalValues3);

    // Assert
    assertThat(actual1)
        .isEqualTo(
            SelectStatement.of(
                "ns",
                "table",
                ImmutableList.of(
                    Projection.column("col1"),
                    Projection.column("col2"),
                    Projection.column("col3")),
                ImmutableList.of(
                    Predicate.column("col1").isEqualTo(Value.ofInt(10)),
                    Predicate.column("col2").isGreaterThan(Value.ofText("aaa")),
                    Predicate.column("col2").isLessThanOrEqualTo(Value.ofText("bbb"))),
                ImmutableList.of(ClusteringOrdering.column("col2").desc()),
                Value.ofInt(100)));
    assertThat(actual2)
        .isEqualTo(
            SelectStatement.of(
                "ns",
                "table",
                ImmutableList.of(
                    Projection.column("col1"),
                    Projection.column("col2"),
                    Projection.column("col3")),
                ImmutableList.of(
                    Predicate.column("col1").isEqualTo(Value.ofInt(10)),
                    Predicate.column("col2").isGreaterThan(Value.ofText("aaa")),
                    Predicate.column("col2").isLessThanOrEqualTo(BindMarker.of())),
                ImmutableList.of(ClusteringOrdering.column("col2").desc()),
                BindMarker.of()));
    assertThat(actual3)
        .isEqualTo(
            SelectStatement.of(
                "ns",
                "table",
                ImmutableList.of(
                    Projection.column("col1"),
                    Projection.column("col2"),
                    Projection.column("col3")),
                ImmutableList.of(
                    Predicate.column("col1").isEqualTo(Value.ofInt(10)),
                    Predicate.column("col2").isGreaterThan(Value.ofText("aaa")),
                    Predicate.column("col2").isLessThanOrEqualTo(Value.ofText("bbb"))),
                ImmutableList.of(ClusteringOrdering.column("col2").desc()),
                Value.ofInt(100)));
  }

  @Test
  public void
      bind_namedValuesGiven_ButStatementHasPositionalBindMarker_ShouldThrowIllegalArgumentException() {
    // Arrange
    SelectStatement statement =
        SelectStatement.of(
            "ns",
            "table",
            ImmutableList.of(
                Projection.column("col1"), Projection.column("col2"), Projection.column("col3")),
            ImmutableList.of(
                Predicate.column("col1").isEqualTo(BindMarker.of()),
                Predicate.column("col2").isGreaterThan(BindMarker.of()),
                Predicate.column("col2").isLessThanOrEqualTo(BindMarker.of())),
            ImmutableList.of(ClusteringOrdering.column("col2").desc()),
            BindMarker.of());
    Map<String, Value> namedValues =
        ImmutableMap.of(
            "name1",
            Value.ofInt(10),
            "name2",
            Value.ofText("aaa"),
            "name3",
            Value.ofText("bbb"),
            "name4",
            Value.ofInt(100));

    // Act Assert
    assertThatThrownBy(() -> statement.bind(namedValues))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void bind_namedValuesGiven_ShouldBindProperly() {
    // Arrange
    SelectStatement statement1 =
        SelectStatement.of(
            "ns",
            "table",
            ImmutableList.of(
                Projection.column("col1"), Projection.column("col2"), Projection.column("col3")),
            ImmutableList.of(
                Predicate.column("col1").isEqualTo(BindMarker.of("name1")),
                Predicate.column("col2").isGreaterThan(BindMarker.of("name2")),
                Predicate.column("col2").isLessThanOrEqualTo(BindMarker.of("name3"))),
            ImmutableList.of(ClusteringOrdering.column("col2").desc()),
            BindMarker.of("name4"));
    Map<String, Value> namedValues1 =
        ImmutableMap.of(
            "name1",
            Value.ofInt(10),
            "name2",
            Value.ofText("aaa"),
            "name3",
            Value.ofText("bbb"),
            "name4",
            Value.ofInt(100));

    SelectStatement statement2 =
        SelectStatement.of(
            "ns",
            "table",
            ImmutableList.of(
                Projection.column("col1"), Projection.column("col2"), Projection.column("col3")),
            ImmutableList.of(
                Predicate.column("col1").isEqualTo(BindMarker.of("name1")),
                Predicate.column("col2").isGreaterThan(BindMarker.of("name2")),
                Predicate.column("col2").isLessThanOrEqualTo(BindMarker.of("name3"))),
            ImmutableList.of(ClusteringOrdering.column("col2").desc()),
            BindMarker.of("name4"));
    Map<String, Value> namedValues2 =
        ImmutableMap.of("name1", Value.ofInt(10), "name2", Value.ofText("aaa"));

    SelectStatement statement3 =
        SelectStatement.of(
            "ns",
            "table",
            ImmutableList.of(
                Projection.column("col1"), Projection.column("col2"), Projection.column("col3")),
            ImmutableList.of(
                Predicate.column("col1").isEqualTo(BindMarker.of("name1")),
                Predicate.column("col2").isGreaterThan(BindMarker.of("name2")),
                Predicate.column("col2").isLessThanOrEqualTo(BindMarker.of("name3"))),
            ImmutableList.of(ClusteringOrdering.column("col2").desc()),
            BindMarker.of("name4"));
    Map<String, Value> namedValues3 =
        ImmutableMap.of(
            "name1",
            Value.ofInt(10),
            "name2",
            Value.ofText("aaa"),
            "name3",
            Value.ofText("bbb"),
            "name4",
            Value.ofInt(100),
            "name5",
            Value.ofText("ccc"));

    SelectStatement statement4 =
        SelectStatement.of(
            "ns",
            "table",
            ImmutableList.of(
                Projection.column("col1"), Projection.column("col2"), Projection.column("col3")),
            ImmutableList.of(
                Predicate.column("col1").isEqualTo(BindMarker.of("name1")),
                Predicate.column("col2").isGreaterThan(BindMarker.of("name2")),
                Predicate.column("col2").isLessThanOrEqualTo(BindMarker.of("name2"))),
            ImmutableList.of(ClusteringOrdering.column("col2").desc()),
            BindMarker.of("name3"));
    Map<String, Value> namedValues4 =
        ImmutableMap.of(
            "name1", Value.ofInt(10), "name2", Value.ofText("aaa"), "name3", Value.ofInt(100));

    // Act
    SelectStatement actual1 = statement1.bind(namedValues1);
    SelectStatement actual2 = statement2.bind(namedValues2);
    SelectStatement actual3 = statement3.bind(namedValues3);
    SelectStatement actual4 = statement4.bind(namedValues4);

    // Assert
    assertThat(actual1)
        .isEqualTo(
            SelectStatement.of(
                "ns",
                "table",
                ImmutableList.of(
                    Projection.column("col1"),
                    Projection.column("col2"),
                    Projection.column("col3")),
                ImmutableList.of(
                    Predicate.column("col1").isEqualTo(Value.ofInt(10)),
                    Predicate.column("col2").isGreaterThan(Value.ofText("aaa")),
                    Predicate.column("col2").isLessThanOrEqualTo(Value.ofText("bbb"))),
                ImmutableList.of(ClusteringOrdering.column("col2").desc()),
                Value.ofInt(100)));
    assertThat(actual2)
        .isEqualTo(
            SelectStatement.of(
                "ns",
                "table",
                ImmutableList.of(
                    Projection.column("col1"),
                    Projection.column("col2"),
                    Projection.column("col3")),
                ImmutableList.of(
                    Predicate.column("col1").isEqualTo(Value.ofInt(10)),
                    Predicate.column("col2").isGreaterThan(Value.ofText("aaa")),
                    Predicate.column("col2").isLessThanOrEqualTo(BindMarker.of("name3"))),
                ImmutableList.of(ClusteringOrdering.column("col2").desc()),
                BindMarker.of("name4")));
    assertThat(actual3)
        .isEqualTo(
            SelectStatement.of(
                "ns",
                "table",
                ImmutableList.of(
                    Projection.column("col1"),
                    Projection.column("col2"),
                    Projection.column("col3")),
                ImmutableList.of(
                    Predicate.column("col1").isEqualTo(Value.ofInt(10)),
                    Predicate.column("col2").isGreaterThan(Value.ofText("aaa")),
                    Predicate.column("col2").isLessThanOrEqualTo(Value.ofText("bbb"))),
                ImmutableList.of(ClusteringOrdering.column("col2").desc()),
                Value.ofInt(100)));
    assertThat(actual4)
        .isEqualTo(
            SelectStatement.of(
                "ns",
                "table",
                ImmutableList.of(
                    Projection.column("col1"),
                    Projection.column("col2"),
                    Projection.column("col3")),
                ImmutableList.of(
                    Predicate.column("col1").isEqualTo(Value.ofInt(10)),
                    Predicate.column("col2").isGreaterThan(Value.ofText("aaa")),
                    Predicate.column("col2").isLessThanOrEqualTo(Value.ofText("aaa"))),
                ImmutableList.of(ClusteringOrdering.column("col2").desc()),
                Value.ofInt(100)));
  }

  @Test
  public void
      bind_positionalValuesGiven_ButStatementHasNamedBindMarker_ShouldThrowIllegalArgumentException() {
    // Arrange
    SelectStatement statement =
        SelectStatement.of(
            "ns",
            "table",
            ImmutableList.of(
                Projection.column("col1"), Projection.column("col2"), Projection.column("col3")),
            ImmutableList.of(
                Predicate.column("col1").isEqualTo(BindMarker.of("name1")),
                Predicate.column("col2").isGreaterThan(BindMarker.of("name2")),
                Predicate.column("col2").isLessThanOrEqualTo(BindMarker.of("name3"))),
            ImmutableList.of(ClusteringOrdering.column("col2").desc()),
            BindMarker.of("name4"));
    List<Value> positionalValues =
        Arrays.asList(Value.ofInt(10), Value.ofText("aaa"), Value.ofText("bbb"), Value.ofInt(100));

    // Act Assert
    assertThatThrownBy(() -> statement.bind(positionalValues))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
