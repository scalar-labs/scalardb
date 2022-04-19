package com.scalar.db.sql.statement;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.scalar.db.sql.BindMarker;
import com.scalar.db.sql.Predicate;
import com.scalar.db.sql.Value;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

@SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_INFERRED")
public class DeleteStatementTest {

  @Test
  public void bind_positionalValuesGiven_ShouldBindProperly() {
    // Arrange
    DeleteStatement statement1 =
        DeleteStatement.of(
            "ns",
            "table",
            ImmutableList.of(
                Predicate.column("col1").isEqualTo(BindMarker.positional()),
                Predicate.column("col2").isEqualTo(BindMarker.positional()),
                Predicate.column("col3").isEqualTo(BindMarker.positional())));
    List<Value> positionalValues1 =
        Arrays.asList(Value.ofInt(10), Value.ofText("aaa"), Value.ofText("bbb"));

    DeleteStatement statement2 =
        DeleteStatement.of(
            "ns",
            "table",
            ImmutableList.of(
                Predicate.column("col1").isEqualTo(BindMarker.positional()),
                Predicate.column("col2").isEqualTo(BindMarker.positional()),
                Predicate.column("col3").isEqualTo(BindMarker.positional())));
    List<Value> positionalValues2 = Arrays.asList(Value.ofInt(10), Value.ofText("aaa"));

    DeleteStatement statement3 =
        DeleteStatement.of(
            "ns",
            "table",
            ImmutableList.of(
                Predicate.column("col1").isEqualTo(BindMarker.positional()),
                Predicate.column("col2").isEqualTo(BindMarker.positional()),
                Predicate.column("col3").isEqualTo(BindMarker.positional())));
    List<Value> positionalValues3 =
        Arrays.asList(
            Value.ofInt(10), Value.ofText("aaa"), Value.ofText("bbb"), Value.ofText("ccc"));

    // Act
    DeleteStatement actual1 = statement1.bind(positionalValues1);
    DeleteStatement actual2 = statement2.bind(positionalValues2);
    DeleteStatement actual3 = statement3.bind(positionalValues3);

    // Assert
    assertThat(actual1)
        .isEqualTo(
            DeleteStatement.of(
                "ns",
                "table",
                ImmutableList.of(
                    Predicate.column("col1").isEqualTo(Value.ofInt(10)),
                    Predicate.column("col2").isEqualTo(Value.ofText("aaa")),
                    Predicate.column("col3").isEqualTo(Value.ofText("bbb")))));
    assertThat(actual2)
        .isEqualTo(
            DeleteStatement.of(
                "ns",
                "table",
                ImmutableList.of(
                    Predicate.column("col1").isEqualTo(Value.ofInt(10)),
                    Predicate.column("col2").isEqualTo(Value.ofText("aaa")),
                    Predicate.column("col3").isEqualTo(BindMarker.positional()))));
    assertThat(actual3)
        .isEqualTo(
            DeleteStatement.of(
                "ns",
                "table",
                ImmutableList.of(
                    Predicate.column("col1").isEqualTo(Value.ofInt(10)),
                    Predicate.column("col2").isEqualTo(Value.ofText("aaa")),
                    Predicate.column("col3").isEqualTo(Value.ofText("bbb")))));
  }

  @Test
  public void
      bind_namedValuesGiven_ButStatementHasPositionalBindMarker_ShouldThrowIllegalArgumentException() {
    // Arrange
    DeleteStatement statement =
        DeleteStatement.of(
            "ns",
            "table",
            ImmutableList.of(
                Predicate.column("col1").isEqualTo(BindMarker.positional()),
                Predicate.column("col2").isEqualTo(BindMarker.positional()),
                Predicate.column("col3").isEqualTo(BindMarker.positional())));
    Map<String, Value> namedValues =
        ImmutableMap.of(
            "name1", Value.ofInt(10), "name2", Value.ofText("aaa"), "name3", Value.ofText("bbb"));

    // Act Assert
    assertThatThrownBy(() -> statement.bind(namedValues))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void bind_namedValuesGiven_ShouldBindProperly() {
    // Arrange
    DeleteStatement statement1 =
        DeleteStatement.of(
            "ns",
            "table",
            ImmutableList.of(
                Predicate.column("col1").isEqualTo(BindMarker.named("name1")),
                Predicate.column("col2").isEqualTo(BindMarker.named("name2")),
                Predicate.column("col3").isEqualTo(BindMarker.named("name3"))));
    Map<String, Value> namedValues1 =
        ImmutableMap.of(
            "name1", Value.ofInt(10), "name2", Value.ofText("aaa"), "name3", Value.ofText("bbb"));

    DeleteStatement statement2 =
        DeleteStatement.of(
            "ns",
            "table",
            ImmutableList.of(
                Predicate.column("col1").isEqualTo(BindMarker.named("name1")),
                Predicate.column("col2").isEqualTo(BindMarker.named("name2")),
                Predicate.column("col3").isEqualTo(BindMarker.named("name3"))));
    Map<String, Value> namedValues2 =
        ImmutableMap.of("name1", Value.ofInt(10), "name2", Value.ofText("aaa"));

    DeleteStatement statement3 =
        DeleteStatement.of(
            "ns",
            "table",
            ImmutableList.of(
                Predicate.column("col1").isEqualTo(BindMarker.named("name1")),
                Predicate.column("col2").isEqualTo(BindMarker.named("name2")),
                Predicate.column("col3").isEqualTo(BindMarker.named("name3"))));
    Map<String, Value> namedValues3 =
        ImmutableMap.of(
            "name1",
            Value.ofInt(10),
            "name2",
            Value.ofText("aaa"),
            "name3",
            Value.ofText("bbb"),
            "name4",
            Value.ofText("ccc"));

    DeleteStatement statement4 =
        DeleteStatement.of(
            "ns",
            "table",
            ImmutableList.of(
                Predicate.column("col1").isEqualTo(BindMarker.named("name1")),
                Predicate.column("col2").isEqualTo(BindMarker.named("name2")),
                Predicate.column("col3").isEqualTo(BindMarker.named("name2"))));
    Map<String, Value> namedValues4 =
        ImmutableMap.of("name1", Value.ofInt(10), "name2", Value.ofText("aaa"));

    // Act
    DeleteStatement actual1 = statement1.bind(namedValues1);
    DeleteStatement actual2 = statement2.bind(namedValues2);
    DeleteStatement actual3 = statement3.bind(namedValues3);
    DeleteStatement actual4 = statement4.bind(namedValues4);

    // Assert
    assertThat(actual1)
        .isEqualTo(
            DeleteStatement.of(
                "ns",
                "table",
                ImmutableList.of(
                    Predicate.column("col1").isEqualTo(Value.ofInt(10)),
                    Predicate.column("col2").isEqualTo(Value.ofText("aaa")),
                    Predicate.column("col3").isEqualTo(Value.ofText("bbb")))));
    assertThat(actual2)
        .isEqualTo(
            DeleteStatement.of(
                "ns",
                "table",
                ImmutableList.of(
                    Predicate.column("col1").isEqualTo(Value.ofInt(10)),
                    Predicate.column("col2").isEqualTo(Value.ofText("aaa")),
                    Predicate.column("col3").isEqualTo(BindMarker.named("name3")))));
    assertThat(actual3)
        .isEqualTo(
            DeleteStatement.of(
                "ns",
                "table",
                ImmutableList.of(
                    Predicate.column("col1").isEqualTo(Value.ofInt(10)),
                    Predicate.column("col2").isEqualTo(Value.ofText("aaa")),
                    Predicate.column("col3").isEqualTo(Value.ofText("bbb")))));
    assertThat(actual4)
        .isEqualTo(
            DeleteStatement.of(
                "ns",
                "table",
                ImmutableList.of(
                    Predicate.column("col1").isEqualTo(Value.ofInt(10)),
                    Predicate.column("col2").isEqualTo(Value.ofText("aaa")),
                    Predicate.column("col3").isEqualTo(Value.ofText("aaa")))));
  }

  @Test
  public void
      bind_positionalValuesGiven_ButStatementHasNamedBindMarker_ShouldThrowIllegalArgumentException() {
    // Arrange
    DeleteStatement statement =
        DeleteStatement.of(
            "ns",
            "table",
            ImmutableList.of(
                Predicate.column("col1").isEqualTo(BindMarker.named("name1")),
                Predicate.column("col2").isEqualTo(BindMarker.named("name2")),
                Predicate.column("col3").isEqualTo(BindMarker.named("name3"))));
    List<Value> positionalValues =
        Arrays.asList(Value.ofInt(10), Value.ofText("aaa"), Value.ofText("bbb"));

    // Act Assert
    assertThatThrownBy(() -> statement.bind(positionalValues))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
