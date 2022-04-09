package com.scalar.db.sql.statement;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.scalar.db.sql.Assignment;
import com.scalar.db.sql.BindMarker;
import com.scalar.db.sql.Value;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.Test;

@SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_INFERRED")
public class InsertStatementTest {

  @Test
  public void bind_positionalValuesGiven_ShouldBindProperly() {
    // Arrange
    InsertStatement statement1 =
        InsertStatement.of(
            "ns",
            "table",
            ImmutableList.of(
                Assignment.column("col1").value(BindMarker.of()),
                Assignment.column("col2").value(BindMarker.of()),
                Assignment.column("col3").value(BindMarker.of())));
    List<Value> positionalValues1 =
        Arrays.asList(Value.ofInt(10), Value.ofText("aaa"), Value.ofText("bbb"));

    InsertStatement statement2 =
        InsertStatement.of(
            "ns",
            "table",
            ImmutableList.of(
                Assignment.column("col1").value(BindMarker.of()),
                Assignment.column("col2").value(BindMarker.of()),
                Assignment.column("col3").value(BindMarker.of())));
    List<Value> positionalValues2 = Arrays.asList(Value.ofInt(10), Value.ofText("aaa"));

    InsertStatement statement3 =
        InsertStatement.of(
            "ns",
            "table",
            ImmutableList.of(
                Assignment.column("col1").value(BindMarker.of()),
                Assignment.column("col2").value(BindMarker.of()),
                Assignment.column("col3").value(BindMarker.of())));
    List<Value> positionalValues3 =
        Arrays.asList(
            Value.ofInt(10), Value.ofText("aaa"), Value.ofText("bbb"), Value.ofText("ccc"));

    // Act
    InsertStatement actual1 = statement1.bind(positionalValues1);
    InsertStatement actual2 = statement2.bind(positionalValues2);
    InsertStatement actual3 = statement3.bind(positionalValues3);

    // Assert
    assertThat(actual1)
        .isEqualTo(
            InsertStatement.of(
                "ns",
                "table",
                ImmutableList.of(
                    Assignment.column("col1").value(Value.ofInt(10)),
                    Assignment.column("col2").value(Value.ofText("aaa")),
                    Assignment.column("col3").value(Value.ofText("bbb")))));
    assertThat(actual2)
        .isEqualTo(
            InsertStatement.of(
                "ns",
                "table",
                ImmutableList.of(
                    Assignment.column("col1").value(Value.ofInt(10)),
                    Assignment.column("col2").value(Value.ofText("aaa")),
                    Assignment.column("col3").value(BindMarker.of()))));
    assertThat(actual3)
        .isEqualTo(
            InsertStatement.of(
                "ns",
                "table",
                ImmutableList.of(
                    Assignment.column("col1").value(Value.ofInt(10)),
                    Assignment.column("col2").value(Value.ofText("aaa")),
                    Assignment.column("col3").value(Value.ofText("bbb")))));
  }

  @Test
  public void
      bind_namedValuesGiven_ButStatementHasPositionalBindMarker_ShouldThrowIllegalArgumentException() {
    // Arrange
    InsertStatement statement =
        InsertStatement.of(
            "ns",
            "table",
            ImmutableList.of(
                Assignment.column("col1").value(BindMarker.of()),
                Assignment.column("col2").value(BindMarker.of()),
                Assignment.column("col3").value(BindMarker.of())));
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
    InsertStatement statement1 =
        InsertStatement.of(
            "ns",
            "table",
            ImmutableList.of(
                Assignment.column("col1").value(BindMarker.of("name1")),
                Assignment.column("col2").value(BindMarker.of("name2")),
                Assignment.column("col3").value(BindMarker.of("name3"))));
    Map<String, Value> namedValues1 =
        ImmutableMap.of(
            "name1", Value.ofInt(10), "name2", Value.ofText("aaa"), "name3", Value.ofText("bbb"));

    InsertStatement statement2 =
        InsertStatement.of(
            "ns",
            "table",
            ImmutableList.of(
                Assignment.column("col1").value(BindMarker.of("name1")),
                Assignment.column("col2").value(BindMarker.of("name2")),
                Assignment.column("col3").value(BindMarker.of("name3"))));
    Map<String, Value> namedValues2 =
        ImmutableMap.of("name1", Value.ofInt(10), "name2", Value.ofText("aaa"));

    InsertStatement statement3 =
        InsertStatement.of(
            "ns",
            "table",
            ImmutableList.of(
                Assignment.column("col1").value(BindMarker.of("name1")),
                Assignment.column("col2").value(BindMarker.of("name2")),
                Assignment.column("col3").value(BindMarker.of("name3"))));
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

    InsertStatement statement4 =
        InsertStatement.of(
            "ns",
            "table",
            ImmutableList.of(
                Assignment.column("col1").value(BindMarker.of("name1")),
                Assignment.column("col2").value(BindMarker.of("name2")),
                Assignment.column("col3").value(BindMarker.of("name2"))));
    Map<String, Value> namedValues4 =
        ImmutableMap.of("name1", Value.ofInt(10), "name2", Value.ofText("aaa"));

    // Act
    InsertStatement actual1 = statement1.bind(namedValues1);
    InsertStatement actual2 = statement2.bind(namedValues2);
    InsertStatement actual3 = statement3.bind(namedValues3);
    InsertStatement actual4 = statement4.bind(namedValues4);

    // Assert
    assertThat(actual1)
        .isEqualTo(
            InsertStatement.of(
                "ns",
                "table",
                ImmutableList.of(
                    Assignment.column("col1").value(Value.ofInt(10)),
                    Assignment.column("col2").value(Value.ofText("aaa")),
                    Assignment.column("col3").value(Value.ofText("bbb")))));
    assertThat(actual2)
        .isEqualTo(
            InsertStatement.of(
                "ns",
                "table",
                ImmutableList.of(
                    Assignment.column("col1").value(Value.ofInt(10)),
                    Assignment.column("col2").value(Value.ofText("aaa")),
                    Assignment.column("col3").value(BindMarker.of("name3")))));
    assertThat(actual3)
        .isEqualTo(
            InsertStatement.of(
                "ns",
                "table",
                ImmutableList.of(
                    Assignment.column("col1").value(Value.ofInt(10)),
                    Assignment.column("col2").value(Value.ofText("aaa")),
                    Assignment.column("col3").value(Value.ofText("bbb")))));
    assertThat(actual4)
        .isEqualTo(
            InsertStatement.of(
                "ns",
                "table",
                ImmutableList.of(
                    Assignment.column("col1").value(Value.ofInt(10)),
                    Assignment.column("col2").value(Value.ofText("aaa")),
                    Assignment.column("col3").value(Value.ofText("aaa")))));
  }

  @Test
  public void
      bind_positionalValuesGiven_ButStatementHasNamedBindMarker_ShouldThrowIllegalArgumentException() {
    // Arrange
    InsertStatement statement =
        InsertStatement.of(
            "ns",
            "table",
            ImmutableList.of(
                Assignment.column("col1").value(BindMarker.of("name1")),
                Assignment.column("col2").value(BindMarker.of("name2")),
                Assignment.column("col3").value(BindMarker.of("name3"))));
    List<Value> positionalValues =
        Arrays.asList(Value.ofInt(10), Value.ofText("aaa"), Value.ofText("bbb"));

    // Act Assert
    assertThatThrownBy(() -> statement.bind(positionalValues))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
