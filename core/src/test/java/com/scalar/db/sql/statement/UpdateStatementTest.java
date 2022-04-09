package com.scalar.db.sql.statement;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.scalar.db.sql.Assignment;
import com.scalar.db.sql.BindMarker;
import com.scalar.db.sql.Predicate;
import com.scalar.db.sql.Value;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.Test;

@SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_INFERRED")
public class UpdateStatementTest {

  @Test
  public void bind_positionalValuesGiven_ShouldBindProperly() {
    // Arrange
    UpdateStatement statement1 =
        UpdateStatement.of(
            "ns",
            "table",
            ImmutableList.of(
                Assignment.column("col4").value(BindMarker.of()),
                Assignment.column("col5").value(BindMarker.of()),
                Assignment.column("col6").value(BindMarker.of())),
            ImmutableList.of(
                Predicate.column("col1").isEqualTo(BindMarker.of()),
                Predicate.column("col2").isEqualTo(BindMarker.of()),
                Predicate.column("col3").isEqualTo(BindMarker.of())));
    List<Value> positionalValues1 =
        Arrays.asList(
            Value.ofBoolean(true),
            Value.ofFloat(1.23F),
            Value.ofDouble(4.56),
            Value.ofInt(10),
            Value.ofText("aaa"),
            Value.ofBigInt(100L));

    UpdateStatement statement2 =
        UpdateStatement.of(
            "ns",
            "table",
            ImmutableList.of(
                Assignment.column("col4").value(BindMarker.of()),
                Assignment.column("col5").value(BindMarker.of()),
                Assignment.column("col6").value(BindMarker.of())),
            ImmutableList.of(
                Predicate.column("col1").isEqualTo(BindMarker.of()),
                Predicate.column("col2").isEqualTo(BindMarker.of()),
                Predicate.column("col3").isEqualTo(BindMarker.of())));
    List<Value> positionalValues2 =
        Arrays.asList(
            Value.ofBoolean(true),
            Value.ofFloat(1.23F),
            Value.ofDouble(4.56),
            Value.ofInt(10),
            Value.ofText("aaa"));

    UpdateStatement statement3 =
        UpdateStatement.of(
            "ns",
            "table",
            ImmutableList.of(
                Assignment.column("col4").value(BindMarker.of()),
                Assignment.column("col5").value(BindMarker.of()),
                Assignment.column("col6").value(BindMarker.of())),
            ImmutableList.of(
                Predicate.column("col1").isEqualTo(BindMarker.of()),
                Predicate.column("col2").isEqualTo(BindMarker.of()),
                Predicate.column("col3").isEqualTo(BindMarker.of())));
    List<Value> positionalValues3 = Arrays.asList(Value.ofBoolean(true), Value.ofFloat(1.23F));

    UpdateStatement statement4 =
        UpdateStatement.of(
            "ns",
            "table",
            ImmutableList.of(
                Assignment.column("col4").value(BindMarker.of()),
                Assignment.column("col5").value(BindMarker.of()),
                Assignment.column("col6").value(BindMarker.of())),
            ImmutableList.of(
                Predicate.column("col1").isEqualTo(BindMarker.of()),
                Predicate.column("col2").isEqualTo(BindMarker.of()),
                Predicate.column("col3").isEqualTo(BindMarker.of())));
    List<Value> positionalValues4 =
        Arrays.asList(
            Value.ofBoolean(true),
            Value.ofFloat(1.23F),
            Value.ofDouble(4.56),
            Value.ofInt(10),
            Value.ofText("aaa"),
            Value.ofBigInt(100L),
            Value.ofText("bbb"));

    // Act
    UpdateStatement actual1 = statement1.bind(positionalValues1);
    UpdateStatement actual2 = statement2.bind(positionalValues2);
    UpdateStatement actual3 = statement3.bind(positionalValues3);
    UpdateStatement actual4 = statement4.bind(positionalValues4);

    // Assert
    assertThat(actual1)
        .isEqualTo(
            UpdateStatement.of(
                "ns",
                "table",
                ImmutableList.of(
                    Assignment.column("col4").value(Value.ofBoolean(true)),
                    Assignment.column("col5").value(Value.ofFloat(1.23F)),
                    Assignment.column("col6").value(Value.ofDouble(4.56))),
                ImmutableList.of(
                    Predicate.column("col1").isEqualTo(Value.ofInt(10)),
                    Predicate.column("col2").isEqualTo(Value.ofText("aaa")),
                    Predicate.column("col3").isEqualTo(Value.ofBigInt(100L)))));
    assertThat(actual2)
        .isEqualTo(
            UpdateStatement.of(
                "ns",
                "table",
                ImmutableList.of(
                    Assignment.column("col4").value(Value.ofBoolean(true)),
                    Assignment.column("col5").value(Value.ofFloat(1.23F)),
                    Assignment.column("col6").value(Value.ofDouble(4.56))),
                ImmutableList.of(
                    Predicate.column("col1").isEqualTo(Value.ofInt(10)),
                    Predicate.column("col2").isEqualTo(Value.ofText("aaa")),
                    Predicate.column("col3").isEqualTo(BindMarker.of()))));
    assertThat(actual3)
        .isEqualTo(
            UpdateStatement.of(
                "ns",
                "table",
                ImmutableList.of(
                    Assignment.column("col4").value(Value.ofBoolean(true)),
                    Assignment.column("col5").value(Value.ofFloat(1.23F)),
                    Assignment.column("col6").value(BindMarker.of())),
                ImmutableList.of(
                    Predicate.column("col1").isEqualTo(BindMarker.of()),
                    Predicate.column("col2").isEqualTo(BindMarker.of()),
                    Predicate.column("col3").isEqualTo(BindMarker.of()))));
    assertThat(actual4)
        .isEqualTo(
            UpdateStatement.of(
                "ns",
                "table",
                ImmutableList.of(
                    Assignment.column("col4").value(Value.ofBoolean(true)),
                    Assignment.column("col5").value(Value.ofFloat(1.23F)),
                    Assignment.column("col6").value(Value.ofDouble(4.56))),
                ImmutableList.of(
                    Predicate.column("col1").isEqualTo(Value.ofInt(10)),
                    Predicate.column("col2").isEqualTo(Value.ofText("aaa")),
                    Predicate.column("col3").isEqualTo(Value.ofBigInt(100L)))));
  }

  @Test
  public void
      bind_namedValuesGiven_ButStatementHasPositionalBindMarker_ShouldThrowIllegalArgumentException() {
    // Arrange
    UpdateStatement statement =
        UpdateStatement.of(
            "ns",
            "table",
            ImmutableList.of(
                Assignment.column("col4").value(BindMarker.of()),
                Assignment.column("col5").value(BindMarker.of()),
                Assignment.column("col6").value(BindMarker.of())),
            ImmutableList.of(
                Predicate.column("col1").isEqualTo(BindMarker.of()),
                Predicate.column("col2").isEqualTo(BindMarker.of()),
                Predicate.column("col3").isEqualTo(BindMarker.of())));
    Map<String, Value> namedValues =
        ImmutableMap.<String, Value>builder()
            .put("name1", Value.ofBoolean(true))
            .put("name2", Value.ofFloat(1.23F))
            .put("name3", Value.ofDouble(4.56))
            .put("name4", Value.ofInt(10))
            .put("name5", Value.ofText("aaa"))
            .put("name6", Value.ofBigInt(100L))
            .build();

    // Act Assert
    assertThatThrownBy(() -> statement.bind(namedValues))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void bind_namedValuesGiven_ShouldBindProperly() {
    // Arrange
    UpdateStatement statement1 =
        UpdateStatement.of(
            "ns",
            "table",
            ImmutableList.of(
                Assignment.column("col4").value(BindMarker.of("name1")),
                Assignment.column("col5").value(BindMarker.of("name2")),
                Assignment.column("col6").value(BindMarker.of("name3"))),
            ImmutableList.of(
                Predicate.column("col1").isEqualTo(BindMarker.of("name4")),
                Predicate.column("col2").isEqualTo(BindMarker.of("name5")),
                Predicate.column("col3").isEqualTo(BindMarker.of("name6"))));
    Map<String, Value> namedValues1 =
        ImmutableMap.<String, Value>builder()
            .put("name1", Value.ofBoolean(true))
            .put("name2", Value.ofFloat(1.23F))
            .put("name3", Value.ofDouble(4.56))
            .put("name4", Value.ofInt(10))
            .put("name5", Value.ofText("aaa"))
            .put("name6", Value.ofBigInt(100L))
            .build();

    UpdateStatement statement2 =
        UpdateStatement.of(
            "ns",
            "table",
            ImmutableList.of(
                Assignment.column("col4").value(BindMarker.of("name1")),
                Assignment.column("col5").value(BindMarker.of("name2")),
                Assignment.column("col6").value(BindMarker.of("name3"))),
            ImmutableList.of(
                Predicate.column("col1").isEqualTo(BindMarker.of("name4")),
                Predicate.column("col2").isEqualTo(BindMarker.of("name5")),
                Predicate.column("col3").isEqualTo(BindMarker.of("name6"))));
    Map<String, Value> namedValues2 =
        ImmutableMap.<String, Value>builder()
            .put("name1", Value.ofBoolean(true))
            .put("name2", Value.ofFloat(1.23F))
            .put("name3", Value.ofDouble(4.56))
            .put("name4", Value.ofInt(10))
            .put("name5", Value.ofText("aaa"))
            .build();

    UpdateStatement statement3 =
        UpdateStatement.of(
            "ns",
            "table",
            ImmutableList.of(
                Assignment.column("col4").value(BindMarker.of("name1")),
                Assignment.column("col5").value(BindMarker.of("name2")),
                Assignment.column("col6").value(BindMarker.of("name3"))),
            ImmutableList.of(
                Predicate.column("col1").isEqualTo(BindMarker.of("name4")),
                Predicate.column("col2").isEqualTo(BindMarker.of("name5")),
                Predicate.column("col3").isEqualTo(BindMarker.of("name6"))));
    Map<String, Value> namedValues3 =
        ImmutableMap.of("name1", Value.ofBoolean(true), "name2", Value.ofFloat(1.23F));

    UpdateStatement statement4 =
        UpdateStatement.of(
            "ns",
            "table",
            ImmutableList.of(
                Assignment.column("col4").value(BindMarker.of("name1")),
                Assignment.column("col5").value(BindMarker.of("name2")),
                Assignment.column("col6").value(BindMarker.of("name3"))),
            ImmutableList.of(
                Predicate.column("col1").isEqualTo(BindMarker.of("name4")),
                Predicate.column("col2").isEqualTo(BindMarker.of("name5")),
                Predicate.column("col3").isEqualTo(BindMarker.of("name6"))));
    Map<String, Value> namedValues4 =
        ImmutableMap.<String, Value>builder()
            .put("name1", Value.ofBoolean(true))
            .put("name2", Value.ofFloat(1.23F))
            .put("name3", Value.ofDouble(4.56))
            .put("name4", Value.ofInt(10))
            .put("name5", Value.ofText("aaa"))
            .put("name6", Value.ofBigInt(100L))
            .put("name7", Value.ofText("bbb"))
            .build();

    UpdateStatement statement5 =
        UpdateStatement.of(
            "ns",
            "table",
            ImmutableList.of(
                Assignment.column("col4").value(BindMarker.of("name1")),
                Assignment.column("col5").value(BindMarker.of("name2")),
                Assignment.column("col6").value(BindMarker.of("name3"))),
            ImmutableList.of(
                Predicate.column("col1").isEqualTo(BindMarker.of("name1")),
                Predicate.column("col2").isEqualTo(BindMarker.of("name2")),
                Predicate.column("col3").isEqualTo(BindMarker.of("name3"))));
    Map<String, Value> namedValues5 =
        ImmutableMap.<String, Value>builder()
            .put("name1", Value.ofInt(10))
            .put("name2", Value.ofText("aaa"))
            .put("name3", Value.ofBigInt(100L))
            .build();

    // Act
    UpdateStatement actual1 = statement1.bind(namedValues1);
    UpdateStatement actual2 = statement2.bind(namedValues2);
    UpdateStatement actual3 = statement3.bind(namedValues3);
    UpdateStatement actual4 = statement4.bind(namedValues4);
    UpdateStatement actual5 = statement5.bind(namedValues5);

    // Assert
    assertThat(actual1)
        .isEqualTo(
            UpdateStatement.of(
                "ns",
                "table",
                ImmutableList.of(
                    Assignment.column("col4").value(Value.ofBoolean(true)),
                    Assignment.column("col5").value(Value.ofFloat(1.23F)),
                    Assignment.column("col6").value(Value.ofDouble(4.56))),
                ImmutableList.of(
                    Predicate.column("col1").isEqualTo(Value.ofInt(10)),
                    Predicate.column("col2").isEqualTo(Value.ofText("aaa")),
                    Predicate.column("col3").isEqualTo(Value.ofBigInt(100L)))));
    assertThat(actual2)
        .isEqualTo(
            UpdateStatement.of(
                "ns",
                "table",
                ImmutableList.of(
                    Assignment.column("col4").value(Value.ofBoolean(true)),
                    Assignment.column("col5").value(Value.ofFloat(1.23F)),
                    Assignment.column("col6").value(Value.ofDouble(4.56))),
                ImmutableList.of(
                    Predicate.column("col1").isEqualTo(Value.ofInt(10)),
                    Predicate.column("col2").isEqualTo(Value.ofText("aaa")),
                    Predicate.column("col3").isEqualTo(BindMarker.of("name6")))));
    assertThat(actual3)
        .isEqualTo(
            UpdateStatement.of(
                "ns",
                "table",
                ImmutableList.of(
                    Assignment.column("col4").value(Value.ofBoolean(true)),
                    Assignment.column("col5").value(Value.ofFloat(1.23F)),
                    Assignment.column("col6").value(BindMarker.of("name3"))),
                ImmutableList.of(
                    Predicate.column("col1").isEqualTo(BindMarker.of("name4")),
                    Predicate.column("col2").isEqualTo(BindMarker.of("name5")),
                    Predicate.column("col3").isEqualTo(BindMarker.of("name6")))));
    assertThat(actual4)
        .isEqualTo(
            UpdateStatement.of(
                "ns",
                "table",
                ImmutableList.of(
                    Assignment.column("col4").value(Value.ofBoolean(true)),
                    Assignment.column("col5").value(Value.ofFloat(1.23F)),
                    Assignment.column("col6").value(Value.ofDouble(4.56))),
                ImmutableList.of(
                    Predicate.column("col1").isEqualTo(Value.ofInt(10)),
                    Predicate.column("col2").isEqualTo(Value.ofText("aaa")),
                    Predicate.column("col3").isEqualTo(Value.ofBigInt(100L)))));
    assertThat(actual5)
        .isEqualTo(
            UpdateStatement.of(
                "ns",
                "table",
                ImmutableList.of(
                    Assignment.column("col4").value(Value.ofInt(10)),
                    Assignment.column("col5").value(Value.ofText("aaa")),
                    Assignment.column("col6").value(Value.ofBigInt(100L))),
                ImmutableList.of(
                    Predicate.column("col1").isEqualTo(Value.ofInt(10)),
                    Predicate.column("col2").isEqualTo(Value.ofText("aaa")),
                    Predicate.column("col3").isEqualTo(Value.ofBigInt(100L)))));
  }

  @Test
  public void
      bind_positionalValuesGiven_ButStatementHasNamedBindMarker_ShouldThrowIllegalArgumentException() {
    // Arrange
    UpdateStatement statement =
        UpdateStatement.of(
            "ns",
            "table",
            ImmutableList.of(
                Assignment.column("col4").value(BindMarker.of("name1")),
                Assignment.column("col5").value(BindMarker.of("name2")),
                Assignment.column("col6").value(BindMarker.of("name3"))),
            ImmutableList.of(
                Predicate.column("col1").isEqualTo(BindMarker.of("name4")),
                Predicate.column("col2").isEqualTo(BindMarker.of("name5")),
                Predicate.column("col3").isEqualTo(BindMarker.of("name6"))));
    List<Value> positionalValues =
        Arrays.asList(
            Value.ofBoolean(true),
            Value.ofFloat(1.23F),
            Value.ofDouble(4.56),
            Value.ofInt(10),
            Value.ofText("aaa"),
            Value.ofBigInt(100L));

    // Act Assert
    assertThatThrownBy(() -> statement.bind(positionalValues))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
