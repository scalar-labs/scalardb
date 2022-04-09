package com.scalar.db.sql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.Result;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.ResultImpl;
import com.scalar.db.io.TextColumn;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;

public class ResultIteratorResultSetTest {

  private static final String COLUMN_NAME_1 = "col1";
  private static final String COLUMN_NAME_2 = "col2";
  private static final String COLUMN_NAME_3 = "col3";
  private static final String COLUMN_NAME_4 = "col4";

  private static final com.scalar.db.api.TableMetadata TABLE_METADATA =
      TableMetadata.newBuilder()
          .addColumn(COLUMN_NAME_1, com.scalar.db.io.DataType.TEXT)
          .addColumn(COLUMN_NAME_2, com.scalar.db.io.DataType.TEXT)
          .addColumn(COLUMN_NAME_3, com.scalar.db.io.DataType.TEXT)
          .addColumn(COLUMN_NAME_4, com.scalar.db.io.DataType.TEXT)
          .addPartitionKey(COLUMN_NAME_1)
          .addClusteringKey(COLUMN_NAME_2)
          .build();

  private ResultIteratorResultSet resultIteratorResultSet;

  @Before
  public void setUp() {
    // Arrange
    Result result1 =
        new ResultImpl(
            ImmutableMap.of(
                COLUMN_NAME_1, TextColumn.of(COLUMN_NAME_1, "aaa"),
                COLUMN_NAME_2, TextColumn.of(COLUMN_NAME_2, "bbb"),
                COLUMN_NAME_3, TextColumn.of(COLUMN_NAME_3, "ccc"),
                COLUMN_NAME_4, TextColumn.of(COLUMN_NAME_4, "ddd")),
            TABLE_METADATA);

    Result result2 =
        new ResultImpl(
            ImmutableMap.of(
                COLUMN_NAME_1, TextColumn.of(COLUMN_NAME_1, "aaa"),
                COLUMN_NAME_2, TextColumn.of(COLUMN_NAME_2, "ccc"),
                COLUMN_NAME_3, TextColumn.of(COLUMN_NAME_3, "ddd"),
                COLUMN_NAME_4, TextColumn.of(COLUMN_NAME_4, "eee")),
            TABLE_METADATA);

    Result result3 =
        new ResultImpl(
            ImmutableMap.of(
                COLUMN_NAME_1, TextColumn.of(COLUMN_NAME_1, "aaa"),
                COLUMN_NAME_2, TextColumn.of(COLUMN_NAME_2, "ddd"),
                COLUMN_NAME_3, TextColumn.of(COLUMN_NAME_3, "eee"),
                COLUMN_NAME_4, TextColumn.of(COLUMN_NAME_4, "fff")),
            TABLE_METADATA);

    resultIteratorResultSet =
        new ResultIteratorResultSet(
            Arrays.asList(result1, result2, result3).iterator(),
            ImmutableList.of(
                Projection.column(COLUMN_NAME_1),
                Projection.column(COLUMN_NAME_2),
                Projection.column(COLUMN_NAME_3),
                Projection.column(COLUMN_NAME_4)));
  }

  @Test
  public void one_ShouldReturnCorrectResults() {
    // Arrange

    // Act
    Optional<Record> record1 = resultIteratorResultSet.one();
    Optional<Record> record2 = resultIteratorResultSet.one();
    Optional<Record> record3 = resultIteratorResultSet.one();
    Optional<Record> record4 = resultIteratorResultSet.one();

    // Assert
    assertThat(record1).isPresent();
    assertThat(record1.get().getText(COLUMN_NAME_1)).isEqualTo("aaa");
    assertThat(record1.get().getText(COLUMN_NAME_2)).isEqualTo("bbb");
    assertThat(record1.get().getText(COLUMN_NAME_3)).isEqualTo("ccc");
    assertThat(record1.get().getText(COLUMN_NAME_4)).isEqualTo("ddd");

    assertThat(record2).isPresent();
    assertThat(record2.get().getText(COLUMN_NAME_1)).isEqualTo("aaa");
    assertThat(record2.get().getText(COLUMN_NAME_2)).isEqualTo("ccc");
    assertThat(record2.get().getText(COLUMN_NAME_3)).isEqualTo("ddd");
    assertThat(record2.get().getText(COLUMN_NAME_4)).isEqualTo("eee");

    assertThat(record3).isPresent();
    assertThat(record3.get().getText(COLUMN_NAME_1)).isEqualTo("aaa");
    assertThat(record3.get().getText(COLUMN_NAME_2)).isEqualTo("ddd");
    assertThat(record3.get().getText(COLUMN_NAME_3)).isEqualTo("eee");
    assertThat(record3.get().getText(COLUMN_NAME_4)).isEqualTo("fff");

    assertThat(record4).isNotPresent();
  }

  @Test
  public void all_ShouldReturnCorrectResults() {
    // Arrange

    // Act
    List<Record> records = resultIteratorResultSet.all();

    // Assert
    assertThat(records.size()).isEqualTo(3);

    assertThat(records.get(0).getText(COLUMN_NAME_1)).isEqualTo("aaa");
    assertThat(records.get(0).getText(COLUMN_NAME_2)).isEqualTo("bbb");
    assertThat(records.get(0).getText(COLUMN_NAME_3)).isEqualTo("ccc");
    assertThat(records.get(0).getText(COLUMN_NAME_4)).isEqualTo("ddd");

    assertThat(records.get(1).getText(COLUMN_NAME_1)).isEqualTo("aaa");
    assertThat(records.get(1).getText(COLUMN_NAME_2)).isEqualTo("ccc");
    assertThat(records.get(1).getText(COLUMN_NAME_3)).isEqualTo("ddd");
    assertThat(records.get(1).getText(COLUMN_NAME_4)).isEqualTo("eee");

    assertThat(records.get(2).getText(COLUMN_NAME_1)).isEqualTo("aaa");
    assertThat(records.get(2).getText(COLUMN_NAME_2)).isEqualTo("ddd");
    assertThat(records.get(2).getText(COLUMN_NAME_3)).isEqualTo("eee");
    assertThat(records.get(2).getText(COLUMN_NAME_4)).isEqualTo("fff");
  }

  @Test
  public void iterator_ShouldReturnCorrectResults() {
    // Arrange

    // Act Assert
    Iterator<Record> iterator = resultIteratorResultSet.iterator();

    assertThat(iterator.hasNext()).isTrue();
    Record record = iterator.next();
    assertThat(record.getText(COLUMN_NAME_1)).isEqualTo("aaa");
    assertThat(record.getText(COLUMN_NAME_2)).isEqualTo("bbb");
    assertThat(record.getText(COLUMN_NAME_3)).isEqualTo("ccc");
    assertThat(record.getText(COLUMN_NAME_4)).isEqualTo("ddd");

    assertThat(iterator.hasNext()).isTrue();
    record = iterator.next();
    assertThat(record.getText(COLUMN_NAME_1)).isEqualTo("aaa");
    assertThat(record.getText(COLUMN_NAME_2)).isEqualTo("ccc");
    assertThat(record.getText(COLUMN_NAME_3)).isEqualTo("ddd");
    assertThat(record.getText(COLUMN_NAME_4)).isEqualTo("eee");

    assertThat(iterator.hasNext()).isTrue();
    record = iterator.next();
    assertThat(record.getText(COLUMN_NAME_1)).isEqualTo("aaa");
    assertThat(record.getText(COLUMN_NAME_2)).isEqualTo("ddd");
    assertThat(record.getText(COLUMN_NAME_3)).isEqualTo("eee");
    assertThat(record.getText(COLUMN_NAME_4)).isEqualTo("fff");

    assertThat(iterator.hasNext()).isFalse();
    assertThatThrownBy(iterator::next).isInstanceOf(NoSuchElementException.class);
  }
}
