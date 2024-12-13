package com.scalar.db.storage.dynamo;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.BlobColumn;
import com.scalar.db.io.BooleanColumn;
import com.scalar.db.io.DateColumn;
import com.scalar.db.io.DoubleColumn;
import com.scalar.db.io.FloatColumn;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.TextColumn;
import com.scalar.db.io.TimeColumn;
import com.scalar.db.io.TimestampColumn;
import com.scalar.db.io.TimestampTZColumn;
import com.scalar.db.storage.ColumnEncodingUtils;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class ValueBinderTest {
  private static final String ANY_NAME = "name";
  private static final boolean ANY_BOOL = true;
  private static final int ANY_INT = 1;
  private static final long ANY_LONG = 1L;
  private static final float ANY_FLOAT = 1.0f;
  private static final double ANY_DOUBLE = 1.0;
  private static final String ANY_STRING = "1";
  private static final LocalDate ANY_DATE = LocalDate.ofEpochDay(1);
  private static final LocalTime ANY_TIME = LocalTime.ofSecondOfDay(1);
  private static final LocalDateTime ANY_TIMESTAMP =
      LocalDateTime.of(LocalDate.ofEpochDay(1), LocalTime.ofSecondOfDay(1));
  private static final Instant ANY_TIMESTAMPTZ = Instant.ofEpochSecond(1);

  private ValueBinder binder;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
    binder = new ValueBinder(":foo");
  }

  @Test
  public void visit_BooleanColumn_ShouldBindAttributeValue() {
    // Arrange
    BooleanColumn column = BooleanColumn.of(ANY_NAME, ANY_BOOL);

    // Act
    column.accept(binder);
    Map<String, AttributeValue> values = binder.build();

    // Assert

    assertThat(values)
        .containsOnly(entry(":foo0", AttributeValue.builder().bool(ANY_BOOL).build()));
  }

  @Test
  public void visit_BooleanColumnWithNullValue_ShouldBindNullAttributeValue() {
    // Arrange
    BooleanColumn column = BooleanColumn.ofNull(ANY_NAME);

    // Act
    column.accept(binder);

    // Assert
    Map<String, AttributeValue> values = binder.build();
    assertThat(values).containsOnly(entry(":foo0", AttributeValue.builder().nul(true).build()));
  }

  @Test
  public void visit_IntColumn_ShouldBindAttributeValue() {
    // Arrange
    IntColumn column = IntColumn.of(ANY_NAME, ANY_INT);

    // Act
    column.accept(binder);

    // Assert
    Map<String, AttributeValue> values = binder.build();
    assertThat(values)
        .containsOnly(entry(":foo0", AttributeValue.builder().n(String.valueOf(ANY_INT)).build()));
  }

  @Test
  public void visit_IntColumnWithNullValue_ShouldBindNullAttributeValue() {
    // Arrange
    IntColumn column = IntColumn.ofNull(ANY_NAME);

    // Act
    column.accept(binder);

    // Assert
    Map<String, AttributeValue> values = binder.build();
    assertThat(values).containsOnly(entry(":foo0", AttributeValue.builder().nul(true).build()));
  }

  @Test
  public void visit_BigIntColumn_ShouldBindAttributeValue() {
    // Arrange
    BigIntColumn column = BigIntColumn.of(ANY_NAME, ANY_LONG);

    // Act
    column.accept(binder);

    // Assert
    Map<String, AttributeValue> values = binder.build();
    assertThat(values)
        .containsOnly(entry(":foo0", AttributeValue.builder().n(String.valueOf(ANY_LONG)).build()));
  }

  @Test
  public void visit_BigIntColumnWithNullValue_ShouldBindNullAttributeValue() {
    // Arrange
    BigIntColumn column = BigIntColumn.ofNull(ANY_NAME);

    // Act
    column.accept(binder);

    // Assert
    Map<String, AttributeValue> values = binder.build();
    assertThat(values).containsOnly(entry(":foo0", AttributeValue.builder().nul(true).build()));
  }

  @Test
  public void visit_FloatColumn_ShouldBindAttributeValue() {
    // Arrange
    FloatColumn column = FloatColumn.of(ANY_NAME, ANY_FLOAT);

    // Act
    column.accept(binder);

    // Assert
    Map<String, AttributeValue> values = binder.build();
    assertThat(values)
        .containsOnly(
            entry(":foo0", AttributeValue.builder().n(String.valueOf(ANY_FLOAT)).build()));
  }

  @Test
  public void visit_FloatColumnWithNullValue_ShouldBindNullAttributeValue() {
    // Arrange
    FloatColumn column = FloatColumn.ofNull(ANY_NAME);

    // Act
    column.accept(binder);

    // Assert
    Map<String, AttributeValue> values = binder.build();
    assertThat(values).containsOnly(entry(":foo0", AttributeValue.builder().nul(true).build()));
  }

  @Test
  public void visit_DoubleColumn_ShouldBindAttributeValue() {
    // Arrange
    DoubleColumn column = DoubleColumn.of(ANY_NAME, ANY_DOUBLE);

    // Act
    column.accept(binder);

    // Assert
    Map<String, AttributeValue> values = binder.build();
    assertThat(values)
        .containsOnly(
            entry(":foo0", AttributeValue.builder().n(String.valueOf(ANY_DOUBLE)).build()));
  }

  @Test
  public void visit_DoubleColumnWithNullValue_ShouldBindNullAttributeValue() {
    // Arrange
    DoubleColumn column = DoubleColumn.ofNull(ANY_NAME);

    // Act
    column.accept(binder);

    // Assert
    Map<String, AttributeValue> values = binder.build();
    assertThat(values).containsOnly(entry(":foo0", AttributeValue.builder().nul(true).build()));
  }

  @Test
  public void visit_TextColumn_ShouldBindAttributeValue() {
    // Arrange
    TextColumn column = TextColumn.of(ANY_NAME, ANY_STRING);

    // Act
    column.accept(binder);

    // Assert
    Map<String, AttributeValue> values = binder.build();
    assertThat(values).containsOnly(entry(":foo0", AttributeValue.builder().s(ANY_STRING).build()));
  }

  @Test
  public void visit_TextColumnWithNullValue_ShouldBindNullAttributeValue() {
    // Arrange
    TextColumn column = TextColumn.ofNull(ANY_NAME);

    // Act
    column.accept(binder);

    // Assert
    Map<String, AttributeValue> values = binder.build();
    assertThat(values).containsOnly(entry(":foo0", AttributeValue.builder().nul(true).build()));
  }

  @Test
  public void visit_BlobColumn_ShouldBindAttributeValue() {
    // Arrange
    BlobColumn column = BlobColumn.of(ANY_NAME, ANY_STRING.getBytes(StandardCharsets.UTF_8));

    // Act
    column.accept(binder);

    // Assert
    Map<String, AttributeValue> values = binder.build();
    assertThat(values)
        .containsOnly(
            entry(
                ":foo0",
                AttributeValue.builder()
                    .b(SdkBytes.fromByteArray(ANY_STRING.getBytes(StandardCharsets.UTF_8)))
                    .build()));
  }

  @Test
  public void visit_BlobColumnWithNullValue_ShouldBindNullAttributeValue() {
    // Arrange
    BlobColumn column = BlobColumn.ofNull(ANY_NAME);

    // Act
    column.accept(binder);

    // Assert
    Map<String, AttributeValue> values = binder.build();
    assertThat(values).containsOnly(entry(":foo0", AttributeValue.builder().nul(true).build()));
  }

  @Test
  public void visit_DateColumn_ShouldBindAttributeValue() {
    // Arrange
    DateColumn column = DateColumn.of(ANY_NAME, ANY_DATE);

    // Act
    column.accept(binder);

    // Assert
    Map<String, AttributeValue> values = binder.build();
    assertThat(values)
        .containsOnly(
            entry(
                ":foo0",
                AttributeValue.builder()
                    .n(String.valueOf(ColumnEncodingUtils.encode(column)))
                    .build()));
  }

  @Test
  public void visit_DateColumnWithNullValue_ShouldBindNullAttributeValue() {
    // Arrange
    DateColumn column = DateColumn.ofNull(ANY_NAME);

    // Act
    column.accept(binder);

    // Assert
    Map<String, AttributeValue> values = binder.build();
    assertThat(values).containsOnly(entry(":foo0", AttributeValue.builder().nul(true).build()));
  }

  @Test
  public void visit_TimeColumn_ShouldBindAttributeValue() {
    // Arrange
    TimeColumn column = TimeColumn.of(ANY_NAME, ANY_TIME);

    // Act
    column.accept(binder);

    // Assert
    Map<String, AttributeValue> values = binder.build();
    assertThat(values)
        .containsOnly(
            entry(
                ":foo0",
                AttributeValue.builder()
                    .n(String.valueOf(ColumnEncodingUtils.encode(column)))
                    .build()));
  }

  @Test
  public void visit_TimeColumnWithNullValue_ShouldBindNullAttributeValue() {
    // Arrange
    TimeColumn column = TimeColumn.ofNull(ANY_NAME);

    // Act
    column.accept(binder);

    // Assert
    Map<String, AttributeValue> values = binder.build();
    assertThat(values).containsOnly(entry(":foo0", AttributeValue.builder().nul(true).build()));
  }

  @Test
  public void visit_TimestampColumn_ShouldBindAttributeValue() {
    // Arrange
    TimestampColumn column = TimestampColumn.of(ANY_NAME, ANY_TIMESTAMP);

    // Act
    column.accept(binder);

    // Assert
    Map<String, AttributeValue> values = binder.build();
    assertThat(values)
        .containsOnly(
            entry(":foo0", AttributeValue.builder().s(ColumnEncodingUtils.encode(column)).build()));
  }

  @Test
  public void visit_TimestampColumnWithNullValue_ShouldBindNullAttributeValue() {
    // Arrange
    TimestampColumn column = TimestampColumn.ofNull(ANY_NAME);

    // Act
    column.accept(binder);

    // Assert
    Map<String, AttributeValue> values = binder.build();
    assertThat(values).containsOnly(entry(":foo0", AttributeValue.builder().nul(true).build()));
  }

  @Test
  public void visit_TimestampTZColumn_ShouldBindAttributeValue() {
    // Arrange
    TimestampTZColumn column = TimestampTZColumn.of(ANY_NAME, ANY_TIMESTAMPTZ);

    // Act
    column.accept(binder);

    // Assert
    Map<String, AttributeValue> values = binder.build();
    assertThat(values)
        .containsOnly(
            entry(":foo0", AttributeValue.builder().s(ColumnEncodingUtils.encode(column)).build()));
  }

  @Test
  public void visit_TimestampTZColumnWithNullValue_ShouldBindNullAttributeValue() {
    // Arrange
    TimestampTZColumn column = TimestampTZColumn.ofNull(ANY_NAME);

    // Act
    column.accept(binder);

    // Assert
    Map<String, AttributeValue> values = binder.build();
    assertThat(values).containsOnly(entry(":foo0", AttributeValue.builder().nul(true).build()));
  }

  @Test
  public void visit_SeveralColumns_ShouldBindAllColumnsCorrectly() {
    // Arrange
    TimestampTZColumn timestampTZColumn = TimestampTZColumn.of(ANY_NAME, ANY_TIMESTAMPTZ);
    BooleanColumn booleanColumn = BooleanColumn.of(ANY_NAME, ANY_BOOL);
    FloatColumn floatColumn = FloatColumn.ofNull(ANY_NAME);
    BigIntColumn bigIntColumn = BigIntColumn.of(ANY_NAME, ANY_LONG);

    // Act
    timestampTZColumn.accept(binder);
    booleanColumn.accept(binder);
    floatColumn.accept(binder);
    bigIntColumn.accept(binder);

    // Assert
    Map<String, AttributeValue> values = binder.build();
    assertThat(values)
        .containsOnly(
            entry(
                ":foo0",
                AttributeValue.builder().s(ColumnEncodingUtils.encode(timestampTZColumn)).build()),
            entry(":foo1", AttributeValue.builder().bool(ANY_BOOL).build()),
            entry(":foo2", AttributeValue.builder().nul(true).build()),
            entry(":foo3", AttributeValue.builder().n(String.valueOf(ANY_LONG)).build()));
  }
}
