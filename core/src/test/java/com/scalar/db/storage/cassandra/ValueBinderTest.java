package com.scalar.db.storage.cassandra;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;

import com.datastax.driver.core.BoundStatement;
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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Date;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class ValueBinderTest {
  private static final String ANY_NAME = "name";
  private static final boolean ANY_BOOL = true;
  private static final int ANY_INT = 1;
  private static final long ANY_LONG = 1;
  private static final float ANY_FLOAT = 1.0f;
  private static final double ANY_DOUBLE = 1.0;
  private static final String ANY_STRING = "1";
  private static final LocalDate ANY_DATE = LocalDate.ofEpochDay(1);
  private static final LocalTime ANY_TIME = LocalTime.ofSecondOfDay(1);
  private static final LocalDateTime ANY_TIMESTAMP =
      LocalDateTime.of(LocalDate.ofEpochDay(1), LocalTime.ofSecondOfDay(1));
  private static final Instant ANY_TIMESTAMPTZ = Instant.ofEpochSecond(1);

  @Mock private BoundStatement bound;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
  }

  @Test
  public void visit_BooleanColumnAcceptCalled_ShouldCallSetBool() {
    // Arrange
    BooleanColumn column = BooleanColumn.of(ANY_NAME, ANY_BOOL);
    ValueBinder binder = new ValueBinder(bound);

    // Act
    column.accept(binder);

    // Assert
    verify(bound).setBool(0, ANY_BOOL);
  }

  @Test
  public void visit_BooleanColumnWithNullValueAcceptCalled_ShouldCallSetToNull() {
    // Arrange
    BooleanColumn column = BooleanColumn.ofNull(ANY_NAME);
    ValueBinder binder = new ValueBinder(bound);

    // Act
    column.accept(binder);

    // Assert
    verify(bound).setToNull(0);
  }

  @Test
  public void visit_IntColumnAcceptCalled_ShouldCallSetInt() {
    // Arrange
    IntColumn column = IntColumn.of(ANY_NAME, ANY_INT);
    ValueBinder binder = new ValueBinder(bound);

    // Act
    column.accept(binder);

    // Assert
    verify(bound).setInt(0, ANY_INT);
  }

  @Test
  public void visit_IntColumnWithNullValueAcceptCalled_ShouldCallSetToNull() {
    // Arrange
    IntColumn column = IntColumn.ofNull(ANY_NAME);
    ValueBinder binder = new ValueBinder(bound);

    // Act
    column.accept(binder);

    // Assert
    verify(bound).setToNull(0);
  }

  @Test
  public void visit_BigIntColumnAcceptCalled_ShouldCallSetLong() {
    // Arrange
    BigIntColumn column = BigIntColumn.of(ANY_NAME, ANY_LONG);
    ValueBinder binder = new ValueBinder(bound);

    // Act
    column.accept(binder);

    // Assert
    verify(bound).setLong(0, ANY_LONG);
  }

  @Test
  public void visit_BigIntColumnWithNullValueAcceptCalled_ShouldCallSetToNull() {
    // Arrange
    BigIntColumn column = BigIntColumn.ofNull(ANY_NAME);
    ValueBinder binder = new ValueBinder(bound);

    // Act
    column.accept(binder);

    // Assert
    verify(bound).setToNull(0);
  }

  @Test
  public void visit_FloatColumnAcceptCalled_ShouldCallSetFloat() {
    // Arrange
    FloatColumn column = FloatColumn.of(ANY_NAME, ANY_FLOAT);
    ValueBinder binder = new ValueBinder(bound);

    // Act
    column.accept(binder);

    // Assert
    verify(bound).setFloat(0, ANY_FLOAT);
  }

  @Test
  public void visit_FloatColumnWithNullValueAcceptCalled_ShouldCallSetToNull() {
    // Arrange
    FloatColumn column = FloatColumn.ofNull(ANY_NAME);
    ValueBinder binder = new ValueBinder(bound);

    // Act
    column.accept(binder);

    // Assert
    verify(bound).setToNull(0);
  }

  @Test
  public void visit_DoubleColumnAcceptCalled_ShouldCallSetDouble() {
    // Arrange
    DoubleColumn column = DoubleColumn.of(ANY_NAME, ANY_DOUBLE);
    ValueBinder binder = new ValueBinder(bound);

    // Act
    column.accept(binder);

    // Assert
    verify(bound).setDouble(0, ANY_DOUBLE);
  }

  @Test
  public void visit_DoubleColumnWithNullValueAcceptCalled_ShouldCallSetToNull() {
    // Arrange
    DoubleColumn column = DoubleColumn.ofNull(ANY_NAME);
    ValueBinder binder = new ValueBinder(bound);

    // Act
    column.accept(binder);

    // Assert
    verify(bound).setToNull(0);
  }

  @Test
  public void visit_TextColumnAcceptCalled_ShouldCallSetString() {
    // Arrange
    TextColumn column = TextColumn.of(ANY_NAME, ANY_STRING);
    ValueBinder binder = new ValueBinder(bound);

    // Act
    column.accept(binder);

    // Assert
    verify(bound).setString(0, ANY_STRING);
  }

  @Test
  public void visit_TextColumnWithNullValueAcceptCalled_ShouldCallSetToNull() {
    // Arrange
    TextColumn column = TextColumn.ofNull(ANY_NAME);
    ValueBinder binder = new ValueBinder(bound);

    // Act
    column.accept(binder);

    // Assert
    verify(bound).setToNull(0);
  }

  @Test
  public void visit_BlobColumnAcceptCalled_ShouldCallSetString() {
    // Arrange
    BlobColumn column = BlobColumn.of(ANY_NAME, ANY_STRING.getBytes(StandardCharsets.UTF_8));
    ValueBinder binder = new ValueBinder(bound);

    // Act
    column.accept(binder);

    // Assert
    verify(bound)
        .setBytes(
            0,
            (ByteBuffer)
                ByteBuffer.allocate(ANY_STRING.length())
                    .put(ANY_STRING.getBytes(StandardCharsets.UTF_8))
                    .flip());
  }

  @Test
  public void visit_BlobColumnWithNullValueAcceptCalled_ShouldCallSetToNull() {
    // Arrange
    BlobColumn column = BlobColumn.ofNull(ANY_NAME);
    ValueBinder binder = new ValueBinder(bound);

    // Act
    column.accept(binder);

    // Assert
    verify(bound).setToNull(0);
  }

  @Test
  public void visit_DateColumnAcceptCalled_ShouldCallSetString() {
    // Arrange
    DateColumn column = DateColumn.of(ANY_NAME, ANY_DATE);
    ValueBinder binder = new ValueBinder(bound);

    // Act
    column.accept(binder);

    // Assert
    verify(bound)
        .setDate(
            0,
            com.datastax.driver.core.LocalDate.fromYearMonthDay(
                ANY_DATE.getYear(), ANY_DATE.getMonthValue(), ANY_DATE.getDayOfMonth()));
  }

  @Test
  public void visit_DateColumnWithNullValueAcceptCalled_ShouldCallSetToNull() {
    // Arrange
    DateColumn column = DateColumn.ofNull(ANY_NAME);
    ValueBinder binder = new ValueBinder(bound);

    // Act
    column.accept(binder);

    // Assert
    verify(bound).setToNull(0);
  }

  @Test
  public void visit_TimeColumnAcceptCalled_ShouldCallSetString() {
    // Arrange
    TimeColumn column = TimeColumn.of(ANY_NAME, ANY_TIME);
    ValueBinder binder = new ValueBinder(bound);

    // Act
    column.accept(binder);

    // Assert
    verify(bound).setTime(0, ANY_TIME.toNanoOfDay());
  }

  @Test
  public void visit_TimeColumnWithNullValueAcceptCalled_ShouldCallSetToNull() {
    // Arrange
    TimeColumn column = TimeColumn.ofNull(ANY_NAME);
    ValueBinder binder = new ValueBinder(bound);

    // Act
    column.accept(binder);

    // Assert
    verify(bound).setToNull(0);
  }

  @Test
  public void visit_TimestampColumnAcceptCalled_ShouldThrowUnsupportOperationException() {
    // Arrange
    TimestampColumn column = TimestampColumn.of(ANY_NAME, ANY_TIMESTAMP);
    ValueBinder binder = new ValueBinder(bound);

    // Act Assert
    assertThatThrownBy(() -> column.accept(binder))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void visit_TimestampColumnWithNullValueAcceptCalled_ShouldCallSetToNull() {
    // Arrange
    TimestampColumn column = TimestampColumn.ofNull(ANY_NAME);
    ValueBinder binder = new ValueBinder(bound);

    // Act Assert
    assertThatThrownBy(() -> column.accept(binder))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void visit_TimestampTZAcceptCalled_ShouldCallSetString() {
    // Arrange
    TimestampTZColumn column = TimestampTZColumn.of(ANY_NAME, ANY_TIMESTAMPTZ);
    ValueBinder binder = new ValueBinder(bound);

    // Act
    column.accept(binder);

    // Assert
    verify(bound).setTimestamp(0, Date.from(ANY_TIMESTAMPTZ));
  }

  @Test
  public void visit_TimestampTZColumnWithNullValueAcceptCalled_ShouldCallSetToNull() {
    // Arrange
    TimestampTZColumn column = TimestampTZColumn.ofNull(ANY_NAME);
    ValueBinder binder = new ValueBinder(bound);

    // Act
    column.accept(binder);

    // Assert
    verify(bound).setToNull(0);
  }

  @Test
  public void visit_AcceptCalledMultipleTimes_ShouldCallSetWithIncremented() {
    // Arrange
    TextColumn column1 = TextColumn.of(ANY_NAME, ANY_STRING);
    IntColumn column2 = IntColumn.of(ANY_NAME, ANY_INT);
    ValueBinder binder = new ValueBinder(bound);

    // Act
    column1.accept(binder);
    column2.accept(binder);

    // Assert
    verify(bound).setString(0, ANY_STRING);
    verify(bound).setInt(1, ANY_INT);
  }

  @Test
  public void visit_AcceptCalledMultipleTimesWithNullValue_ShouldSetProperly() {
    // Arrange
    IntColumn column1 = IntColumn.of(ANY_NAME, ANY_INT);
    BlobColumn column2 = BlobColumn.ofNull(ANY_NAME);
    TextColumn column3 = TextColumn.ofNull(ANY_NAME);
    IntColumn column4 = IntColumn.of(ANY_NAME, ANY_INT);
    ValueBinder binder = new ValueBinder(bound);

    // Act
    column1.accept(binder);
    column2.accept(binder);
    column3.accept(binder);
    column4.accept(binder);

    // Assert
    verify(bound).setInt(0, ANY_INT);
    verify(bound).setToNull(1);
    verify(bound).setToNull(2);
    verify(bound).setInt(3, ANY_INT);
  }

  @Test
  public void constructor_NullGiven_ShouldThrowNullPointerException() {
    // Act Assert
    assertThatThrownBy(() -> new ValueBinder(null)).isInstanceOf(NullPointerException.class);
  }
}
