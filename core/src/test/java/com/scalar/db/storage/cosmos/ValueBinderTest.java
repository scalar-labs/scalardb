package com.scalar.db.storage.cosmos;

import static org.mockito.Mockito.verify;

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
import java.util.Base64;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

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

  @Mock private Consumer<Object> consumer;

  private ValueBinder binder;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
    binder = new ValueBinder();
    binder.set(consumer);
  }

  @Test
  public void visit_BooleanColumn_ShouldCallAccept() {
    // Arrange
    BooleanColumn column = BooleanColumn.of(ANY_NAME, ANY_BOOL);

    // Act
    column.accept(binder);

    // Assert
    verify(consumer).accept(ANY_BOOL);
  }

  @Test
  public void visit_IntColumn_ShouldCallAccept() {
    // Arrange
    IntColumn column = IntColumn.of(ANY_NAME, ANY_INT);

    // Act
    column.accept(binder);

    // Assert
    verify(consumer).accept(ANY_INT);
  }

  @Test
  public void visit_BigIntColumn_ShouldCallAccept() {
    // Arrange
    BigIntColumn column = BigIntColumn.of(ANY_NAME, ANY_LONG);

    // Act
    column.accept(binder);

    // Assert
    verify(consumer).accept(ANY_LONG);
  }

  @Test
  public void visit_FloatColumn_ShouldCallAccept() {
    // Arrange
    FloatColumn column = FloatColumn.of(ANY_NAME, ANY_FLOAT);

    // Act
    column.accept(binder);

    // Assert
    verify(consumer).accept(ANY_FLOAT);
  }

  @Test
  public void visit_DoubleColumn_ShouldCallAccept() {
    // Arrange
    DoubleColumn column = DoubleColumn.of(ANY_NAME, ANY_DOUBLE);

    // Act
    column.accept(binder);

    // Assert
    verify(consumer).accept(ANY_DOUBLE);
  }

  @Test
  public void visit_TextColumn_ShouldCallAccept() {
    // Arrange
    TextColumn column = TextColumn.of(ANY_NAME, ANY_STRING);

    // Act
    column.accept(binder);

    // Assert
    verify(consumer).accept(ANY_STRING);
  }

  @Test
  public void visit_BlobColumn_ShouldCallAccept() {
    // Arrange
    BlobColumn column = BlobColumn.of(ANY_NAME, ANY_STRING.getBytes(StandardCharsets.UTF_8));

    // Act
    column.accept(binder);

    // Assert
    verify(consumer)
        .accept(Base64.getEncoder().encodeToString(ANY_STRING.getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  public void visit_DateColumn_ShouldCallAccept() {
    // Arrange
    DateColumn column = DateColumn.of(ANY_NAME, ANY_DATE);

    // Act
    column.accept(binder);

    // Assert
    verify(consumer).accept(ColumnEncodingUtils.encode(column));
  }

  @Test
  public void visit_TimeColumn_ShouldCallAccept() {
    // Arrange
    TimeColumn column = TimeColumn.of(ANY_NAME, ANY_TIME);

    // Act
    column.accept(binder);

    // Assert
    verify(consumer).accept(ColumnEncodingUtils.encode(column));
  }

  @Test
  public void visit_TimestampColumn_ShouldCallAccept() {
    // Arrange
    TimestampColumn column = TimestampColumn.of(ANY_NAME, ANY_TIMESTAMP);

    // Act
    column.accept(binder);

    // Assert
    verify(consumer).accept(ColumnEncodingUtils.encode(column));
  }

  @Test
  public void visit_TimestampTZColumn_ShouldCallAccept() {
    // Arrange
    TimestampTZColumn column = TimestampTZColumn.of(ANY_NAME, ANY_TIMESTAMPTZ);

    // Act
    column.accept(binder);

    // Assert
    verify(consumer).accept(ColumnEncodingUtils.encode(column));
  }

  @Test
  public void visit_BooleanColumnWithNullValue_ShouldCallAcceptWithNull() {
    // Arrange
    BooleanColumn column = BooleanColumn.ofNull(ANY_NAME);

    // Act
    column.accept(binder);

    // Assert
    verify(consumer).accept(null);
  }

  @Test
  public void visit_IntColumnWithNullValue_ShouldCallAcceptWithNull() {
    // Arrange
    IntColumn column = IntColumn.ofNull(ANY_NAME);

    // Act
    column.accept(binder);

    // Assert
    verify(consumer).accept(null);
  }

  @Test
  public void visit_BigIntColumnWithNullValue_ShouldCallAcceptWithNull() {
    // Arrange
    BigIntColumn column = BigIntColumn.ofNull(ANY_NAME);

    // Act
    column.accept(binder);

    // Assert
    verify(consumer).accept(null);
  }

  @Test
  public void visit_FloatColumnWithNullValue_ShouldCallAcceptWithNull() {
    // Arrange
    FloatColumn column = FloatColumn.ofNull(ANY_NAME);

    // Act
    column.accept(binder);

    // Assert
    verify(consumer).accept(null);
  }

  @Test
  public void visit_DoubleColumnWithNullValue_ShouldCallAcceptWithNull() {
    // Arrange
    DoubleColumn column = DoubleColumn.ofNull(ANY_NAME);

    // Act
    column.accept(binder);

    // Assert
    verify(consumer).accept(null);
  }

  @Test
  public void visit_TextColumnWithNullValue_ShouldCallAcceptWithNull() {
    // Arrange
    TextColumn column = TextColumn.ofNull(ANY_NAME);

    // Act
    column.accept(binder);

    // Assert
    verify(consumer).accept(null);
  }

  @Test
  public void visit_BlobColumnWithNullValue_ShouldCallAcceptWithNull() {
    // Arrange
    BlobColumn column = BlobColumn.ofNull(ANY_NAME);

    // Act
    column.accept(binder);

    // Assert
    verify(consumer).accept(null);
  }

  @Test
  public void visit_DateColumnWithNullValue_ShouldCallAcceptWithNull() {
    // Arrange
    DateColumn column = DateColumn.ofNull(ANY_NAME);

    // Act
    column.accept(binder);

    // Assert
    verify(consumer).accept(null);
  }

  @Test
  public void visit_TimeColumnWithNullValue_ShouldCallAcceptWithNull() {
    // Arrange
    TimeColumn column = TimeColumn.ofNull(ANY_NAME);

    // Act
    column.accept(binder);

    // Assert
    verify(consumer).accept(null);
  }

  @Test
  public void visit_TimestampColumnWithNullValue_ShouldCallAcceptWithNull() {
    // Arrange
    TimestampColumn column = TimestampColumn.ofNull(ANY_NAME);

    // Act
    column.accept(binder);

    // Assert
    verify(consumer).accept(null);
  }

  @Test
  public void visit_TimestampTZColumnWithNullValue_ShouldCallAcceptWithNull() {
    // Arrange
    TimestampTZColumn column = TimestampTZColumn.ofNull(ANY_NAME);

    // Act
    column.accept(binder);

    // Assert
    verify(consumer).accept(null);
  }
}
