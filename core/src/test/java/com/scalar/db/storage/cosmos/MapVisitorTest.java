package com.scalar.db.storage.cosmos;

import static org.assertj.core.api.Assertions.assertThat;

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
import com.scalar.db.storage.TimeRelatedColumnEncodingUtils;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class MapVisitorTest {
  private static final boolean ANY_BOOLEAN = false;
  private static final BooleanColumn ANY_BOOLEAN_COLUMN =
      BooleanColumn.of("any_boolean", ANY_BOOLEAN);
  private static final int ANY_INT = Integer.MIN_VALUE;
  private static final IntColumn ANY_INT_COLUMN = IntColumn.of("any_int", ANY_INT);
  private static final long ANY_BIGINT = BigIntColumn.MAX_VALUE;
  private static final BigIntColumn ANY_BIGINT_COLUMN = BigIntColumn.of("any_bigint", ANY_BIGINT);
  private static final float ANY_FLOAT = Float.MIN_NORMAL;
  private static final FloatColumn ANY_FLOAT_COLUMN = FloatColumn.of("any_float", ANY_FLOAT);
  private static final double ANY_DOUBLE = Double.MIN_NORMAL;
  private static final DoubleColumn ANY_DOUBLE_COLUMN = DoubleColumn.of("any_double", ANY_DOUBLE);
  private static final String ANY_TEXT = "test";
  private static final TextColumn ANY_TEXT_COLUMN = TextColumn.of("any_text", ANY_TEXT);
  private static final byte[] ANY_BLOB = ANY_TEXT.getBytes(StandardCharsets.UTF_8);
  private static final BlobColumn ANY_BLOB_COLUMN = BlobColumn.of("any_blob", ANY_BLOB);
  private static final LocalDate ANY_DATE = DateColumn.MAX_VALUE;
  private static final DateColumn ANY_DATE_COLUMN = DateColumn.of("any_date", ANY_DATE);
  private static final LocalTime ANY_TIME = TimeColumn.MAX_VALUE;
  private static final TimeColumn ANY_TIME_COLUMN = TimeColumn.of("any_time", ANY_TIME);
  private static final LocalDateTime ANY_TIMESTAMP = TimestampColumn.MAX_VALUE;
  private static final TimestampColumn ANY_TIMESTAMP_COLUMN =
      TimestampColumn.of("any_timestamp", ANY_TIMESTAMP);
  private static final Instant ANY_TIMESTAMPTZ = TimestampTZColumn.MAX_VALUE;
  private static final TimestampTZColumn ANY_TIMESTAMPTZ_COLUMN =
      TimestampTZColumn.of("any_timestamptz", ANY_TIMESTAMPTZ);

  private MapVisitor visitor;

  @BeforeEach
  public void setUp() {
    visitor = new MapVisitor();
  }

  @Test
  public void visit_BooleanColumnAcceptCalled_ShouldGetMap() {
    // Act
    ANY_BOOLEAN_COLUMN.accept(visitor);

    // Assert
    assertThat(visitor.get().get(ANY_BOOLEAN_COLUMN.getName())).isEqualTo(ANY_BOOLEAN);
  }

  @Test
  public void visit_BooleanColumnWithNullValueAcceptCalled_ShouldGetMap() {
    // Act
    BooleanColumn.ofNull("any_boolean").accept(visitor);

    // Assert
    assertThat(visitor.get().containsKey("any_boolean")).isTrue();
    assertThat(visitor.get().get("any_boolean")).isNull();
  }

  @Test
  public void visit_IntColumnAcceptCalled_ShouldGetMap() {
    // Act
    ANY_INT_COLUMN.accept(visitor);

    // Assert
    assertThat(visitor.get().get(ANY_INT_COLUMN.getName())).isEqualTo(ANY_INT);
  }

  @Test
  public void visit_IntColumnWithNullValueAcceptCalled_ShouldGetMap() {
    // Act
    IntColumn.ofNull("any_int").accept(visitor);

    // Assert
    assertThat(visitor.get().containsKey("any_int")).isTrue();
    assertThat(visitor.get().get("any_int")).isNull();
  }

  @Test
  public void visit_BigIntColumnAcceptCalled_ShouldGetMap() {
    // Act
    ANY_BIGINT_COLUMN.accept(visitor);

    // Assert
    assertThat(visitor.get().get(ANY_BIGINT_COLUMN.getName())).isEqualTo(ANY_BIGINT);
  }

  @Test
  public void visit_BigIntColumnWithNullValueAcceptCalled_ShouldGetMap() {
    // Act
    BigIntColumn.ofNull("any_bigint").accept(visitor);

    // Assert
    assertThat(visitor.get().containsKey("any_bigint")).isTrue();
    assertThat(visitor.get().get("any_bigint")).isNull();
  }

  @Test
  public void visit_FloatColumnAcceptCalled_ShouldGetMap() {
    // Act
    ANY_FLOAT_COLUMN.accept(visitor);

    // Assert
    assertThat(visitor.get().get(ANY_FLOAT_COLUMN.getName())).isEqualTo(ANY_FLOAT);
  }

  @Test
  public void visit_FloatColumnWithNullValueAcceptCalled_ShouldGetMap() {
    // Act
    FloatColumn.ofNull("any_float").accept(visitor);

    // Assert
    assertThat(visitor.get().containsKey("any_float")).isTrue();
    assertThat(visitor.get().get("any_float")).isNull();
  }

  @Test
  public void visit_DoubleColumnAcceptCalled_ShouldGetMap() {
    // Act
    ANY_DOUBLE_COLUMN.accept(visitor);

    // Assert
    assertThat(visitor.get().get(ANY_DOUBLE_COLUMN.getName())).isEqualTo(ANY_DOUBLE);
  }

  @Test
  public void visit_DoubleColumnWithNullValueAcceptCalled_ShouldGetMap() {
    // Act
    FloatColumn.ofNull("any_double").accept(visitor);

    // Assert
    assertThat(visitor.get().containsKey("any_double")).isTrue();
    assertThat(visitor.get().get("any_double")).isNull();
  }

  @Test
  public void visit_TextColumnAcceptCalled_ShouldGetMap() {
    // Act
    ANY_TEXT_COLUMN.accept(visitor);

    // Assert
    assertThat(visitor.get().get(ANY_TEXT_COLUMN.getName())).isEqualTo(ANY_TEXT);
  }

  @Test
  public void visit_TextColumnWithNullValueAcceptCalled_ShouldGetMap() {
    // Act
    TextColumn.ofNull("any_text").accept(visitor);

    // Assert
    assertThat(visitor.get().containsKey("any_text")).isTrue();
    assertThat(visitor.get().get("any_text")).isNull();
  }

  @Test
  public void visit_BlobColumnAcceptCalled_ShouldGetMap() {
    // Act
    ANY_BLOB_COLUMN.accept(visitor);

    // Assert
    ByteBuffer expected =
        (ByteBuffer)
            ByteBuffer.allocate(ANY_TEXT.length())
                .put(ANY_TEXT.getBytes(StandardCharsets.UTF_8))
                .flip();
    assertThat(visitor.get().get(ANY_BLOB_COLUMN.getName())).isEqualTo(expected);
  }

  @Test
  public void visit_BlobColumnWithNullValueAcceptCalled_ShouldGetMap() {
    // Act
    BlobColumn.ofNull("any_blob").accept(visitor);

    // Assert
    assertThat(visitor.get().containsKey("any_blob")).isTrue();
    assertThat(visitor.get().get("any_blob")).isNull();
  }

  @Test
  public void visit_DateColumnAcceptCalled_ShouldGetMap() {
    // Act
    ANY_DATE_COLUMN.accept(visitor);

    // Assert
    assertThat(visitor.get().get(ANY_DATE_COLUMN.getName()))
        .isEqualTo(TimeRelatedColumnEncodingUtils.encode(ANY_DATE_COLUMN));
  }

  @Test
  public void visit_DateColumnWithNullValueAcceptCalled_ShouldGetMap() {
    // Act
    DateColumn.ofNull("any_date").accept(visitor);

    // Assert
    assertThat(visitor.get().containsKey("any_date")).isTrue();
    assertThat(visitor.get().get("any_date")).isNull();
  }

  @Test
  public void visit_TimeColumnAcceptCalled_ShouldGetMap() {
    // Act
    ANY_TIME_COLUMN.accept(visitor);

    // Assert
    assertThat(visitor.get().get(ANY_TIME_COLUMN.getName()))
        .isEqualTo(TimeRelatedColumnEncodingUtils.encode(ANY_TIME_COLUMN));
  }

  @Test
  public void visit_TimeColumnWithNullValueAcceptCalled_ShouldGetMap() {
    // Act
    TimeColumn.ofNull("any_time").accept(visitor);

    // Assert
    assertThat(visitor.get().containsKey("any_time")).isTrue();
    assertThat(visitor.get().get("any_time")).isNull();
  }

  @Test
  public void visit_TimestampColumnAcceptCalled_ShouldGetMap() {
    // Act
    ANY_TIMESTAMP_COLUMN.accept(visitor);

    // Assert
    assertThat(visitor.get().get(ANY_TIMESTAMP_COLUMN.getName()))
        .isEqualTo(TimeRelatedColumnEncodingUtils.encode(ANY_TIMESTAMP_COLUMN));
  }

  @Test
  public void visit_TimestampColumnWithNullValueAcceptCalled_ShouldGetMap() {
    // Act
    TimestampColumn.ofNull("any_timestamp").accept(visitor);

    // Assert
    assertThat(visitor.get().containsKey("any_timestamp")).isTrue();
    assertThat(visitor.get().get("any_timestamp")).isNull();
  }

  @Test
  public void visit_TimestampTZColumnAcceptCalled_ShouldGetMap() {
    // Act
    ANY_TIMESTAMPTZ_COLUMN.accept(visitor);

    // Assert
    assertThat(visitor.get().get(ANY_TIMESTAMPTZ_COLUMN.getName()))
        .isEqualTo(TimeRelatedColumnEncodingUtils.encode(ANY_TIMESTAMPTZ_COLUMN));
  }

  @Test
  public void visit_TimestampTZColumnWithNullValueAcceptCalled_ShouldGetMap() {
    // Act
    TimestampTZColumn.ofNull("any_timestamptz").accept(visitor);

    // Assert
    assertThat(visitor.get().containsKey("any_timestamptz")).isTrue();
    assertThat(visitor.get().get("any_timestamptz")).isNull();
  }
}
