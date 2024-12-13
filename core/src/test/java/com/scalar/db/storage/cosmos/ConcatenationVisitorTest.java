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
import com.scalar.db.storage.ColumnEncodingUtils;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ConcatenationVisitorTest {
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
  private static final byte[] ANY_BLOB = "scalar".getBytes(StandardCharsets.UTF_8);
  private static final BlobColumn ANY_BLOB_COLUMN = BlobColumn.of("any_blob", ANY_BLOB);
  private static final DateColumn ANY_DATE_COLUMN = DateColumn.of("any_date", DateColumn.MAX_VALUE);
  private static final TimeColumn ANY_TIME_COLUMN = TimeColumn.of("any_time", TimeColumn.MAX_VALUE);
  private static final TimestampColumn ANY_TIMESTAMP_COLUMN =
      TimestampColumn.of("any_timestamp", TimestampColumn.MAX_VALUE);
  private static final TimestampTZColumn ANY_TIMESTAMPTZ_COLUMN =
      TimestampTZColumn.of("any_timestamp_tz", TimestampTZColumn.MAX_VALUE);
  private ConcatenationVisitor visitor;

  @BeforeEach
  public void setUp() {
    visitor = new ConcatenationVisitor();
  }

  @Test
  public void build_AllTypesGiven_ShouldBuildString() {
    // Act
    visitor.visit(ANY_BOOLEAN_COLUMN);
    visitor.visit(ANY_INT_COLUMN);
    visitor.visit(ANY_BIGINT_COLUMN);
    visitor.visit(ANY_FLOAT_COLUMN);
    visitor.visit(ANY_DOUBLE_COLUMN);
    visitor.visit(ANY_TEXT_COLUMN);
    visitor.visit(ANY_BLOB_COLUMN);
    visitor.visit(ANY_DATE_COLUMN);
    visitor.visit(ANY_TIME_COLUMN);
    visitor.visit(ANY_TIMESTAMP_COLUMN);
    visitor.visit(ANY_TIMESTAMPTZ_COLUMN);
    String actual = visitor.build();

    // Assert
    String[] values = actual.split(":", -1);
    assertThat(values.length).isEqualTo(11);
    assertThat(values[0]).isEqualTo(String.valueOf(ANY_BOOLEAN));
    assertThat(values[1]).isEqualTo(String.valueOf(ANY_INT));
    assertThat(values[2]).isEqualTo(String.valueOf(ANY_BIGINT));
    assertThat(values[3]).isEqualTo(String.valueOf(ANY_FLOAT));
    assertThat(values[4]).isEqualTo(String.valueOf(ANY_DOUBLE));
    assertThat(values[5]).isEqualTo(ANY_TEXT);
    assertThat(values[6])
        .isEqualTo(Base64.getUrlEncoder().withoutPadding().encodeToString(ANY_BLOB));
    assertThat(values[7]).isEqualTo(String.valueOf(ColumnEncodingUtils.encode(ANY_DATE_COLUMN)));
    assertThat(values[8]).isEqualTo(String.valueOf(ColumnEncodingUtils.encode(ANY_TIME_COLUMN)));
    assertThat(values[9]).isEqualTo(ColumnEncodingUtils.encode(ANY_TIMESTAMP_COLUMN));
    assertThat(values[10]).isEqualTo(ColumnEncodingUtils.encode(ANY_TIMESTAMPTZ_COLUMN));
  }

  @Test
  public void visit_BooleanColumnAcceptCalled_ShouldBuildBooleanAsString() {
    // Act
    ANY_BOOLEAN_COLUMN.accept(visitor);

    // Assert
    assertThat(visitor.build()).isEqualTo(String.valueOf(ANY_BOOLEAN));
  }

  @Test
  public void visit_IntColumnAcceptCalled_ShouldBuildIntAsString() {
    // Act
    ANY_INT_COLUMN.accept(visitor);

    // Assert
    assertThat(visitor.build()).isEqualTo(String.valueOf(ANY_INT));
  }

  @Test
  public void visit_BigIntColumnAcceptCalled_ShouldBuildBigIntAsString() {
    // Act
    ANY_BIGINT_COLUMN.accept(visitor);

    // Assert
    assertThat(visitor.build()).isEqualTo(String.valueOf(ANY_BIGINT));
  }

  @Test
  public void visit_FloatColumnAcceptCalled_ShouldBuildFloatAsString() {
    // Act
    ANY_FLOAT_COLUMN.accept(visitor);

    // Assert
    assertThat(visitor.build()).isEqualTo(String.valueOf(ANY_FLOAT));
  }

  @Test
  public void visit_DoubleColumnAcceptCalled_ShouldBuildDoubleAsString() {
    // Act
    ANY_DOUBLE_COLUMN.accept(visitor);

    // Assert
    assertThat(visitor.build()).isEqualTo(String.valueOf(ANY_DOUBLE));
  }

  @Test
  public void visit_TextColumnAcceptCalled_ShouldBuildText() {
    // Act
    ANY_TEXT_COLUMN.accept(visitor);

    // Assert
    assertThat(visitor.build()).isEqualTo(ANY_TEXT);
  }

  @Test
  public void visit_BlobColumnAcceptCalled_ShouldBuildBlobAsString() {
    // Act
    ANY_BLOB_COLUMN.accept(visitor);

    // Assert
    assertThat(visitor.build())
        .isEqualTo(Base64.getUrlEncoder().withoutPadding().encodeToString(ANY_BLOB));
  }

  @Test
  public void visit_DateColumnAcceptCalled_ShouldBuildDateAsString() {
    // Act
    ANY_DATE_COLUMN.accept(visitor);

    // Assert
    assertThat(visitor.build())
        .isEqualTo(String.valueOf(ColumnEncodingUtils.encode(ANY_DATE_COLUMN)));
  }

  @Test
  public void visit_TimeColumnAcceptCalled_ShouldBuildTimeAsString() {
    // Act
    ANY_TIME_COLUMN.accept(visitor);

    // Assert
    assertThat(visitor.build())
        .isEqualTo(String.valueOf(ColumnEncodingUtils.encode(ANY_TIME_COLUMN)));
  }

  @Test
  public void visit_TimestampColumnAcceptCalled_ShouldBuildTimestampAsString() {
    // Act
    ANY_TIMESTAMP_COLUMN.accept(visitor);

    // Assert
    assertThat(visitor.build()).isEqualTo(ColumnEncodingUtils.encode(ANY_TIMESTAMP_COLUMN));
  }

  @Test
  public void visit_TimestampTZColumnAcceptCalled_ShouldBuildTimestampTZAsString() {
    // Act
    ANY_TIMESTAMPTZ_COLUMN.accept(visitor);

    // Assert
    assertThat(visitor.build()).isEqualTo(ColumnEncodingUtils.encode(ANY_TIMESTAMPTZ_COLUMN));
  }
}
