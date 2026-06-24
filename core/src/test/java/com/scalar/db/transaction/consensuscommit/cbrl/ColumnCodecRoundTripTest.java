package com.scalar.db.transaction.consensuscommit.cbrl;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.BlobColumn;
import com.scalar.db.io.BooleanColumn;
import com.scalar.db.io.Column;
import com.scalar.db.io.DateColumn;
import com.scalar.db.io.DoubleColumn;
import com.scalar.db.io.FloatColumn;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.TextColumn;
import com.scalar.db.io.TimeColumn;
import com.scalar.db.io.TimestampColumn;
import com.scalar.db.io.TimestampTZColumn;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import org.junit.jupiter.api.Test;

/**
 * Round-trips every ScalarDB column type through {@link CbrlRestore}'s proto&lt;-&gt;io converters,
 * so a table with any column type restores correctly — not just the five the integration test
 * happens to use. The encoding must match {@code WriteSetEncoder} (same {@code
 * TimeRelatedColumnEncodingUtils}), which this verifies via the value round-trip.
 */
class ColumnCodecRoundTripTest {

  private static final String C = "col";

  @Test
  void everyColumnType_valueRoundTripsThroughProto() {
    assertRoundTrip(IntColumn.of(C, 42));
    assertRoundTrip(BigIntColumn.of(C, 9_000_000_000L));
    assertRoundTrip(BooleanColumn.of(C, true));
    assertRoundTrip(TextColumn.of(C, "hello"));
    assertRoundTrip(BlobColumn.of(C, new byte[] {1, 2, 3}));
    assertRoundTrip(FloatColumn.of(C, 1.5f));
    assertRoundTrip(DoubleColumn.of(C, 2.25d));
    assertRoundTrip(DateColumn.of(C, LocalDate.of(2026, 6, 24)));
    assertRoundTrip(TimeColumn.of(C, LocalTime.of(1, 2, 3)));
    assertRoundTrip(TimestampColumn.of(C, LocalDateTime.of(2026, 6, 24, 1, 2, 3)));
    assertRoundTrip(TimestampTZColumn.of(C, Instant.ofEpochSecond(1_700_000_000L)));
  }

  @Test
  void everyColumnType_nullRoundTripsThroughProto() {
    assertRoundTrip(IntColumn.ofNull(C));
    assertRoundTrip(BigIntColumn.ofNull(C));
    assertRoundTrip(BooleanColumn.ofNull(C));
    assertRoundTrip(TextColumn.ofNull(C));
    assertRoundTrip(BlobColumn.ofNull(C));
    assertRoundTrip(FloatColumn.ofNull(C));
    assertRoundTrip(DoubleColumn.ofNull(C));
    assertRoundTrip(DateColumn.ofNull(C));
    assertRoundTrip(TimeColumn.ofNull(C));
    assertRoundTrip(TimestampColumn.ofNull(C));
    assertRoundTrip(TimestampTZColumn.ofNull(C));
  }

  private static void assertRoundTrip(Column<?> column) {
    Column<?> roundTripped = CbrlRestore.toIoColumn(CbrlRestore.ioColumnToProto(column));
    assertThat(roundTripped).as("round-trip for %s", column).isEqualTo(column);
  }
}
