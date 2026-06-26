package com.scalar.db.transaction.consensuscommit.cbrl;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableList;
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
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Round-trips every ScalarDB column type through {@link CbrlRestore}'s own proto&lt;-&gt;io
 * converter pair ({@code ioColumnToProto} then {@code toIoColumn}), proving the two are mutual
 * inverses for every type and for null — so a table with any column type restores correctly, not
 * just the five the integration test happens to use. (This checks the pair against itself; it does
 * not cross-check against {@code WriteSetEncoder}'s encoder, which produces the persisted redo.)
 */
class ColumnCodecRoundTripTest {

  private static final String C = "col";

  @Test
  void everyColumnType_valueRoundTripsThroughProto() {
    // Arrange
    List<Column<?>> columns =
        ImmutableList.of(
            IntColumn.of(C, 42),
            BigIntColumn.of(C, 9_000_000_000L),
            BooleanColumn.of(C, true),
            TextColumn.of(C, "hello"),
            BlobColumn.of(C, new byte[] {1, 2, 3}),
            FloatColumn.of(C, 1.5f),
            DoubleColumn.of(C, 2.25d),
            DateColumn.of(C, LocalDate.of(2026, 6, 24)),
            TimeColumn.of(C, LocalTime.of(1, 2, 3)),
            TimestampColumn.of(C, LocalDateTime.of(2026, 6, 24, 1, 2, 3)),
            TimestampTZColumn.of(C, Instant.ofEpochSecond(1_700_000_000L)));
    // Act & Assert
    columns.forEach(ColumnCodecRoundTripTest::assertRoundTrip);
  }

  @Test
  void everyColumnType_nullRoundTripsThroughProto() {
    // Arrange
    List<Column<?>> nullColumns =
        ImmutableList.of(
            IntColumn.ofNull(C),
            BigIntColumn.ofNull(C),
            BooleanColumn.ofNull(C),
            TextColumn.ofNull(C),
            BlobColumn.ofNull(C),
            FloatColumn.ofNull(C),
            DoubleColumn.ofNull(C),
            DateColumn.ofNull(C),
            TimeColumn.ofNull(C),
            TimestampColumn.ofNull(C),
            TimestampTZColumn.ofNull(C));
    // Act & Assert
    nullColumns.forEach(ColumnCodecRoundTripTest::assertRoundTrip);
  }

  private static void assertRoundTrip(Column<?> column) {
    Column<?> roundTripped = CbrlRestore.toIoColumn(CbrlRestore.ioColumnToProto(column));
    assertThat(roundTripped).as("round-trip for %s", column).isEqualTo(column);
  }
}
