package com.scalar.db.storage.cosmos;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.BigIntValue;
import com.scalar.db.io.BlobColumn;
import com.scalar.db.io.BooleanColumn;
import com.scalar.db.io.DoubleColumn;
import com.scalar.db.io.FloatColumn;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.TextColumn;
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
  private static final long ANY_BIGINT = BigIntValue.MAX_VALUE;
  private static final BigIntColumn ANY_BIGINT_COLUMN = BigIntColumn.of("any_bigint", ANY_BIGINT);
  private static final float ANY_FLOAT = Float.MIN_NORMAL;
  private static final FloatColumn ANY_FLOAT_COLUMN = FloatColumn.of("any_float", ANY_FLOAT);
  private static final double ANY_DOUBLE = Double.MIN_NORMAL;
  private static final DoubleColumn ANY_DOUBLE_COLUMN = DoubleColumn.of("any_double", ANY_DOUBLE);
  private static final String ANY_TEXT = "test";
  private static final TextColumn ANY_TEXT_COLUMN = TextColumn.of("any_text", ANY_TEXT);
  private static final byte[] ANY_BLOB = "scalar".getBytes(StandardCharsets.UTF_8);
  private static final BlobColumn ANY_BLOB_COLUMN = BlobColumn.of("any_blob", ANY_BLOB);
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
    visitor.visit(BlobColumn.ofNull("any_blob"));
    visitor.visit(ANY_BIGINT_COLUMN);
    visitor.visit(ANY_FLOAT_COLUMN);
    visitor.visit(TextColumn.ofNull("any_text"));
    visitor.visit(ANY_DOUBLE_COLUMN);
    visitor.visit(ANY_TEXT_COLUMN);
    visitor.visit(ANY_BLOB_COLUMN);
    String actual = visitor.build();

    // Assert
    String[] values = actual.split(":", -1);
    assertThat(values.length).isEqualTo(7);
    assertThat(values[0]).isEqualTo(String.valueOf(ANY_BOOLEAN));
    assertThat(values[1]).isEqualTo(String.valueOf(ANY_INT));
    assertThat(values[2]).isEqualTo(String.valueOf(ANY_BIGINT));
    assertThat(values[3]).isEqualTo(String.valueOf(ANY_FLOAT));
    assertThat(values[4]).isEqualTo(String.valueOf(ANY_DOUBLE));
    assertThat(values[5]).isEqualTo(ANY_TEXT);
    assertThat(values[6])
        .isEqualTo(Base64.getUrlEncoder().withoutPadding().encodeToString(ANY_BLOB));
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
  public void visit_NullTextColumnAcceptCalled_ShouldDoNothing() {
    // Act
    TextColumn.ofNull("any_text").accept(visitor);

    // Assert
    assertThat(visitor.build()).isEqualTo("");
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
  public void visit_NullBlobColumnAcceptCalled_ShouldDoNothing() {
    // Act
    BlobColumn.ofNull("any_blob").accept(visitor);

    // Assert
    assertThat(visitor.build()).isEqualTo("");
  }
}
