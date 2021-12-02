package com.scalar.db.storage.cosmos;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.io.BigIntValue;
import com.scalar.db.io.BlobValue;
import com.scalar.db.io.BooleanValue;
import com.scalar.db.io.DoubleValue;
import com.scalar.db.io.FloatValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.TextValue;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.junit.Before;
import org.junit.Test;

public class ConcatenationVisitorTest {
  private static final boolean ANY_BOOLEAN = false;
  private static final BooleanValue ANY_BOOLEAN_VALUE =
      new BooleanValue("any_boolean", ANY_BOOLEAN);
  private static final int ANY_INT = Integer.MIN_VALUE;
  private static final IntValue ANY_INT_VALUE = new IntValue("any_int", ANY_INT);
  private static final long ANY_BIGINT = BigIntValue.MAX_VALUE;
  private static final BigIntValue ANY_BIGINT_VALUE = new BigIntValue("any_bigint", ANY_BIGINT);
  private static final float ANY_FLOAT = Float.MIN_NORMAL;
  private static final FloatValue ANY_FLOAT_VALUE = new FloatValue("any_float", ANY_FLOAT);
  private static final double ANY_DOUBLE = Double.MIN_NORMAL;
  private static final DoubleValue ANY_DOUBLE_VALUE = new DoubleValue("any_double", ANY_DOUBLE);
  private static final String ANY_TEXT = "test";
  private static final TextValue ANY_TEXT_VALUE = new TextValue("any_text", ANY_TEXT);
  private static final byte[] ANY_BLOB = "scalar".getBytes(StandardCharsets.UTF_8);
  private static final BlobValue ANY_BLOB_VALUE = new BlobValue("any_blob", ANY_BLOB);
  private ConcatenationVisitor visitor;

  @Before
  public void setUp() {
    visitor = new ConcatenationVisitor();
  }

  @Test
  public void build_AllTypesGiven_ShouldBuildString() {
    // Act
    visitor.visit(ANY_BOOLEAN_VALUE);
    visitor.visit(ANY_INT_VALUE);
    visitor.visit(ANY_BIGINT_VALUE);
    visitor.visit(ANY_FLOAT_VALUE);
    visitor.visit(ANY_DOUBLE_VALUE);
    visitor.visit(ANY_TEXT_VALUE);
    visitor.visit(ANY_BLOB_VALUE);
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
  public void visit_BooleanValueAcceptCalled_ShouldBuildBooleanAsString() {
    // Act
    ANY_BOOLEAN_VALUE.accept(visitor);

    // Assert
    assertThat(visitor.build()).isEqualTo(String.valueOf(ANY_BOOLEAN));
  }

  @Test
  public void visit_IntValueAcceptCalled_ShouldBuildIntAsString() {
    // Act
    ANY_INT_VALUE.accept(visitor);

    // Assert
    assertThat(visitor.build()).isEqualTo(String.valueOf(ANY_INT));
  }

  @Test
  public void visit_BigIntValueAcceptCalled_ShouldBuildBigIntAsString() {
    // Act
    ANY_BIGINT_VALUE.accept(visitor);

    // Assert
    assertThat(visitor.build()).isEqualTo(String.valueOf(ANY_BIGINT));
  }

  @Test
  public void visit_FloatValueAcceptCalled_ShouldBuildFloatAsString() {
    // Act
    ANY_FLOAT_VALUE.accept(visitor);

    // Assert
    assertThat(visitor.build()).isEqualTo(String.valueOf(ANY_FLOAT));
  }

  @Test
  public void visit_DoubleValueAcceptCalled_ShouldBuildDoubleAsString() {
    // Act
    ANY_DOUBLE_VALUE.accept(visitor);

    // Assert
    assertThat(visitor.build()).isEqualTo(String.valueOf(ANY_DOUBLE));
  }

  @Test
  public void visit_TextValueAcceptCalled_ShouldBuildText() {
    // Act
    ANY_TEXT_VALUE.accept(visitor);

    // Assert
    assertThat(visitor.build()).isEqualTo(ANY_TEXT);
  }

  @Test
  public void visit_BlobValueAcceptCalled_ShouldBuildBlobAsString() {
    // Act
    ANY_BLOB_VALUE.accept(visitor);

    // Assert
    assertThat(visitor.build())
        .isEqualTo(Base64.getUrlEncoder().withoutPadding().encodeToString(ANY_BLOB));
  }
}
