package com.scalar.db.storage.dynamo;

import static org.assertj.core.api.Assertions.assertThat;

import com.scalar.db.io.BigIntValue;
import com.scalar.db.io.BlobValue;
import com.scalar.db.io.BooleanValue;
import com.scalar.db.io.DoubleValue;
import com.scalar.db.io.FloatValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.TextValue;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.junit.Before;
import org.junit.Test;

public class MapVisitorTest {
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
  private static final byte[] ANY_BLOB = ANY_TEXT.getBytes(StandardCharsets.UTF_8);
  private static final BlobValue ANY_BLOB_VALUE = new BlobValue("any_blob", ANY_BLOB);
  private MapVisitor visitor;

  @Before
  public void setUp() {
    visitor = new MapVisitor();
  }

  @Test
  public void visit_BooleanValueAcceptCalled_ShouldGetMap() {
    // Act
    ANY_BOOLEAN_VALUE.accept(visitor);

    // Assert
    assertThat(visitor.get().get(ANY_BOOLEAN_VALUE.getName()).bool()).isEqualTo(ANY_BOOLEAN);
  }

  @Test
  public void visit_IntValueAcceptCalled_ShouldGetMap() {
    // Act
    ANY_INT_VALUE.accept(visitor);

    // Assert
    assertThat(Integer.valueOf(visitor.get().get(ANY_INT_VALUE.getName()).n())).isEqualTo(ANY_INT);
  }

  @Test
  public void visit_BigIntValueAcceptCalled_ShouldGetMap() {
    // Act
    ANY_BIGINT_VALUE.accept(visitor);

    // Assert
    assertThat(Long.valueOf(visitor.get().get(ANY_BIGINT_VALUE.getName()).n()))
        .isEqualTo(ANY_BIGINT);
  }

  @Test
  public void visit_FloatValueAcceptCalled_ShouldGetMap() {
    // Act
    ANY_FLOAT_VALUE.accept(visitor);

    // Assert
    assertThat(Float.valueOf(visitor.get().get(ANY_FLOAT_VALUE.getName()).n()))
        .isEqualTo(ANY_FLOAT);
  }

  @Test
  public void visit_DoubleValueAcceptCalled_ShouldGetMap() {
    // Act
    ANY_DOUBLE_VALUE.accept(visitor);

    // Assert
    assertThat(Double.valueOf(visitor.get().get(ANY_DOUBLE_VALUE.getName()).n()))
        .isEqualTo(ANY_DOUBLE);
  }

  @Test
  public void visit_TextValueAcceptCalled_ShouldGetMap() {
    // Act
    ANY_TEXT_VALUE.accept(visitor);

    // Assert
    assertThat(visitor.get().get(ANY_TEXT_VALUE.getName()).s()).isEqualTo(ANY_TEXT);
  }

  @Test
  public void visit_BlobValueAcceptCalled_ShouldGetMap() {
    // Act
    ANY_BLOB_VALUE.accept(visitor);

    // Assert
    ByteBuffer expected =
        (ByteBuffer)
            ByteBuffer.allocate(ANY_TEXT.length())
                .put(ANY_TEXT.getBytes(StandardCharsets.UTF_8))
                .flip();
    assertThat(visitor.get().get(ANY_BLOB_VALUE.getName()).b().asByteBuffer()).isEqualTo(expected);
  }
}
