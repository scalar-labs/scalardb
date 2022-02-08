package com.scalar.db.storage.cassandra;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.datastax.driver.core.BoundStatement;
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

  @Mock private BoundStatement bound;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();
  }

  @Test
  public void visit_BooleanValueAcceptCalled_ShouldCallSetBool() {
    // Arrange
    BooleanValue value = new BooleanValue(ANY_NAME, ANY_BOOL);
    ValueBinder binder = new ValueBinder(bound);

    // Act
    value.accept(binder);

    // Assert
    verify(bound).setBool(0, ANY_BOOL);
  }

  @Test
  public void visit_IntValueAcceptCalled_ShouldCallSetInt() {
    // Arrange
    IntValue value = new IntValue(ANY_NAME, ANY_INT);
    ValueBinder binder = new ValueBinder(bound);

    // Act
    value.accept(binder);

    // Assert
    verify(bound).setInt(0, ANY_INT);
  }

  @Test
  public void visit_BigIntValueAcceptCalled_ShouldCallSetLong() {
    // Arrange
    BigIntValue value = new BigIntValue(ANY_NAME, ANY_LONG);
    ValueBinder binder = new ValueBinder(bound);

    // Act
    value.accept(binder);

    // Assert
    verify(bound).setLong(0, ANY_LONG);
  }

  @Test
  public void visit_FloatValueAcceptCalled_ShouldCallSetFloat() {
    // Arrange
    FloatValue value = new FloatValue(ANY_NAME, ANY_FLOAT);
    ValueBinder binder = new ValueBinder(bound);

    // Act
    value.accept(binder);

    // Assert
    verify(bound).setFloat(0, ANY_FLOAT);
  }

  @Test
  public void visit_DoubleValueAcceptCalled_ShouldCallSetDouble() {
    // Arrange
    DoubleValue value = new DoubleValue(ANY_NAME, ANY_DOUBLE);
    ValueBinder binder = new ValueBinder(bound);

    // Act
    value.accept(binder);

    // Assert
    verify(bound).setDouble(0, ANY_DOUBLE);
  }

  @Test
  public void visit_TextValueAcceptCalled_ShouldCallSetString() {
    // Arrange
    TextValue value = new TextValue(ANY_NAME, ANY_STRING);
    ValueBinder binder = new ValueBinder(bound);

    // Act
    value.accept(binder);

    // Assert
    verify(bound).setString(0, ANY_STRING);
  }

  @Test
  public void visit_BlobValueAcceptCalled_ShouldCallSetString() {
    // Arrange
    BlobValue value = new BlobValue(ANY_NAME, ANY_STRING.getBytes(StandardCharsets.UTF_8));
    ValueBinder binder = new ValueBinder(bound);

    // Act
    value.accept(binder);

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
  public void visit_AcceptCalledMultipleTimes_ShouldCallSetWithIncremented() {
    // Arrange
    TextValue value1 = new TextValue(ANY_NAME, ANY_STRING);
    IntValue value2 = new IntValue(ANY_NAME, ANY_INT);
    ValueBinder binder = new ValueBinder(bound);

    // Act
    value1.accept(binder);
    value2.accept(binder);

    // Assert
    verify(bound).setString(0, ANY_STRING);
    verify(bound).setInt(1, ANY_INT);
  }

  @Test
  public void visit_AcceptCalledMultipleTimesWithNullValue_ShouldSkipNull() {
    // Arrange
    IntValue value1 = new IntValue(ANY_NAME, ANY_INT);
    BlobValue value2 = new BlobValue(ANY_NAME, (byte[]) null);
    TextValue value3 = new TextValue(ANY_NAME, (byte[]) null);
    IntValue value4 = new IntValue(ANY_NAME, ANY_INT);
    ValueBinder binder = new ValueBinder(bound);

    // Act
    value1.accept(binder);
    value2.accept(binder);
    value3.accept(binder);
    value4.accept(binder);

    // Assert
    verify(bound).setInt(0, ANY_INT);
    verify(bound, never()).setBytes(anyInt(), any(ByteBuffer.class));
    verify(bound, never()).setString(anyInt(), anyString());
    verify(bound).setInt(3, ANY_INT);
  }

  @Test
  public void constructor_NullGiven_ShouldThrowNullPointerException() {
    // Act Assert
    assertThatThrownBy(() -> new ValueBinder(null)).isInstanceOf(NullPointerException.class);
  }
}
