package com.scalar.db.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

public class KeyTest {
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_NAME_3 = "name3";
  private static final String ANY_NAME_4 = "name4";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";
  private static final String ANY_TEXT_3 = "text3";
  private static final String ANY_TEXT_4 = "text4";
  private static final int ANY_INT_1 = 10;
  private static final int ANY_INT_2 = 20;
  private static final LocalDate ANY_DATE = DateColumn.MAX_VALUE;
  private static final LocalTime ANY_TIME = TimeColumn.MAX_VALUE;
  private static final LocalDateTime ANY_TIMESTAMP = TimestampColumn.MAX_VALUE;
  private static final Instant ANY_TIMESTAMPTZ =
      ANY_TIMESTAMP.plusHours(1).toInstant(ZoneOffset.UTC);

  @Test
  public void constructor_WithSingleBooleanValue_ShouldReturnWhatsSet() {
    // Arrange
    String name = ANY_NAME_1;
    boolean value = true;
    Key key = new Key(name, value);

    // Act Assert
    List<Value<?>> values = key.get();
    assertThat(values.size()).isEqualTo(1);
    assertThat(values.get(0).getName()).isEqualTo(name);
    assertThat(values.get(0).getAsBoolean()).isEqualTo(value);

    assertThat(key.size()).isEqualTo(1);
    assertThat(key.getColumnName(0)).isEqualTo(name);
    assertThat(key.getBooleanValue(0)).isEqualTo(value);
  }

  @Test
  public void constructor_WithSingleIntegerValue_ShouldReturnWhatsSet() {
    // Arrange
    String name = ANY_NAME_1;
    int value = 100;
    Key key = new Key(name, value);

    // Act Assert
    List<Value<?>> values = key.get();
    assertThat(values.size()).isEqualTo(1);
    assertThat(values.get(0).getName()).isEqualTo(name);
    assertThat(values.get(0).getAsInt()).isEqualTo(value);

    assertThat(key.size()).isEqualTo(1);
    assertThat(key.getColumnName(0)).isEqualTo(name);
    assertThat(key.getIntValue(0)).isEqualTo(value);
  }

  @Test
  public void constructor_WithSingleLongValue_ShouldReturnWhatsSet() {
    // Arrange
    String name = ANY_NAME_1;
    long value = 1000L;
    Key key = new Key(name, value);

    // Act Assert
    List<Value<?>> values = key.get();
    assertThat(values.size()).isEqualTo(1);
    assertThat(values.get(0).getName()).isEqualTo(name);
    assertThat(values.get(0).getAsLong()).isEqualTo(value);

    assertThat(key.size()).isEqualTo(1);
    assertThat(key.getColumnName(0)).isEqualTo(name);
    assertThat(key.getBigIntValue(0)).isEqualTo(value);
  }

  @Test
  public void constructor_WithSingleFloatValue_ShouldReturnWhatsSet() {
    // Arrange
    String name = ANY_NAME_1;
    float value = 1.0f;
    Key key = new Key(name, value);

    // Act Assert
    List<Value<?>> values = key.get();
    assertThat(values.size()).isEqualTo(1);
    assertThat(values.get(0).getName()).isEqualTo(name);
    assertThat(values.get(0).getAsFloat()).isEqualTo(value);

    assertThat(key.size()).isEqualTo(1);
    assertThat(key.getColumnName(0)).isEqualTo(name);
    assertThat(key.getFloatValue(0)).isEqualTo(value);
  }

  @Test
  public void constructor_WithSingleDoubleValue_ShouldReturnWhatsSet() {
    // Arrange
    String name = ANY_NAME_1;
    double value = 1.01d;
    Key key = new Key(name, value);

    // Act Assert
    List<Value<?>> values = key.get();
    assertThat(values.size()).isEqualTo(1);
    assertThat(values.get(0).getName()).isEqualTo(name);
    assertThat(values.get(0).getAsDouble()).isEqualTo(value);

    assertThat(key.size()).isEqualTo(1);
    assertThat(key.getColumnName(0)).isEqualTo(name);
    assertThat(key.getDoubleValue(0)).isEqualTo(value);
  }

  @Test
  public void constructor_WithSingleStringValue_ShouldReturnWhatsSet() {
    // Arrange
    String name = ANY_NAME_1;
    String value = "value";
    Key key = new Key(name, value);

    // Act Assert
    List<Value<?>> values = key.get();
    assertThat(values.size()).isEqualTo(1);
    assertThat(values.get(0).getName()).isEqualTo(name);
    assertThat(values.get(0).getAsString().get()).isEqualTo(value);

    assertThat(key.size()).isEqualTo(1);
    assertThat(key.getColumnName(0)).isEqualTo(name);
    assertThat(key.getTextValue(0)).isEqualTo(value);
  }

  @Test
  public void constructor_WithSingleByteArrayValue_ShouldReturnWhatsSet() {
    // Arrange
    String name = ANY_NAME_1;
    byte[] value = "value".getBytes(StandardCharsets.UTF_8);
    Key key = new Key(name, value);

    // Act Assert
    List<Value<?>> values = key.get();
    assertThat(values.size()).isEqualTo(1);
    assertThat(values.get(0).getName()).isEqualTo(name);
    assertThat(Arrays.equals(values.get(0).getAsBytes().get(), value)).isTrue();

    assertThat(key.size()).isEqualTo(1);
    assertThat(key.getColumnName(0)).isEqualTo(name);
    assertThat(key.getBlobValue(0)).isEqualTo(ByteBuffer.wrap(value));
    assertThat(key.getBlobValueAsByteBuffer(0)).isEqualTo(ByteBuffer.wrap(value));
    assertThat(key.getBlobValueAsBytes(0)).isEqualTo(value);
  }

  @Test
  public void constructor_WithSingleByteBufferValue_ShouldReturnWhatsSet() {
    // Arrange
    String name = ANY_NAME_1;
    byte[] value = "value".getBytes(StandardCharsets.UTF_8);
    Key key = new Key(name, ByteBuffer.wrap(value));

    // Act Assert
    List<Value<?>> values = key.get();
    assertThat(values.size()).isEqualTo(1);
    assertThat(values.get(0).getName()).isEqualTo(name);
    assertThat(Arrays.equals(values.get(0).getAsBytes().get(), value)).isTrue();

    assertThat(key.size()).isEqualTo(1);
    assertThat(key.getColumnName(0)).isEqualTo(name);
    assertThat(key.getBlobValue(0)).isEqualTo(ByteBuffer.wrap(value));
    assertThat(key.getBlobValueAsByteBuffer(0)).isEqualTo(ByteBuffer.wrap(value));
    assertThat(key.getBlobValueAsBytes(0)).isEqualTo(value);
  }

  @Test
  public void constructor_WithSingleDateValue_ShouldReturnWhatsSet() {
    // Arrange
    String name = ANY_NAME_1;
    Key key = new Key(name, ANY_DATE);

    // Act Assert
    List<Column<?>> columns = key.getColumns();
    assertThat(columns.size()).isEqualTo(1);
    assertThat(columns.get(0).getName()).isEqualTo(name);
    assertThat(columns.get(0).getDateValue()).isEqualTo(ANY_DATE);

    assertThat(key.size()).isEqualTo(1);
    assertThat(key.getColumnName(0)).isEqualTo(name);
    assertThat(key.getDateValue(0)).isEqualTo(ANY_DATE);
  }

  @Test
  public void constructor_WithSingleTimeValue_ShouldReturnWhatsSet() {
    // Arrange
    String name = ANY_NAME_1;
    Key key = new Key(name, ANY_TIME);

    // Act Assert
    List<Column<?>> columns = key.getColumns();
    assertThat(columns.size()).isEqualTo(1);
    assertThat(columns.get(0).getName()).isEqualTo(name);
    assertThat(columns.get(0).getTimeValue()).isEqualTo(ANY_TIME);

    assertThat(key.size()).isEqualTo(1);
    assertThat(key.getColumnName(0)).isEqualTo(name);
    assertThat(key.getTimeValue(0)).isEqualTo(ANY_TIME);
  }

  @Test
  public void constructor_WithSingleTimestampValue_ShouldReturnWhatsSet() {
    // Arrange
    String name = ANY_NAME_1;
    Key key = new Key(name, ANY_TIMESTAMP);

    // Act Assert
    List<Column<?>> columns = key.getColumns();
    assertThat(columns.size()).isEqualTo(1);
    assertThat(columns.get(0).getName()).isEqualTo(name);
    assertThat(columns.get(0).getTimestampValue()).isEqualTo(ANY_TIMESTAMP);

    assertThat(key.size()).isEqualTo(1);
    assertThat(key.getColumnName(0)).isEqualTo(name);
    assertThat(key.getTimestampValue(0)).isEqualTo(ANY_TIMESTAMP);
  }

  @Test
  public void constructor_WithSingleTimestampTZValue_ShouldReturnWhatsSet() {
    // Arrange
    String name = ANY_NAME_1;
    Key key = new Key(name, ANY_TIMESTAMPTZ);

    // Act Assert
    List<Column<?>> columns = key.getColumns();
    assertThat(columns.size()).isEqualTo(1);
    assertThat(columns.get(0).getName()).isEqualTo(name);
    assertThat(columns.get(0).getTimestampTZValue()).isEqualTo(ANY_TIMESTAMPTZ);

    assertThat(key.size()).isEqualTo(1);
    assertThat(key.getColumnName(0)).isEqualTo(name);
    assertThat(key.getTimestampTZValue(0)).isEqualTo(ANY_TIMESTAMPTZ);
  }

  @Test
  public void constructor_WithMultipleNamesAndValues_ShouldReturnWhatsSet() {
    // Arrange
    Key key1 = new Key("key1", true, "key2", 5678);
    Key key2 = new Key("key3", 1234L, "key4", 4.56f, "key5", 1.23);
    Key key3 =
        new Key(
            "key6",
            "string_key",
            "key7",
            "blob_key".getBytes(StandardCharsets.UTF_8),
            "key8",
            ByteBuffer.wrap("blob_key2".getBytes(StandardCharsets.UTF_8)),
            "key9",
            2468);
    Key key4 = new Key("key1", true, "key2", 5678, "key3", 1234L, "key4", 4.56f, "key5", 1.23);
    Key key5 =
        new Key(
            "key1",
            ANY_DATE,
            "key2",
            ANY_TIME,
            "key3",
            ANY_TIMESTAMP,
            "key4",
            ANY_TIMESTAMPTZ,
            "key5",
            1.23);

    // Act Assert
    List<Value<?>> values1 = key1.get();
    assertThat(values1.size()).isEqualTo(2);
    assertThat(values1.get(0)).isEqualTo(new BooleanValue("key1", true));
    assertThat(values1.get(1)).isEqualTo(new IntValue("key2", 5678));

    assertThat(key1.size()).isEqualTo(2);
    assertThat(key1.getColumnName(0)).isEqualTo("key1");
    assertThat(key1.getBooleanValue(0)).isEqualTo(true);
    assertThat(key1.getColumnName(1)).isEqualTo("key2");
    assertThat(key1.getIntValue(1)).isEqualTo(5678);

    List<Value<?>> values2 = key2.get();
    assertThat(values2.size()).isEqualTo(3);
    assertThat(values2.get(0)).isEqualTo(new BigIntValue("key3", 1234L));
    assertThat(values2.get(1)).isEqualTo(new FloatValue("key4", 4.56f));
    assertThat(values2.get(2)).isEqualTo(new DoubleValue("key5", 1.23));

    assertThat(key2.size()).isEqualTo(3);
    assertThat(key2.getColumnName(0)).isEqualTo("key3");
    assertThat(key2.getBigIntValue(0)).isEqualTo(1234L);
    assertThat(key2.getColumnName(1)).isEqualTo("key4");
    assertThat(key2.getFloatValue(1)).isEqualTo(4.56f);
    assertThat(key2.getColumnName(2)).isEqualTo("key5");
    assertThat(key2.getDoubleValue(2)).isEqualTo(1.23);

    List<Value<?>> values3 = key3.get();
    assertThat(values3.size()).isEqualTo(4);
    assertThat(values3.get(0)).isEqualTo(new TextValue("key6", "string_key"));
    assertThat(values3.get(1))
        .isEqualTo(new BlobValue("key7", "blob_key".getBytes(StandardCharsets.UTF_8)));
    assertThat(values3.get(2))
        .isEqualTo(
            new BlobValue("key8", ByteBuffer.wrap("blob_key2".getBytes(StandardCharsets.UTF_8))));
    assertThat(values3.get(3)).isEqualTo(new IntValue("key9", 2468));

    assertThat(key3.size()).isEqualTo(4);
    assertThat(key3.getColumnName(0)).isEqualTo("key6");
    assertThat(key3.getTextValue(0)).isEqualTo("string_key");
    assertThat(key3.getColumnName(1)).isEqualTo("key7");
    assertThat(key3.getBlobValue(1))
        .isEqualTo(ByteBuffer.wrap("blob_key".getBytes(StandardCharsets.UTF_8)));
    assertThat(key3.getBlobValueAsByteBuffer(1))
        .isEqualTo(ByteBuffer.wrap("blob_key".getBytes(StandardCharsets.UTF_8)));
    assertThat(key3.getBlobValueAsBytes(1)).isEqualTo("blob_key".getBytes(StandardCharsets.UTF_8));
    assertThat(key3.getColumnName(2)).isEqualTo("key8");
    assertThat(key3.getBlobValue(2))
        .isEqualTo(ByteBuffer.wrap("blob_key2".getBytes(StandardCharsets.UTF_8)));
    assertThat(key3.getBlobValueAsByteBuffer(2))
        .isEqualTo(ByteBuffer.wrap("blob_key2".getBytes(StandardCharsets.UTF_8)));
    assertThat(key3.getBlobValueAsBytes(2)).isEqualTo("blob_key2".getBytes(StandardCharsets.UTF_8));
    assertThat(key3.getColumnName(3)).isEqualTo("key9");
    assertThat(key3.getIntValue(3)).isEqualTo(2468);

    List<Value<?>> values4 = key4.get();
    assertThat(values4.size()).isEqualTo(5);
    assertThat(values4.get(0)).isEqualTo(new BooleanValue("key1", true));
    assertThat(values4.get(1)).isEqualTo(new IntValue("key2", 5678));
    assertThat(values4.get(2)).isEqualTo(new BigIntValue("key3", 1234L));
    assertThat(values4.get(3)).isEqualTo(new FloatValue("key4", 4.56f));
    assertThat(values4.get(4)).isEqualTo(new DoubleValue("key5", 1.23));

    assertThat(key4.size()).isEqualTo(5);
    assertThat(key4.getColumnName(0)).isEqualTo("key1");
    assertThat(key4.getBooleanValue(0)).isEqualTo(true);
    assertThat(key4.getColumnName(1)).isEqualTo("key2");
    assertThat(key4.getIntValue(1)).isEqualTo(5678);
    assertThat(key4.getColumnName(2)).isEqualTo("key3");
    assertThat(key4.getBigIntValue(2)).isEqualTo(1234L);
    assertThat(key4.getColumnName(3)).isEqualTo("key4");
    assertThat(key4.getFloatValue(3)).isEqualTo(4.56f);
    assertThat(key4.getColumnName(4)).isEqualTo("key5");
    assertThat(key4.getDoubleValue(4)).isEqualTo(1.23);

    List<Column<?>> columns5 = key5.getColumns();
    assertThat(columns5.size()).isEqualTo(5);
    assertThat(columns5.get(0)).isEqualTo(DateColumn.of("key1", ANY_DATE));
    assertThat(columns5.get(1)).isEqualTo(TimeColumn.of("key2", ANY_TIME));
    assertThat(columns5.get(2)).isEqualTo(TimestampColumn.of("key3", ANY_TIMESTAMP));
    assertThat(columns5.get(3)).isEqualTo(TimestampTZColumn.of("key4", ANY_TIMESTAMPTZ));
    assertThat(columns5.get(4)).isEqualTo(DoubleColumn.of("key5", 1.23));

    assertThat(key5.size()).isEqualTo(5);
    assertThat(key5.getColumnName(0)).isEqualTo("key1");
    assertThat(key5.getDateValue(0)).isEqualTo(ANY_DATE);
    assertThat(key5.getColumnName(1)).isEqualTo("key2");
    assertThat(key5.getTimeValue(1)).isEqualTo(ANY_TIME);
    assertThat(key5.getColumnName(2)).isEqualTo("key3");
    assertThat(key5.getTimestampValue(2)).isEqualTo(ANY_TIMESTAMP);
    assertThat(key5.getColumnName(3)).isEqualTo("key4");
    assertThat(key5.getTimestampTZValue(3)).isEqualTo(ANY_TIMESTAMPTZ);
    assertThat(key5.getColumnName(4)).isEqualTo("key5");
    assertThat(key5.getDoubleValue(4)).isEqualTo(1.23);
  }

  @Test
  public void ofBoolean_ShouldReturnWhatsSet() {
    // Arrange
    String name = ANY_NAME_1;
    boolean value = true;
    Key key = Key.ofBoolean(name, value);

    // Act Assert
    List<Value<?>> values = key.get();
    assertThat(values.size()).isEqualTo(1);
    assertThat(values.get(0).getName()).isEqualTo(name);
    assertThat(values.get(0).getAsBoolean()).isEqualTo(value);

    assertThat(key.size()).isEqualTo(1);
    assertThat(key.getColumnName(0)).isEqualTo(name);
    assertThat(key.getBooleanValue(0)).isEqualTo(value);
  }

  @Test
  public void ofInt_ShouldReturnWhatsSet() {
    // Arrange
    String name = ANY_NAME_1;
    int value = 100;
    Key key = Key.ofInt(name, value);

    // Act Assert
    List<Value<?>> values = key.get();
    assertThat(values.size()).isEqualTo(1);
    assertThat(values.get(0).getName()).isEqualTo(name);
    assertThat(values.get(0).getAsInt()).isEqualTo(value);

    assertThat(key.size()).isEqualTo(1);
    assertThat(key.getColumnName(0)).isEqualTo(name);
    assertThat(key.getIntValue(0)).isEqualTo(value);
  }

  @Test
  public void ofBigInt_ShouldReturnWhatsSet() {
    // Arrange
    String name = ANY_NAME_1;
    long value = 1000L;
    Key key = Key.ofBigInt(name, value);

    // Act Assert
    List<Value<?>> values = key.get();
    assertThat(values.size()).isEqualTo(1);
    assertThat(values.get(0).getName()).isEqualTo(name);
    assertThat(values.get(0).getAsLong()).isEqualTo(value);

    assertThat(key.size()).isEqualTo(1);
    assertThat(key.getColumnName(0)).isEqualTo(name);
    assertThat(key.getBigIntValue(0)).isEqualTo(value);
  }

  @Test
  public void ofFloat_ShouldReturnWhatsSet() {
    // Arrange
    String name = ANY_NAME_1;
    float value = 1.0f;
    Key key = Key.ofFloat(name, value);

    // Act Assert
    List<Value<?>> values = key.get();
    assertThat(values.size()).isEqualTo(1);
    assertThat(values.get(0).getName()).isEqualTo(name);
    assertThat(values.get(0).getAsFloat()).isEqualTo(value);

    assertThat(key.size()).isEqualTo(1);
    assertThat(key.getColumnName(0)).isEqualTo(name);
    assertThat(key.getFloatValue(0)).isEqualTo(value);
  }

  @Test
  public void ofDouble_ShouldReturnWhatsSet() {
    // Arrange
    String name = ANY_NAME_1;
    double value = 1.01d;
    Key key = Key.ofDouble(name, value);

    // Act Assert
    List<Value<?>> values = key.get();
    assertThat(values.size()).isEqualTo(1);
    assertThat(values.get(0).getName()).isEqualTo(name);
    assertThat(values.get(0).getAsDouble()).isEqualTo(value);

    assertThat(key.size()).isEqualTo(1);
    assertThat(key.getColumnName(0)).isEqualTo(name);
    assertThat(key.getDoubleValue(0)).isEqualTo(value);
  }

  @Test
  public void ofText_ShouldReturnWhatsSet() {
    // Arrange
    String name = ANY_NAME_1;
    String value = "value";
    Key key = Key.ofText(name, value);

    // Act Assert
    List<Value<?>> values = key.get();
    assertThat(values.size()).isEqualTo(1);
    assertThat(values.get(0).getName()).isEqualTo(name);
    assertThat(values.get(0).getAsString().isPresent()).isTrue();
    assertThat(values.get(0).getAsString().get()).isEqualTo(value);

    assertThat(key.size()).isEqualTo(1);
    assertThat(key.getColumnName(0)).isEqualTo(name);
    assertThat(key.getTextValue(0)).isEqualTo(value);
  }

  @Test
  public void ofBlob_ByteArrayValueGiven_ShouldReturnWhatsSet() {
    // Arrange
    String name = ANY_NAME_1;
    byte[] value = "value".getBytes(StandardCharsets.UTF_8);
    Key key = Key.ofBlob(name, value);

    // Act Assert
    List<Value<?>> values = key.get();
    assertThat(values.size()).isEqualTo(1);
    assertThat(values.get(0).getName()).isEqualTo(name);
    assertThat(values.get(0).getAsBytes().isPresent()).isTrue();
    assertThat(Arrays.equals(values.get(0).getAsBytes().get(), value)).isTrue();

    assertThat(key.size()).isEqualTo(1);
    assertThat(key.getColumnName(0)).isEqualTo(name);
    assertThat(key.getBlobValue(0)).isEqualTo(ByteBuffer.wrap(value));
    assertThat(key.getBlobValueAsByteBuffer(0)).isEqualTo(ByteBuffer.wrap(value));
    assertThat(key.getBlobValueAsBytes(0)).isEqualTo(value);
  }

  @Test
  public void ofBlob_ByteBufferValueGiven_ShouldReturnWhatsSet() {
    // Arrange
    String name = ANY_NAME_1;
    byte[] value = "value".getBytes(StandardCharsets.UTF_8);
    Key key = Key.ofBlob(name, ByteBuffer.wrap(value));

    // Act Assert
    List<Value<?>> values = key.get();
    assertThat(values.size()).isEqualTo(1);
    assertThat(values.get(0).getName()).isEqualTo(name);
    assertThat(values.get(0).getAsBytes().isPresent()).isTrue();
    assertThat(Arrays.equals(values.get(0).getAsBytes().get(), value)).isTrue();

    assertThat(key.size()).isEqualTo(1);
    assertThat(key.getColumnName(0)).isEqualTo(name);
    assertThat(key.getBlobValue(0)).isEqualTo(ByteBuffer.wrap(value));
    assertThat(key.getBlobValueAsByteBuffer(0)).isEqualTo(ByteBuffer.wrap(value));
    assertThat(key.getBlobValueAsBytes(0)).isEqualTo(value);
  }

  @Test
  public void ofDate_ShouldReturnWhatsSet() {
    // Arrange
    String name = ANY_NAME_1;
    Key key = Key.ofDate(name, ANY_DATE);

    // Act Assert
    List<Column<?>> columns = key.getColumns();
    assertThat(columns.size()).isEqualTo(1);
    assertThat(columns.get(0).getName()).isEqualTo(name);
    assertThat(columns.get(0).getDateValue()).isEqualTo(ANY_DATE);

    assertThat(key.size()).isEqualTo(1);
    assertThat(key.getColumnName(0)).isEqualTo(name);
    assertThat(key.getDateValue(0)).isEqualTo(ANY_DATE);
  }

  @Test
  public void ofTime_ShouldReturnWhatsSet() {
    // Arrange
    String name = ANY_NAME_1;
    Key key = Key.ofTime(name, ANY_TIME);

    // Act Assert
    List<Column<?>> columns = key.getColumns();
    assertThat(columns.size()).isEqualTo(1);
    assertThat(columns.get(0).getName()).isEqualTo(name);
    assertThat(columns.get(0).getTimeValue()).isEqualTo(ANY_TIME);

    assertThat(key.size()).isEqualTo(1);
    assertThat(key.getColumnName(0)).isEqualTo(name);
    assertThat(key.getTimeValue(0)).isEqualTo(ANY_TIME);
  }

  @Test
  public void ofTimestamp_ShouldReturnWhatsSet() {
    // Arrange
    String name = ANY_NAME_1;
    Key key = Key.ofTimestamp(name, ANY_TIMESTAMP);

    // Act Assert
    List<Column<?>> columns = key.getColumns();
    assertThat(columns.size()).isEqualTo(1);
    assertThat(columns.get(0).getName()).isEqualTo(name);
    assertThat(columns.get(0).getTimestampValue()).isEqualTo(ANY_TIMESTAMP);

    assertThat(key.size()).isEqualTo(1);
    assertThat(key.getColumnName(0)).isEqualTo(name);
    assertThat(key.getTimestampValue(0)).isEqualTo(ANY_TIMESTAMP);
  }

  @Test
  public void ofTimestampTZ_ShouldReturnWhatsSet() {
    // Arrange
    String name = ANY_NAME_1;
    Key key = Key.ofTimestampTZ(name, ANY_TIMESTAMPTZ);

    // Act Assert
    List<Column<?>> columns = key.getColumns();
    assertThat(columns.size()).isEqualTo(1);
    assertThat(columns.get(0).getName()).isEqualTo(name);
    assertThat(columns.get(0).getTimestampTZValue()).isEqualTo(ANY_TIMESTAMPTZ);

    assertThat(key.size()).isEqualTo(1);
    assertThat(key.getColumnName(0)).isEqualTo(name);
    assertThat(key.getTimestampTZValue(0)).isEqualTo(ANY_TIMESTAMPTZ);
  }

  @Test
  public void of_ShouldReturnWhatsSet() {
    // Arrange
    Key key1 = Key.of("key1", true, "key2", 5678);
    Key key2 = Key.of("key3", 1234L, "key4", 4.56f, "key5", 1.23);
    Key key3 =
        Key.of(
            "key6",
            "string_key",
            "key7",
            "blob_key".getBytes(StandardCharsets.UTF_8),
            "key8",
            ByteBuffer.wrap("blob_key2".getBytes(StandardCharsets.UTF_8)),
            "key9",
            2468);
    Key key4 = Key.of("key1", true, "key2", 5678, "key3", 1234L, "key4", 4.56f, "key5", 1.23);
    Key key5 =
        Key.of(
            "key1",
            ANY_DATE,
            "key2",
            ANY_TIME,
            "key3",
            ANY_TIMESTAMP,
            "key4",
            ANY_TIMESTAMPTZ,
            "key5",
            1.23);

    // Act Assert
    List<Value<?>> values1 = key1.get();
    assertThat(values1.size()).isEqualTo(2);
    assertThat(values1.get(0)).isEqualTo(new BooleanValue("key1", true));
    assertThat(values1.get(1)).isEqualTo(new IntValue("key2", 5678));

    assertThat(key1.size()).isEqualTo(2);
    assertThat(key1.getColumnName(0)).isEqualTo("key1");
    assertThat(key1.getBooleanValue(0)).isEqualTo(true);
    assertThat(key1.getColumnName(1)).isEqualTo("key2");
    assertThat(key1.getIntValue(1)).isEqualTo(5678);

    List<Value<?>> values2 = key2.get();
    assertThat(values2.size()).isEqualTo(3);
    assertThat(values2.get(0)).isEqualTo(new BigIntValue("key3", 1234L));
    assertThat(values2.get(1)).isEqualTo(new FloatValue("key4", 4.56f));
    assertThat(values2.get(2)).isEqualTo(new DoubleValue("key5", 1.23));

    assertThat(key2.size()).isEqualTo(3);
    assertThat(key2.getColumnName(0)).isEqualTo("key3");
    assertThat(key2.getBigIntValue(0)).isEqualTo(1234L);
    assertThat(key2.getColumnName(1)).isEqualTo("key4");
    assertThat(key2.getFloatValue(1)).isEqualTo(4.56f);
    assertThat(key2.getColumnName(2)).isEqualTo("key5");
    assertThat(key2.getDoubleValue(2)).isEqualTo(1.23);

    List<Value<?>> values3 = key3.get();
    assertThat(values3.size()).isEqualTo(4);
    assertThat(values3.get(0)).isEqualTo(new TextValue("key6", "string_key"));
    assertThat(values3.get(1))
        .isEqualTo(new BlobValue("key7", "blob_key".getBytes(StandardCharsets.UTF_8)));
    assertThat(values3.get(2))
        .isEqualTo(
            new BlobValue("key8", ByteBuffer.wrap("blob_key2".getBytes(StandardCharsets.UTF_8))));
    assertThat(values3.get(3)).isEqualTo(new IntValue("key9", 2468));

    assertThat(key3.size()).isEqualTo(4);
    assertThat(key3.getColumnName(0)).isEqualTo("key6");
    assertThat(key3.getTextValue(0)).isEqualTo("string_key");
    assertThat(key3.getColumnName(1)).isEqualTo("key7");
    assertThat(key3.getBlobValue(1))
        .isEqualTo(ByteBuffer.wrap("blob_key".getBytes(StandardCharsets.UTF_8)));
    assertThat(key3.getBlobValueAsByteBuffer(1))
        .isEqualTo(ByteBuffer.wrap("blob_key".getBytes(StandardCharsets.UTF_8)));
    assertThat(key3.getBlobValueAsBytes(1)).isEqualTo("blob_key".getBytes(StandardCharsets.UTF_8));
    assertThat(key3.getColumnName(2)).isEqualTo("key8");
    assertThat(key3.getBlobValue(2))
        .isEqualTo(ByteBuffer.wrap("blob_key2".getBytes(StandardCharsets.UTF_8)));
    assertThat(key3.getBlobValueAsByteBuffer(2))
        .isEqualTo(ByteBuffer.wrap("blob_key2".getBytes(StandardCharsets.UTF_8)));
    assertThat(key3.getBlobValueAsBytes(2)).isEqualTo("blob_key2".getBytes(StandardCharsets.UTF_8));
    assertThat(key3.getColumnName(3)).isEqualTo("key9");
    assertThat(key3.getIntValue(3)).isEqualTo(2468);

    List<Value<?>> values4 = key4.get();
    assertThat(values4.size()).isEqualTo(5);
    assertThat(values4.get(0)).isEqualTo(new BooleanValue("key1", true));
    assertThat(values4.get(1)).isEqualTo(new IntValue("key2", 5678));
    assertThat(values4.get(2)).isEqualTo(new BigIntValue("key3", 1234L));
    assertThat(values4.get(3)).isEqualTo(new FloatValue("key4", 4.56f));
    assertThat(values4.get(4)).isEqualTo(new DoubleValue("key5", 1.23));

    assertThat(key4.size()).isEqualTo(5);
    assertThat(key1.getColumnName(0)).isEqualTo("key1");
    assertThat(key1.getBooleanValue(0)).isEqualTo(true);
    assertThat(key1.getColumnName(1)).isEqualTo("key2");
    assertThat(key1.getIntValue(1)).isEqualTo(5678);
    assertThat(key4.getColumnName(2)).isEqualTo("key3");
    assertThat(key4.getBigIntValue(2)).isEqualTo(1234L);
    assertThat(key4.getColumnName(3)).isEqualTo("key4");
    assertThat(key4.getFloatValue(3)).isEqualTo(4.56f);
    assertThat(key4.getColumnName(4)).isEqualTo("key5");
    assertThat(key4.getDoubleValue(4)).isEqualTo(1.23);

    List<Column<?>> columns5 = key5.getColumns();
    assertThat(columns5.size()).isEqualTo(5);
    assertThat(columns5.get(0)).isEqualTo(DateColumn.of("key1", ANY_DATE));
    assertThat(columns5.get(1)).isEqualTo(TimeColumn.of("key2", ANY_TIME));
    assertThat(columns5.get(2)).isEqualTo(TimestampColumn.of("key3", ANY_TIMESTAMP));
    assertThat(columns5.get(3)).isEqualTo(TimestampTZColumn.of("key4", ANY_TIMESTAMPTZ));
    assertThat(columns5.get(4)).isEqualTo(DoubleColumn.of("key5", 1.23));

    assertThat(key5.size()).isEqualTo(5);
    assertThat(key5.getColumnName(0)).isEqualTo("key1");
    assertThat(key5.getDateValue(0)).isEqualTo(ANY_DATE);
    assertThat(key5.getColumnName(1)).isEqualTo("key2");
    assertThat(key5.getTimeValue(1)).isEqualTo(ANY_TIME);
    assertThat(key5.getColumnName(2)).isEqualTo("key3");
    assertThat(key5.getTimestampValue(2)).isEqualTo(ANY_TIMESTAMP);
    assertThat(key5.getColumnName(3)).isEqualTo("key4");
    assertThat(key5.getTimestampTZValue(3)).isEqualTo(ANY_TIMESTAMPTZ);
    assertThat(key5.getColumnName(4)).isEqualTo("key5");
    assertThat(key5.getDoubleValue(4)).isEqualTo(1.23);
  }

  @Test
  public void get_ProperKeysGivenInFactoryMethod_ShouldReturnWhatsSet() {
    // Arrange
    Key key = Key.of(ANY_NAME_1, ANY_TEXT_1, ANY_NAME_2, ANY_TEXT_2);

    // Act
    List<Value<?>> values = key.get();

    // Assert
    assertThat(values)
        .isEqualTo(
            Arrays.asList(
                new TextValue(ANY_NAME_1, ANY_TEXT_1), new TextValue(ANY_NAME_2, ANY_TEXT_2)));
  }

  @Test
  public void get_ProperKeysGivenInBuilder_ShouldReturnWhatsSet() {
    // Arrange
    Key key =
        Key.newBuilder()
            .addBoolean("key1", true)
            .addInt("key2", 5678)
            .addBigInt("key3", 1234L)
            .addFloat("key4", 4.56f)
            .addDouble("key5", 1.23)
            .addText("key6", "string_key")
            .addBlob("key7", "blob_key".getBytes(StandardCharsets.UTF_8))
            .addBlob("key8", ByteBuffer.wrap("blob_key2".getBytes(StandardCharsets.UTF_8)))
            .add(new IntValue("key9", 1357))
            .addAll(Arrays.asList(new IntValue("key10", 2468), new BigIntValue("key11", 1111L)))
            .build();

    // Act Assert
    List<Value<?>> values = key.get();
    assertThat(values.size()).isEqualTo(11);
    assertThat(values.get(0)).isEqualTo(new BooleanValue("key1", true));
    assertThat(values.get(1)).isEqualTo(new IntValue("key2", 5678));
    assertThat(values.get(2)).isEqualTo(new BigIntValue("key3", 1234L));
    assertThat(values.get(3)).isEqualTo(new FloatValue("key4", 4.56f));
    assertThat(values.get(4)).isEqualTo(new DoubleValue("key5", 1.23));
    assertThat(values.get(5)).isEqualTo(new TextValue("key6", "string_key"));
    assertThat(values.get(6))
        .isEqualTo(new BlobValue("key7", "blob_key".getBytes(StandardCharsets.UTF_8)));
    assertThat(values.get(7))
        .isEqualTo(
            new BlobValue("key8", ByteBuffer.wrap("blob_key2".getBytes(StandardCharsets.UTF_8))));
    assertThat(values.get(8)).isEqualTo(new IntValue("key9", 1357));
    assertThat(values.get(9)).isEqualTo(new IntValue("key10", 2468));
    assertThat(values.get(10)).isEqualTo(new BigIntValue("key11", 1111L));

    assertThat(key.size()).isEqualTo(11);
    assertThat(key.getColumnName(0)).isEqualTo("key1");
    assertThat(key.getBooleanValue(0)).isEqualTo(true);
    assertThat(key.getColumnName(1)).isEqualTo("key2");
    assertThat(key.getIntValue(1)).isEqualTo(5678);
    assertThat(key.getColumnName(2)).isEqualTo("key3");
    assertThat(key.getBigIntValue(2)).isEqualTo(1234L);
    assertThat(key.getColumnName(3)).isEqualTo("key4");
    assertThat(key.getFloatValue(3)).isEqualTo(4.56f);
    assertThat(key.getColumnName(4)).isEqualTo("key5");
    assertThat(key.getDoubleValue(4)).isEqualTo(1.23);
    assertThat(key.getColumnName(5)).isEqualTo("key6");
    assertThat(key.getTextValue(5)).isEqualTo("string_key");
    assertThat(key.getColumnName(6)).isEqualTo("key7");
    assertThat(key.getBlobValue(6))
        .isEqualTo(ByteBuffer.wrap("blob_key".getBytes(StandardCharsets.UTF_8)));
    assertThat(key.getBlobValueAsByteBuffer(6))
        .isEqualTo(ByteBuffer.wrap("blob_key".getBytes(StandardCharsets.UTF_8)));
    assertThat(key.getBlobValueAsBytes(6)).isEqualTo("blob_key".getBytes(StandardCharsets.UTF_8));
    assertThat(key.getColumnName(7)).isEqualTo("key8");
    assertThat(key.getBlobValue(7))
        .isEqualTo(ByteBuffer.wrap("blob_key2".getBytes(StandardCharsets.UTF_8)));
    assertThat(key.getBlobValueAsByteBuffer(7))
        .isEqualTo(ByteBuffer.wrap("blob_key2".getBytes(StandardCharsets.UTF_8)));
    assertThat(key.getBlobValueAsBytes(7)).isEqualTo("blob_key2".getBytes(StandardCharsets.UTF_8));
    assertThat(key.getColumnName(8)).isEqualTo("key9");
    assertThat(key.getIntValue(8)).isEqualTo(1357);
    assertThat(key.getColumnName(9)).isEqualTo("key10");
    assertThat(key.getIntValue(9)).isEqualTo(2468);
    assertThat(key.getColumnName(10)).isEqualTo("key11");
    assertThat(key.getBigIntValue(10)).isEqualTo(1111L);
  }

  @Test
  public void getColumns_ProperKeysGivenInBuilder_ShouldReturnWhatsSet() {
    // Arrange
    Key key =
        Key.newBuilder()
            .add(BooleanColumn.of("key1", true))
            .add(IntColumn.of("key2", 5678))
            .add(BigIntColumn.of("key3", 1234L))
            .add(FloatColumn.of("key4", 4.56f))
            .add(DoubleColumn.of("key5", 1.23))
            .add(TextColumn.of("key6", "string_key"))
            .add(BlobColumn.of("key7", "blob_key".getBytes(StandardCharsets.UTF_8)))
            .addTimestampTZ("key8", ANY_TIMESTAMPTZ)
            .addTime("key9", ANY_TIME)
            .addDate("key10", ANY_DATE)
            .addTimestamp("key11", ANY_TIMESTAMP)
            .build();

    // Act
    List<Column<?>> columns = key.getColumns();

    // Assert
    assertThat(columns.size()).isEqualTo(11);
    assertThat(columns.get(0).getName()).isEqualTo("key1");
    assertThat(columns.get(0).getBooleanValue()).isEqualTo(true);
    assertThat(columns.get(1).getName()).isEqualTo("key2");
    assertThat(columns.get(1).getIntValue()).isEqualTo(5678);
    assertThat(columns.get(2).getName()).isEqualTo("key3");
    assertThat(columns.get(2).getBigIntValue()).isEqualTo(1234L);
    assertThat(columns.get(3).getName()).isEqualTo("key4");
    assertThat(columns.get(3).getFloatValue()).isEqualTo(4.56f);
    assertThat(columns.get(4).getName()).isEqualTo("key5");
    assertThat(columns.get(4).getDoubleValue()).isEqualTo(1.23);
    assertThat(columns.get(5).getName()).isEqualTo("key6");
    assertThat(columns.get(5).getTextValue()).isEqualTo("string_key");
    assertThat(columns.get(6).getName()).isEqualTo("key7");
    assertThat(columns.get(6).getBlobValue())
        .isEqualTo(ByteBuffer.wrap("blob_key".getBytes(StandardCharsets.UTF_8)));
    assertThat(columns.get(7).getName()).isEqualTo("key8");
    assertThat(columns.get(7).getTimestampTZValue()).isEqualTo(ANY_TIMESTAMPTZ);
    assertThat(columns.get(8).getName()).isEqualTo("key9");
    assertThat(columns.get(8).getTimeValue()).isEqualTo(ANY_TIME);
    assertThat(columns.get(9).getName()).isEqualTo("key10");
    assertThat(columns.get(9).getDateValue()).isEqualTo(ANY_DATE);
    assertThat(columns.get(10).getName()).isEqualTo("key11");
    assertThat(columns.get(10).getTimestampValue()).isEqualTo(ANY_TIMESTAMP);
  }

  @Test
  public void equals_DifferentObjectsSameValuesGiven_ShouldReturnTrue() {
    // Arrange
    Key oneKey = Key.of(ANY_NAME_1, ANY_TEXT_1, ANY_NAME_2, ANY_TEXT_2);
    Key anotherKey = Key.of(ANY_NAME_1, ANY_TEXT_1, ANY_NAME_2, ANY_TEXT_2);

    // Act
    boolean result = oneKey.equals(anotherKey);

    // Assert
    assertThat(result).isTrue();
  }

  @Test
  public void equals_SameObjectsGiven_ShouldReturnTrue() {
    // Arrange
    Key oneKey = Key.of(ANY_NAME_1, ANY_TEXT_1, ANY_NAME_2, ANY_TEXT_2);

    // Act
    @SuppressWarnings("SelfEquals")
    boolean result = oneKey.equals(oneKey);

    // Assert
    assertThat(result).isTrue();
  }

  @Test
  public void equals_DifferentObjectsDifferentValuesGiven_ShouldReturnFalse() {
    // Arrange
    Key oneKey = Key.of(ANY_NAME_1, ANY_TEXT_1, ANY_NAME_2, ANY_TEXT_2);
    Key anotherKey = Key.of(ANY_NAME_3, ANY_TEXT_3, ANY_NAME_4, ANY_TEXT_4);

    // Act
    boolean result = oneKey.equals(anotherKey);

    // Assert
    assertThat(result).isFalse();
  }

  @Test
  public void equals_DifferentTypesSameValuesGiven_ShouldReturnFalse() {
    // Arrange
    Key oneKey = Key.of(ANY_NAME_1, ANY_TEXT_1, ANY_NAME_2, ANY_TEXT_2);
    Key anotherKey =
        Key.of(
            ANY_NAME_1,
            ANY_TEXT_1.getBytes(StandardCharsets.UTF_8),
            ANY_NAME_2,
            ANY_TEXT_2.getBytes(StandardCharsets.UTF_8));

    // Act
    boolean result = oneKey.equals(anotherKey);

    // Assert
    assertThat(result).isFalse();
  }

  @Test
  public void compareTo_ThisTextBiggerThanGiven_ShouldReturnPositive() {
    // Arrange
    Key oneKey = Key.of(ANY_NAME_3, ANY_TEXT_3, ANY_NAME_4, ANY_TEXT_4);
    Key anotherKey = Key.of(ANY_NAME_1, ANY_TEXT_1, ANY_NAME_2, ANY_TEXT_2);

    // Act
    int actual = oneKey.compareTo(anotherKey);

    // Assert
    assertThat(actual > 0).isTrue();
  }

  @Test
  public void compareTo_ThisNumberBiggerThanGiven_ShouldReturnPositive() {
    // Arrange
    Key oneKey = Key.of(ANY_NAME_1, ANY_INT_1, ANY_NAME_2, ANY_INT_2);
    Key anotherKey = Key.of(ANY_NAME_1, ANY_INT_1, ANY_NAME_2, ANY_INT_1);

    // Act
    int actual = oneKey.compareTo(anotherKey);

    // Assert
    assertThat(actual > 0).isTrue();
  }

  @Test
  public void compareTo_ThisEqualsToGiven_ShouldReturnZero() {
    // Arrange
    Key oneKey = Key.of(ANY_NAME_1, ANY_TEXT_1, ANY_NAME_2, ANY_INT_2);
    Key anotherKey = Key.of(ANY_NAME_1, ANY_TEXT_1, ANY_NAME_2, ANY_INT_2);

    // Act
    int actual = oneKey.compareTo(anotherKey);

    // Assert
    assertThat(actual == 0).isTrue();
  }

  @Test
  public void compareTo_ThisTextSmallerThanGiven_ShouldReturnNegative() {
    // Arrange
    Key oneKey = Key.of(ANY_NAME_1, ANY_TEXT_1, ANY_NAME_2, ANY_TEXT_2);
    Key anotherKey = Key.of(ANY_NAME_3, ANY_TEXT_3, ANY_NAME_4, ANY_TEXT_4);

    // Act
    int actual = oneKey.compareTo(anotherKey);

    // Assert
    assertThat(actual < 0).isTrue();
  }

  @Test
  public void compareTo_ThisNumberSmallerThanGiven_ShouldReturnNegative() {
    // Arrange
    Key oneKey = Key.of(ANY_NAME_1, ANY_INT_1, ANY_NAME_2, ANY_INT_1);
    Key anotherKey = Key.of(ANY_NAME_1, ANY_INT_1, ANY_NAME_2, ANY_INT_2);

    // Act
    int actual = oneKey.compareTo(anotherKey);

    // Assert
    assertThat(actual < 0).isTrue();
  }

  @Test
  public void constructor_NullGiven_ShouldThrowNullPointerException() {
    // Act Assert
    assertThatThrownBy(() -> new Key((List<Value<?>>) null))
        .isInstanceOf(NullPointerException.class);
  }
}
