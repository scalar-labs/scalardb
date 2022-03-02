package com.scalar.db.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

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
    assertThat(key.getBoolean(0)).isEqualTo(value);
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
    assertThat(key.getInt(0)).isEqualTo(value);
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
    assertThat(key.getBigInt(0)).isEqualTo(value);
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
    assertThat(key.getFloat(0)).isEqualTo(value);
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
    assertThat(key.getDouble(0)).isEqualTo(value);
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
    assertThat(key.getText(0)).isEqualTo(value);
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
    assertThat(key.getBlob(0)).isEqualTo(ByteBuffer.wrap(value));
    assertThat(key.getBlobAsByteBuffer(0)).isEqualTo(ByteBuffer.wrap(value));
    assertThat(key.getBlobAsBytes(0)).isEqualTo(value);
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
    assertThat(key.getBlob(0)).isEqualTo(ByteBuffer.wrap(value));
    assertThat(key.getBlobAsByteBuffer(0)).isEqualTo(ByteBuffer.wrap(value));
    assertThat(key.getBlobAsBytes(0)).isEqualTo(value);
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

    // Act Assert
    List<Value<?>> values1 = key1.get();
    assertThat(values1.size()).isEqualTo(2);
    assertThat(values1.get(0)).isEqualTo(new BooleanValue("key1", true));
    assertThat(values1.get(1)).isEqualTo(new IntValue("key2", 5678));

    assertThat(key1.size()).isEqualTo(2);
    assertThat(key1.getColumnName(0)).isEqualTo("key1");
    assertThat(key1.getBoolean(0)).isEqualTo(true);
    assertThat(key1.getColumnName(1)).isEqualTo("key2");
    assertThat(key1.getInt(1)).isEqualTo(5678);

    List<Value<?>> values2 = key2.get();
    assertThat(values2.size()).isEqualTo(3);
    assertThat(values2.get(0)).isEqualTo(new BigIntValue("key3", 1234L));
    assertThat(values2.get(1)).isEqualTo(new FloatValue("key4", 4.56f));
    assertThat(values2.get(2)).isEqualTo(new DoubleValue("key5", 1.23));

    assertThat(key2.size()).isEqualTo(3);
    assertThat(key2.getColumnName(0)).isEqualTo("key3");
    assertThat(key2.getBigInt(0)).isEqualTo(1234L);
    assertThat(key2.getColumnName(1)).isEqualTo("key4");
    assertThat(key2.getFloat(1)).isEqualTo(4.56f);
    assertThat(key2.getColumnName(2)).isEqualTo("key5");
    assertThat(key2.getDouble(2)).isEqualTo(1.23);

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
    assertThat(key3.getText(0)).isEqualTo("string_key");
    assertThat(key3.getColumnName(1)).isEqualTo("key7");
    assertThat(key3.getBlob(1))
        .isEqualTo(ByteBuffer.wrap("blob_key".getBytes(StandardCharsets.UTF_8)));
    assertThat(key3.getBlobAsByteBuffer(1))
        .isEqualTo(ByteBuffer.wrap("blob_key".getBytes(StandardCharsets.UTF_8)));
    assertThat(key3.getBlobAsBytes(1)).isEqualTo("blob_key".getBytes(StandardCharsets.UTF_8));
    assertThat(key3.getColumnName(2)).isEqualTo("key8");
    assertThat(key3.getBlob(2))
        .isEqualTo(ByteBuffer.wrap("blob_key2".getBytes(StandardCharsets.UTF_8)));
    assertThat(key3.getBlobAsByteBuffer(2))
        .isEqualTo(ByteBuffer.wrap("blob_key2".getBytes(StandardCharsets.UTF_8)));
    assertThat(key3.getBlobAsBytes(2)).isEqualTo("blob_key2".getBytes(StandardCharsets.UTF_8));
    assertThat(key3.getColumnName(3)).isEqualTo("key9");
    assertThat(key3.getInt(3)).isEqualTo(2468);

    List<Value<?>> values4 = key4.get();
    assertThat(values4.size()).isEqualTo(5);
    assertThat(values4.get(0)).isEqualTo(new BooleanValue("key1", true));
    assertThat(values4.get(1)).isEqualTo(new IntValue("key2", 5678));
    assertThat(values4.get(2)).isEqualTo(new BigIntValue("key3", 1234L));
    assertThat(values4.get(3)).isEqualTo(new FloatValue("key4", 4.56f));
    assertThat(values4.get(4)).isEqualTo(new DoubleValue("key5", 1.23));

    assertThat(key4.size()).isEqualTo(5);
    assertThat(key1.getColumnName(0)).isEqualTo("key1");
    assertThat(key1.getBoolean(0)).isEqualTo(true);
    assertThat(key1.getColumnName(1)).isEqualTo("key2");
    assertThat(key1.getInt(1)).isEqualTo(5678);
    assertThat(key4.getColumnName(2)).isEqualTo("key3");
    assertThat(key4.getBigInt(2)).isEqualTo(1234L);
    assertThat(key4.getColumnName(3)).isEqualTo("key4");
    assertThat(key4.getFloat(3)).isEqualTo(4.56f);
    assertThat(key4.getColumnName(4)).isEqualTo("key5");
    assertThat(key4.getDouble(4)).isEqualTo(1.23);
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
    assertThat(key.getBoolean(0)).isEqualTo(value);
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
    assertThat(key.getInt(0)).isEqualTo(value);
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
    assertThat(key.getBigInt(0)).isEqualTo(value);
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
    assertThat(key.getFloat(0)).isEqualTo(value);
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
    assertThat(key.getDouble(0)).isEqualTo(value);
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
    assertThat(key.getText(0)).isEqualTo(value);
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
    assertThat(key.getBlob(0)).isEqualTo(ByteBuffer.wrap(value));
    assertThat(key.getBlobAsByteBuffer(0)).isEqualTo(ByteBuffer.wrap(value));
    assertThat(key.getBlobAsBytes(0)).isEqualTo(value);
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
    assertThat(key.getBlob(0)).isEqualTo(ByteBuffer.wrap(value));
    assertThat(key.getBlobAsByteBuffer(0)).isEqualTo(ByteBuffer.wrap(value));
    assertThat(key.getBlobAsBytes(0)).isEqualTo(value);
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

    // Act Assert
    List<Value<?>> values1 = key1.get();
    assertThat(values1.size()).isEqualTo(2);
    assertThat(values1.get(0)).isEqualTo(new BooleanValue("key1", true));
    assertThat(values1.get(1)).isEqualTo(new IntValue("key2", 5678));

    assertThat(key1.size()).isEqualTo(2);
    assertThat(key1.getColumnName(0)).isEqualTo("key1");
    assertThat(key1.getBoolean(0)).isEqualTo(true);
    assertThat(key1.getColumnName(1)).isEqualTo("key2");
    assertThat(key1.getInt(1)).isEqualTo(5678);

    List<Value<?>> values2 = key2.get();
    assertThat(values2.size()).isEqualTo(3);
    assertThat(values2.get(0)).isEqualTo(new BigIntValue("key3", 1234L));
    assertThat(values2.get(1)).isEqualTo(new FloatValue("key4", 4.56f));
    assertThat(values2.get(2)).isEqualTo(new DoubleValue("key5", 1.23));

    assertThat(key2.size()).isEqualTo(3);
    assertThat(key2.getColumnName(0)).isEqualTo("key3");
    assertThat(key2.getBigInt(0)).isEqualTo(1234L);
    assertThat(key2.getColumnName(1)).isEqualTo("key4");
    assertThat(key2.getFloat(1)).isEqualTo(4.56f);
    assertThat(key2.getColumnName(2)).isEqualTo("key5");
    assertThat(key2.getDouble(2)).isEqualTo(1.23);

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
    assertThat(key3.getText(0)).isEqualTo("string_key");
    assertThat(key3.getColumnName(1)).isEqualTo("key7");
    assertThat(key3.getBlob(1))
        .isEqualTo(ByteBuffer.wrap("blob_key".getBytes(StandardCharsets.UTF_8)));
    assertThat(key3.getBlobAsByteBuffer(1))
        .isEqualTo(ByteBuffer.wrap("blob_key".getBytes(StandardCharsets.UTF_8)));
    assertThat(key3.getBlobAsBytes(1)).isEqualTo("blob_key".getBytes(StandardCharsets.UTF_8));
    assertThat(key3.getColumnName(2)).isEqualTo("key8");
    assertThat(key3.getBlob(2))
        .isEqualTo(ByteBuffer.wrap("blob_key2".getBytes(StandardCharsets.UTF_8)));
    assertThat(key3.getBlobAsByteBuffer(2))
        .isEqualTo(ByteBuffer.wrap("blob_key2".getBytes(StandardCharsets.UTF_8)));
    assertThat(key3.getBlobAsBytes(2)).isEqualTo("blob_key2".getBytes(StandardCharsets.UTF_8));
    assertThat(key3.getColumnName(3)).isEqualTo("key9");
    assertThat(key3.getInt(3)).isEqualTo(2468);

    List<Value<?>> values4 = key4.get();
    assertThat(values4.size()).isEqualTo(5);
    assertThat(values4.get(0)).isEqualTo(new BooleanValue("key1", true));
    assertThat(values4.get(1)).isEqualTo(new IntValue("key2", 5678));
    assertThat(values4.get(2)).isEqualTo(new BigIntValue("key3", 1234L));
    assertThat(values4.get(3)).isEqualTo(new FloatValue("key4", 4.56f));
    assertThat(values4.get(4)).isEqualTo(new DoubleValue("key5", 1.23));

    assertThat(key4.size()).isEqualTo(5);
    assertThat(key1.getColumnName(0)).isEqualTo("key1");
    assertThat(key1.getBoolean(0)).isEqualTo(true);
    assertThat(key1.getColumnName(1)).isEqualTo("key2");
    assertThat(key1.getInt(1)).isEqualTo(5678);
    assertThat(key4.getColumnName(2)).isEqualTo("key3");
    assertThat(key4.getBigInt(2)).isEqualTo(1234L);
    assertThat(key4.getColumnName(3)).isEqualTo("key4");
    assertThat(key4.getFloat(3)).isEqualTo(4.56f);
    assertThat(key4.getColumnName(4)).isEqualTo("key5");
    assertThat(key4.getDouble(4)).isEqualTo(1.23);
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
    assertThat(key.getBoolean(0)).isEqualTo(true);
    assertThat(key.getColumnName(1)).isEqualTo("key2");
    assertThat(key.getInt(1)).isEqualTo(5678);
    assertThat(key.getColumnName(2)).isEqualTo("key3");
    assertThat(key.getBigInt(2)).isEqualTo(1234L);
    assertThat(key.getColumnName(3)).isEqualTo("key4");
    assertThat(key.getFloat(3)).isEqualTo(4.56f);
    assertThat(key.getColumnName(4)).isEqualTo("key5");
    assertThat(key.getDouble(4)).isEqualTo(1.23);
    assertThat(key.getColumnName(5)).isEqualTo("key6");
    assertThat(key.getText(5)).isEqualTo("string_key");
    assertThat(key.getColumnName(6)).isEqualTo("key7");
    assertThat(key.getBlob(6))
        .isEqualTo(ByteBuffer.wrap("blob_key".getBytes(StandardCharsets.UTF_8)));
    assertThat(key.getBlobAsByteBuffer(6))
        .isEqualTo(ByteBuffer.wrap("blob_key".getBytes(StandardCharsets.UTF_8)));
    assertThat(key.getBlobAsBytes(6)).isEqualTo("blob_key".getBytes(StandardCharsets.UTF_8));
    assertThat(key.getColumnName(7)).isEqualTo("key8");
    assertThat(key.getBlob(7))
        .isEqualTo(ByteBuffer.wrap("blob_key2".getBytes(StandardCharsets.UTF_8)));
    assertThat(key.getBlobAsByteBuffer(7))
        .isEqualTo(ByteBuffer.wrap("blob_key2".getBytes(StandardCharsets.UTF_8)));
    assertThat(key.getBlobAsBytes(7)).isEqualTo("blob_key2".getBytes(StandardCharsets.UTF_8));
    assertThat(key.getColumnName(8)).isEqualTo("key9");
    assertThat(key.getInt(8)).isEqualTo(1357);
    assertThat(key.getColumnName(9)).isEqualTo("key10");
    assertThat(key.getInt(9)).isEqualTo(2468);
    assertThat(key.getColumnName(10)).isEqualTo("key11");
    assertThat(key.getBigInt(10)).isEqualTo(1111L);
  }

  @Test
  public void get_TryToModifyReturned_ShouldThrowException() {
    // Arrange
    Key key = Key.of(ANY_NAME_1, ANY_TEXT_1, ANY_NAME_2, ANY_TEXT_2);

    // Act Assert
    List<Value<?>> values = key.get();
    assertThatThrownBy(() -> values.add(new TextValue(ANY_NAME_3, ANY_TEXT_3)))
        .isInstanceOf(UnsupportedOperationException.class);
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
