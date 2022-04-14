package com.scalar.db.sql;

import com.google.common.base.MoreObjects;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

@Immutable
public class Value implements Term {

  public final Type type;
  @Nullable public final Object value;

  private Value(Type type, @Nullable Object value) {
    this.type = Objects.requireNonNull(type);
    this.value = value;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("type", type).add("value", value).toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Value)) {
      return false;
    }
    Value that = (Value) o;
    if (type != that.type) {
      return false;
    }
    if (type == Type.BLOB_BYTES) {
      return Arrays.equals((byte[]) value, (byte[]) that.value);
    }
    return Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, value);
  }

  public static Value ofBoolean(boolean value) {
    return new Value(Type.BOOLEAN, value);
  }

  public static Value ofInt(int value) {
    return new Value(Type.INT, value);
  }

  public static Value ofBigInt(long value) {
    return new Value(Type.BIGINT, value);
  }

  public static Value ofFloat(float value) {
    return new Value(Type.FLOAT, value);
  }

  public static Value ofDouble(double value) {
    return new Value(Type.DOUBLE, value);
  }

  public static Value ofText(String value) {
    return new Value(Type.TEXT, value);
  }

  public static Value ofBlob(ByteBuffer value) {
    return new Value(Type.BLOB_BYTE_BUFFER, value);
  }

  public static Value ofBlob(byte[] value) {
    return new Value(Type.BLOB_BYTES, value);
  }

  public static Value ofNull() {
    return new Value(Type.NULL, null);
  }

  public enum Type {
    BOOLEAN,
    INT,
    BIGINT,
    FLOAT,
    DOUBLE,
    TEXT,
    BLOB_BYTE_BUFFER,
    BLOB_BYTES,
    NULL
  }
}
