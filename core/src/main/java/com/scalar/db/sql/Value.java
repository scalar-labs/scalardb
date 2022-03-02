package com.scalar.db.sql;

import java.nio.ByteBuffer;
import javax.annotation.Nullable;

public class Value {

  public final Type type;
  @Nullable public final Object value;

  private Value(Type type, @Nullable Object value) {
    this.type = type;
    this.value = value;
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
