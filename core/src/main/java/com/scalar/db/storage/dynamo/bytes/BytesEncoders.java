package com.scalar.db.storage.dynamo.bytes;

public final class BytesEncoders {
  public static final BooleanBytesEncoder BOOLEAN = new BooleanBytesEncoder();
  public static final IntBytesEncoder INT = new IntBytesEncoder();
  public static final BigIntBytesEncoder BIGINT = new BigIntBytesEncoder();
  public static final FloatBytesEncoder FLOAT = new FloatBytesEncoder();
  public static final DoubleBytesEncoder DOUBLE = new DoubleBytesEncoder();
  public static final TextBytesEncoder TEXT = new TextBytesEncoder();
  public static final BlobBytesEncoder BLOB = new BlobBytesEncoder();
}
