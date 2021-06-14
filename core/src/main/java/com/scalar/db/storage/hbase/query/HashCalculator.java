package com.scalar.db.storage.hbase.query;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.scalar.db.io.BigIntValue;
import com.scalar.db.io.BlobValue;
import com.scalar.db.io.BooleanValue;
import com.scalar.db.io.DoubleValue;
import com.scalar.db.io.FloatValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.ValueVisitor;
import java.nio.charset.Charset;

public class HashCalculator implements ValueVisitor {

  private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

  private final Key key;
  private final Hasher hasher = Hashing.murmur3_32().newHasher();

  public HashCalculator(Key key) {
    this.key = key;
  }

  public int calculate() {
    key.forEach(v -> v.accept(this));
    return hasher.hash().asInt();
  }

  @Override
  public void visit(BooleanValue value) {
    hasher.putBoolean(value.getAsBoolean());
  }

  @Override
  public void visit(IntValue value) {
    hasher.putInt(value.getAsInt());
  }

  @Override
  public void visit(BigIntValue value) {
    hasher.putLong(value.getAsLong());
  }

  @Override
  public void visit(FloatValue value) {
    hasher.putFloat(value.getAsFloat());
  }

  @Override
  public void visit(DoubleValue value) {
    hasher.putDouble(value.getAsDouble());
  }

  @Override
  public void visit(TextValue value) {
    // TODO defaultCharset?
    hasher.putString(value.getAsString().orElse(""), Charset.defaultCharset());
  }

  @Override
  public void visit(BlobValue value) {
    hasher.putBytes(value.getAsBytes().orElse(EMPTY_BYTE_ARRAY));
  }
}
