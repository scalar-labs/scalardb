package com.scalar.db.storage.dynamo.bytes;

import static com.scalar.db.storage.dynamo.bytes.BytesUtils.mask;

import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.io.TextValue;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class TextBytesEncoder implements BytesEncoder<TextValue> {
  private static final byte TERM = 0x00;

  TextBytesEncoder() {}

  @Override
  public int encodedLength(TextValue value, Order order) {
    assert value.getAsString().isPresent();
    return value.getAsString().get().getBytes(StandardCharsets.UTF_8).length + 1;
  }

  @Override
  public void encode(TextValue value, Order order, ByteBuffer dst) {
    assert value.getAsString().isPresent();
    if (value.getAsString().get().contains("\u0000")) {
      throw new IllegalArgumentException(
          CoreError.DYNAMO_ENCODER_CANNOT_ENCODE_TEXT_VALUE_CONTAINING_0X0000.buildMessage());
    }

    byte[] bytes = value.getAsString().get().getBytes(StandardCharsets.UTF_8);
    for (byte b : bytes) {
      dst.put(mask(b, order));
    }
    dst.put(mask(TERM, order));
  }
}
