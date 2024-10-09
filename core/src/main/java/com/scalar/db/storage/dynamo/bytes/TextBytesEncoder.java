package com.scalar.db.storage.dynamo.bytes;

import static com.scalar.db.storage.dynamo.bytes.BytesUtils.mask;

import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.io.TextColumn;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class TextBytesEncoder implements BytesEncoder<TextColumn> {
  private static final byte TERM = 0x00;

  TextBytesEncoder() {}

  @Override
  public int encodedLength(TextColumn column, Order order) {
    assert column.getValue().isPresent();

    return column.getValue().get().getBytes(StandardCharsets.UTF_8).length + 1;
  }

  @Override
  public void encode(TextColumn column, Order order, ByteBuffer dst) {
    assert column.getValue().isPresent();

    String value = column.getValue().get();

    if (value.contains("\u0000")) {
      throw new IllegalArgumentException(
          CoreError.DYNAMO_ENCODER_CANNOT_ENCODE_TEXT_VALUE_CONTAINING_0X0000.buildMessage());
    }

    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    for (byte b : bytes) {
      dst.put(mask(b, order));
    }
    dst.put(mask(TERM, order));
  }
}
