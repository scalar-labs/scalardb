package com.scalar.db.storage.dynamo.bytes;

import com.scalar.db.api.Scan.Ordering.Order;
import java.nio.ByteBuffer;
import java.util.Optional;

public final class BytesUtils {
  private static final byte MASK = (byte) 0xff;

  private BytesUtils() {}

  /**
   * Masks the specified byte if DESC order.
   *
   * @param b a byte
   * @param order an order
   * @return a masked byte if DESC order, otherwise the specified byte without any processing
   */
  public static byte mask(byte b, Order order) {
    if (order == Order.ASC) {
      // do nothing, just return the specified byte
      return b;
    } else {
      // mask the specified byte to make it descending order
      return (byte) (b ^ MASK);
    }
  }

  /**
   * Returns the closest next bytes of the specified bytes.
   *
   * @param bytes bytes
   * @return the closest next bytes of the bytes
   */
  public static Optional<ByteBuffer> getClosestNextBytes(ByteBuffer bytes) {
    int offset = bytes.remaining();

    if (offset == 0) {
      // if the bytes are empty, the next bytes are {0x00}
      return Optional.of(ByteBuffer.wrap(new byte[] {0}));
    }

    // search for the place where the trailing 0xFFs start
    while (offset > 0) {
      if (bytes.get(offset - 1) != (byte) 0xFF) {
        break;
      }
      offset--;
    }

    if (offset == 0) {
      // if the bytes are 0xFFFF... (only FFs) bytes value
      return Optional.empty();
    }

    // copy the right length of the original
    ByteBuffer newBytes = copy(bytes, offset);

    // increment the last one
    newBytes.put(offset - 1, (byte) (bytes.get(offset - 1) + 1));

    return Optional.of(newBytes);
  }

  /**
   * Returns the closest previous bytes of the specified bytes.
   *
   * @param bytes bytes
   * @return the closest previous bytes of the bytes
   */
  public static Optional<ByteBuffer> getClosestPreviousBytes(ByteBuffer bytes) {
    int length = bytes.remaining();

    if (length == 0) {
      // if the bytes are empty, the previous bytes are nothing
      return Optional.empty();
    }

    if (bytes.get(length - 1) == 0x00) {
      // if the tail of the bytes is 0x00, cut the last byte off
      return Optional.of(copy(bytes, length - 1));
    }

    // copy the right length of the original
    ByteBuffer newBytes = copy(bytes, length);

    // decrement the last one
    newBytes.put(length - 1, (byte) (bytes.get(length - 1) - 1));

    return Optional.of(newBytes);
  }

  private static ByteBuffer copy(ByteBuffer src, int length) {
    ByteBuffer newBytes = ByteBuffer.allocate(length);
    for (int i = 0; i < length; i++) {
      newBytes.put(src.get());
    }
    newBytes.flip();
    return newBytes;
  }

  /**
   * Converts the specified bytebuffer to a byte array.
   *
   * @param src the source bytebuffer
   * @return a converted byte array
   */
  public static byte[] toBytes(ByteBuffer src) {
    byte[] bytes = new byte[src.remaining()];
    src.get(bytes);
    return bytes;
  }

  static void encodeLong(long value, Order order, ByteBuffer dst) {
    // Flip a sign bit to make it binary comparable
    dst.put(mask((byte) ((value >> 56) ^ 0x80), order));
    dst.put(mask((byte) (value >> 48), order));
    dst.put(mask((byte) (value >> 40), order));
    dst.put(mask((byte) (value >> 32), order));
    dst.put(mask((byte) (value >> 24), order));
    dst.put(mask((byte) (value >> 16), order));
    dst.put(mask((byte) (value >> 8), order));
    dst.put(mask((byte) value, order));
  }

  static void encodeInt(int value, Order order, ByteBuffer dst) {
    // Flip a sign bit to make it binary comparable
    dst.put(mask((byte) ((value >> 24) ^ 0x80), order));
    dst.put(mask((byte) (value >> 16), order));
    dst.put(mask((byte) (value >> 8), order));
    dst.put(mask((byte) value, order));
  }
}
