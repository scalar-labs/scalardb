package com.scalar.db.storage.dynamo.bytes;

import com.scalar.db.api.Scan.Ordering.Order;

public final class BytesUtils {
  private static final byte MASK = (byte) 0xff;

  private BytesUtils() {}

  /**
   * Masks the specified byte if DESC order
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
}
