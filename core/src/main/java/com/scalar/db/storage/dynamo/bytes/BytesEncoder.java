package com.scalar.db.storage.dynamo.bytes;

import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.io.Value;
import java.nio.ByteBuffer;

/**
 * A bytes-encoder that encodes a value to bytes while preserving the sort order.
 *
 * @param <T> the value type
 */
public interface BytesEncoder<T extends Value<?>> {

  /**
   * Calculates the encoded bytes length.
   *
   * @param value a value
   * @param order an order
   * @return the encoded bytes length
   */
  int encodedLength(T value, Order order);

  /**
   * Encodes the value to bytes
   *
   * @param value a value
   * @param order an order
   * @param dst a ByteBuffer to write the encoded bytes
   */
  void encode(T value, Order order, ByteBuffer dst);
}
