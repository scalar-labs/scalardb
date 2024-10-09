package com.scalar.db.storage.dynamo.bytes;

import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.io.Column;
import java.nio.ByteBuffer;

/**
 * A bytes-encoder that encodes a column to bytes while preserving the sort order.
 *
 * @param <T> the value type
 */
public interface BytesEncoder<T extends Column<?>> {

  /**
   * Calculates the encoded bytes length.
   *
   * @param column a column
   * @param order an order
   * @return the encoded bytes length
   */
  int encodedLength(T column, Order order);

  /**
   * Encodes the column to bytes
   *
   * @param column a column
   * @param order an order
   * @param dst a ByteBuffer to write the encoded bytes
   */
  void encode(T column, Order order, ByteBuffer dst);
}
