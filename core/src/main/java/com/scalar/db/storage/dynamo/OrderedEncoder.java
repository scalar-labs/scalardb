package com.scalar.db.storage.dynamo;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.io.BigIntValue;
import com.scalar.db.io.BooleanValue;
import com.scalar.db.io.DoubleValue;
import com.scalar.db.io.FloatValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.Value;
import org.apache.hadoop.hbase.types.OrderedNumeric;
import org.apache.hadoop.hbase.types.OrderedString;
import org.apache.hadoop.hbase.util.SimplePositionedMutableByteRange;

public class OrderedEncoder {
  private static final ImmutableMap<Order, org.apache.hadoop.hbase.util.Order> ORDER_MAP =
      ImmutableMap.<Order, org.apache.hadoop.hbase.util.Order>builder()
          .put(Order.ASC, org.apache.hadoop.hbase.util.Order.ASCENDING)
          .put(Order.DESC, org.apache.hadoop.hbase.util.Order.DESCENDING)
          .build();

  private static final ImmutableMap<String, Number> MAX_VALUE_MAP =
      ImmutableMap.<String, Number>builder()
          .put(IntValue.class.getName(), Integer.MAX_VALUE)
          .put(BigIntValue.class.getName(), Long.MAX_VALUE)
          .put(FloatValue.class.getName(), Float.MAX_VALUE)
          .put(DoubleValue.class.getName(), Double.MAX_VALUE)
          .put(BooleanValue.class.getName(), (byte) 1)
          .build();

  public static byte[] orderedEncode(Value value, Order order) {
    SimplePositionedMutableByteRange simplePositionedMutableByteRange;

    if (value instanceof TextValue) {
      OrderedString orderedString = new OrderedString(ORDER_MAP.get(order));
      String textValue = ((TextValue) value).get().get();
      simplePositionedMutableByteRange =
          new SimplePositionedMutableByteRange(orderedString.encodedLength(textValue));
      orderedString.encode(simplePositionedMutableByteRange, textValue);
    } else {
      OrderedNumeric orderedNumeric = new OrderedNumeric(ORDER_MAP.get(order));

      // using max value of the primitive type to get length of encoded bytes to keep order when
      // concatenating values after
      simplePositionedMutableByteRange =
          new SimplePositionedMutableByteRange(
              orderedNumeric.encodedLength(getMaxNumberValue(value)));
      orderedNumeric.encode(simplePositionedMutableByteRange, (Number) value.get());
    }
    return simplePositionedMutableByteRange.getBytes();
  }

  public static Number getMaxNumberValue(Value value) {
    return MAX_VALUE_MAP.get(value.getClass().getName());
  }
}
