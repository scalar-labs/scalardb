package com.scalar.db.storage.dynamo.ordered;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.Scan.Ordering.Order;
import com.scalar.db.io.BigIntValue;
import com.scalar.db.io.BlobValue;
import com.scalar.db.io.BooleanValue;
import com.scalar.db.io.DoubleValue;
import com.scalar.db.io.FloatValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.Value;
import com.scalar.db.storage.dynamo.ordered.util.OrderedBlobVar;
import com.scalar.db.storage.dynamo.ordered.util.OrderedBytesBase;
import com.scalar.db.storage.dynamo.ordered.util.OrderedFloat32;
import com.scalar.db.storage.dynamo.ordered.util.OrderedFloat64;
import com.scalar.db.storage.dynamo.ordered.util.OrderedInt32;
import com.scalar.db.storage.dynamo.ordered.util.OrderedInt64;
import com.scalar.db.storage.dynamo.ordered.util.OrderedInt8;
import com.scalar.db.storage.dynamo.ordered.util.OrderedString;
import com.scalar.db.storage.dynamo.ordered.util.SimplePositionedMutableByteRange;

public class OrderedEncoder {

  private static final com.scalar.db.storage.dynamo.ordered.util.Order ASC_ORDERED_ENCODING =
      com.scalar.db.storage.dynamo.ordered.util.Order.ASCENDING;
  private static final com.scalar.db.storage.dynamo.ordered.util.Order DESC_ORDERED_ENCODING =
      com.scalar.db.storage.dynamo.ordered.util.Order.DESCENDING;

  private static final ImmutableMap<String, OrderedBytesBase<?>> ASC_ORDERED_ENCODER_MAP =
      ImmutableMap.<String, OrderedBytesBase<?>>builder()
          .put(IntValue.class.getName(), new OrderedInt32(ASC_ORDERED_ENCODING))
          .put(BigIntValue.class.getName(), new OrderedInt64(ASC_ORDERED_ENCODING))
          .put(FloatValue.class.getName(), new OrderedFloat32(ASC_ORDERED_ENCODING))
          .put(DoubleValue.class.getName(), new OrderedFloat64(ASC_ORDERED_ENCODING))
          .put(BlobValue.class.getName(), new OrderedBlobVar(ASC_ORDERED_ENCODING))
          .put(TextValue.class.getName(), new OrderedString(ASC_ORDERED_ENCODING))
          .put(BooleanValue.class.getName(), new OrderedInt8(ASC_ORDERED_ENCODING))
          .build();
  private static final ImmutableMap<String, OrderedBytesBase<?>> DESC_ORDERED_ENCODER_MAP =
      ImmutableMap.<String, OrderedBytesBase<?>>builder()
          .put(IntValue.class.getName(), new OrderedInt32(DESC_ORDERED_ENCODING))
          .put(BigIntValue.class.getName(), new OrderedInt64(DESC_ORDERED_ENCODING))
          .put(FloatValue.class.getName(), new OrderedFloat32(DESC_ORDERED_ENCODING))
          .put(DoubleValue.class.getName(), new OrderedFloat64(DESC_ORDERED_ENCODING))
          .put(BlobValue.class.getName(), new OrderedBlobVar(DESC_ORDERED_ENCODING))
          .put(TextValue.class.getName(), new OrderedString(DESC_ORDERED_ENCODING))
          .put(BooleanValue.class.getName(), new OrderedInt8(DESC_ORDERED_ENCODING))
          .build();

  private static final ImmutableMap<Order, ImmutableMap<String, OrderedBytesBase<?>>>
      ORDERED_ENCODER_MAP =
          ImmutableMap.<Order, ImmutableMap<String, OrderedBytesBase<?>>>builder()
              .put(Order.ASC, ASC_ORDERED_ENCODER_MAP)
              .put(Order.DESC, DESC_ORDERED_ENCODER_MAP)
              .build();

  @SuppressWarnings("unchecked")
  public static byte[] orderedEncode(Value<?> value, Order order) {
    SimplePositionedMutableByteRange simplePositionedMutableByteRange;
    OrderedBytesBase orderedBytesBaseEncoder =
        ORDERED_ENCODER_MAP.get(order).get(value.getClass().getName());

    if (value instanceof TextValue) {
      if (!((TextValue) value).get().isPresent()) {
        return new byte[] {0};
      }
      String textValue = ((TextValue) value).get().get();
      simplePositionedMutableByteRange =
          new SimplePositionedMutableByteRange(orderedBytesBaseEncoder.encodedLength(textValue));
      orderedBytesBaseEncoder.encode(simplePositionedMutableByteRange, textValue);
    } else if (value instanceof BooleanValue) {
      boolean booleanValue = ((BooleanValue) value).get();
      byte byteValue = (byte) (booleanValue ? 1 : 0);
      simplePositionedMutableByteRange =
          new SimplePositionedMutableByteRange(orderedBytesBaseEncoder.encodedLength(byteValue));
      orderedBytesBaseEncoder.encode(simplePositionedMutableByteRange, byteValue);
    } else if (value instanceof BlobValue) {
      if (!((BlobValue) value).get().isPresent()) {
        return new byte[] {0};
      }
      byte[] bytes = ((BlobValue) value).get().get();
      simplePositionedMutableByteRange =
          new SimplePositionedMutableByteRange(orderedBytesBaseEncoder.encodedLength(bytes));
      orderedBytesBaseEncoder.encode(simplePositionedMutableByteRange, bytes);
    } else {
      simplePositionedMutableByteRange =
          new SimplePositionedMutableByteRange(orderedBytesBaseEncoder.encodedLength(value.get()));
      orderedBytesBaseEncoder.encode(simplePositionedMutableByteRange, value.get());
    }
    return simplePositionedMutableByteRange.getBytes();
  }
}
