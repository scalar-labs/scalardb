/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.scalar.db.storage.dynamo.ordered.util;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Extends {@link ByteRange} with additional methods to support
 * tracking a consumers position within the viewport. The API is extended with methods {@link
 * #get()} and {@link #put(byte)} for interacting with the backing array from the current position
 * forward. This frees the caller from managing their own index into the array.
 *
 * <p>Designed to be a slimmed-down, mutable alternative to {@link java.nio.ByteBuffer}.
 */
@InterfaceAudience.Public
public interface PositionedByteRange extends ByteRange {

  // net new API is here.

  /**
   * The current {@code position} marker. This valuae is 0-indexed, relative to the beginning of the
   * range.
   */
  int getPosition();

  /**
   * Update the {@code position} index. May not be greater than {@code length}.
   *
   * @param position the new position in this range.
   * @return this.
   */
  PositionedByteRange setPosition(int position);

  /** The number of bytes remaining between position and the end of the range. */
  int getRemaining();

  /** Retrieve the next byte from this range without incrementing position. */
  byte peek();

  /** Retrieve the next byte from this range. */
  byte get();

  /** Retrieve the next short value from this range. */
  short getShort();

  /** Retrieve the next int value from this range. */
  int getInt();

  /** Retrieve the next long value from this range. */
  long getLong();

  /**
   * Retrieve the next long value, which is stored as VLong, from this range
   *
   * @return the long value which is stored as VLong
   */
  long getVLong();

  /**
   * Fill {@code dst} with bytes from the range, starting from {@code position}. This range's {@code
   * position} is incremented by the length of {@code dst}, the number of bytes copied.
   *
   * @param dst the destination of the copy.
   * @return this.
   */
  PositionedByteRange get(byte[] dst);

  /**
   * Fill {@code dst} with bytes from the range, starting from the current {@code position}. {@code
   * length} bytes are copied into {@code dst}, starting at {@code offset}. This range's {@code
   * position} is incremented by the number of bytes copied.
   *
   * @param dst the destination of the copy.
   * @param offset the offset into {@code dst} to start the copy.
   * @param length the number of bytes to copy into {@code dst}.
   * @return this.
   */
  PositionedByteRange get(byte[] dst, int offset, int length);

  /**
   * Store {@code val} at the next position in this range.
   *
   * @param val the new value.
   * @return this.
   */
  PositionedByteRange put(byte val);

  /**
   * Store short {@code val} at the next position in this range.
   *
   * @param val the new value.
   * @return this.
   */
  PositionedByteRange putShort(short val);

  /**
   * Store int {@code val} at the next position in this range.
   *
   * @param val the new value.
   * @return this.
   */
  PositionedByteRange putInt(int val);

  /**
   * Store long {@code val} at the next position in this range.
   *
   * @param val the new value.
   * @return this.
   */
  PositionedByteRange putLong(long val);

  /**
   * Store the long {@code val} at the next position as a VLong
   *
   * @param val the value to store
   * @return number of bytes written
   */
  int putVLong(long val);

  /**
   * Store the content of {@code val} in this range, starting at the next position.
   *
   * @param val the new value.
   * @return this.
   */
  PositionedByteRange put(byte[] val);

  /**
   * Store {@code length} bytes from {@code val} into this range. Bytes from {@code val} are copied
   * starting at {@code offset} into the range, starting at the current position.
   *
   * @param val the new value.
   * @param offset the offset in {@code val} from which to start copying.
   * @param length the number of bytes to copy from {@code val}.
   * @return this.
   */
  PositionedByteRange put(byte[] val, int offset, int length);

  /**
   * Return the current limit
   *
   * @return limit
   */
  int getLimit();

  /**
   * Limits the byte range upto a specified value. Limit cannot be greater than capacity
   *
   * @return PositionedByteRange
   */
  PositionedByteRange setLimit(int limit);

  // override parent interface declarations to return this interface.

  @Override
  PositionedByteRange unset();

  @Override
  PositionedByteRange set(int capacity);

  @Override
  PositionedByteRange set(byte[] bytes);

  @Override
  PositionedByteRange set(byte[] bytes, int offset, int length);

  @Override
  PositionedByteRange setOffset(int offset);

  @Override
  PositionedByteRange setLength(int length);

  @Override
  PositionedByteRange get(int index, byte[] dst);

  @Override
  PositionedByteRange get(int index, byte[] dst, int offset, int length);

  @Override
  PositionedByteRange put(int index, byte val);

  @Override
  PositionedByteRange putShort(int index, short val);

  @Override
  PositionedByteRange putInt(int index, int val);

  @Override
  PositionedByteRange putLong(int index, long val);

  @Override
  PositionedByteRange put(int index, byte[] val);

  @Override
  PositionedByteRange put(int index, byte[] val, int offset, int length);

  @Override
  PositionedByteRange deepCopy();

  @Override
  PositionedByteRange shallowCopy();

  @Override
  PositionedByteRange shallowCopySubRange(int innerOffset, int copyLength);
}
