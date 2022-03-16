package com.scalar.db.io;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ComparisonChain;
import com.google.common.primitives.UnsignedBytes;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

/**
 * A {@code Value} (column) for a binary data
 *
 * @author Hiroyuki Yamada
 * @deprecated As of release 3.6.0. Will be removed in release 5.0.0
 */
@Deprecated
@Immutable
public final class BlobValue implements Value<Optional<byte[]>> {
  private static final String ANONYMOUS = "";
  private final String name;
  private final Optional<byte[]> value;

  /**
   * Constructs a {@code BlobValue} with the specified name and value
   *
   * @param name name of the {@code Value} (column)
   * @param value value of the {@code Value} (column)
   */
  public BlobValue(String name, @Nullable byte[] value) {
    this.name = checkNotNull(name);
    if (value == null) {
      this.value = Optional.empty();
    } else {
      byte[] bytes = new byte[value.length];
      System.arraycopy(value, 0, bytes, 0, value.length);
      this.value = Optional.of(bytes);
    }
  }

  /**
   * Constructs a {@code BlobValue} with the specified value. The name of this value (column) is
   * anonymous.
   *
   * @param value value of the {@code Value} (column)
   */
  public BlobValue(@Nullable byte[] value) {
    this(ANONYMOUS, value);
  }

  /**
   * Constructs a {@code BlobValue} with the specified name and value
   *
   * @param name name of the {@code Value} (column)
   * @param value value of the {@code Value} (column)
   */
  public BlobValue(String name, @Nullable ByteBuffer value) {
    this.name = checkNotNull(name);
    if (value == null) {
      this.value = Optional.empty();
    } else {
      byte[] bytes = new byte[value.remaining()];
      value.get(bytes);
      this.value = Optional.of(bytes);
    }
  }

  /**
   * Constructs a {@code BlobValue} with the specified value. The name of this value (column) is
   * anonymous.
   *
   * @param value value of the {@code Value} (column)
   */
  public BlobValue(@Nullable ByteBuffer value) {
    this(ANONYMOUS, value);
  }

  @Override
  @Nonnull
  public Optional<byte[]> get() {
    return value.map(
        v -> {
          byte[] bytes = new byte[v.length];
          System.arraycopy(v, 0, bytes, 0, v.length);
          return bytes;
        });
  }

  @Override
  public DataType getDataType() {
    return DataType.BLOB;
  }

  @Override
  public Optional<byte[]> getAsBytes() {
    return get();
  }

  @Override
  public Optional<ByteBuffer> getAsByteBuffer() {
    return get().map(ByteBuffer::wrap);
  }

  @Override
  @Nonnull
  public String getName() {
    return name;
  }

  @Override
  public BlobValue copyWith(String name) {
    if (value.isPresent()) {
      byte[] bytes = new byte[value.get().length];
      System.arraycopy(value.get(), 0, bytes, 0, value.get().length);
      return new BlobValue(name, bytes);
    } else {
      return new BlobValue(name, (byte[]) null);
    }
  }

  @Override
  public void accept(ValueVisitor v) {
    v.visit(this);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, Arrays.hashCode(value.orElse(null)));
  }

  /**
   * Indicates whether some other object is "equal to" this object. The other object is considered
   * equal if:
   *
   * <ul>
   *   <li>both super class instances are equal and
   *   <li>it is also an {@code BlogValue} and
   *   <li>both instances have the same name and value
   * </ul>
   *
   * @param o an object to be tested for equality
   * @return {@code true} if the other object is "equal to" this object otherwise {@code false}
   */
  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof BlobValue)) {
      return false;
    }
    BlobValue other = (BlobValue) o;
    if (!name.equals(other.name)) {
      return false;
    }
    if (value.isPresent() && other.value.isPresent()) {
      return Arrays.equals(value.get(), other.value.get());
    }
    return !value.isPresent() && !other.value.isPresent();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("name", name)
        .add("value", value.map(Arrays::toString).toString())
        .toString();
  }

  @Override
  public int compareTo(@Nonnull Value<Optional<byte[]>> o) {
    if (value.isPresent() && o.get().isPresent()) {
      return ComparisonChain.start()
          .compare(value.get(), o.get().get(), UnsignedBytes.lexicographicalComparator())
          .compare(name, o.getName())
          .result();
    } else {
      // either bytes or o.bytes is empty
      if (value.isPresent()) {
        return 1;
      } else if (o.get().isPresent()) {
        return -1;
      } else {
        return 0;
      }
    }
  }
}
