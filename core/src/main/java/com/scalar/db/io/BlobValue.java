package com.scalar.db.io;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ComparisonChain;
import com.google.common.primitives.UnsignedBytes;
import java.util.Arrays;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/**
 * A {@code Value} for a binary data
 *
 * @author Hiroyuki Yamada
 */
@Immutable
public final class BlobValue implements Value<Optional<byte[]>> {
  private static final String ANONYMOUS = "";
  private final String name;
  private final Optional<byte[]> value;

  /**
   * Constructs a {@code BlobValue} with the specified name and value
   *
   * @param name name of the {@code Value}
   * @param value content of the {@code Value}
   */
  public BlobValue(String name, byte[] value) {
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
   * Constructs a {@code BlobValue} with the specified value. The name of this value is anonymous.
   *
   * @param value content of the {@code Value}
   */
  public BlobValue(byte[] value) {
    this(ANONYMOUS, value);
  }

  @Override
  @Nonnull
  public Optional<byte[]> get() {
    return value;
  }

  @Override
  public Optional<byte[]> getAsBytes() {
    return value;
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
      return new BlobValue(name, null);
    }
  }

  @Override
  public void accept(ValueVisitor v) {
    v.visit(this);
  }

  @Override
  public int hashCode() {
    return value.hashCode();
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
  public int compareTo(Value<Optional<byte[]>> o) {
    BlobValue other = (BlobValue) o;
    if (value.isPresent() && other.value.isPresent()) {
      return ComparisonChain.start()
          .compare(value.get(), other.value.get(), UnsignedBytes.lexicographicalComparator())
          .compare(name, other.name)
          .result();
    } else {
      // either bytes or o.bytes is empty
      if (value.isPresent()) {
        return 1;
      } else if (other.value.isPresent()) {
        return -1;
      } else {
        return 0;
      }
    }
  }
}
