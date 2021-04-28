package com.scalar.db.io;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ComparisonChain;

import java.nio.Buffer;
import java.nio.ByteBuffer;
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
public final class BlobValue implements Value<BlobValue> {
  private static final String ANONYMOUS = "";
  private final String name;
  private final Optional<ByteBuffer> bytes;

  /**
   * Constructs a {@code BlobValue} with the specified name and value
   *
   * @param name name of the {@code Value}
   * @param value content of the {@code Value}
   */
  public BlobValue(String name, byte[] value) {
    this.name = checkNotNull(name);
    if (value == null) {
      bytes = Optional.empty();
    } else {
      bytes = Optional.of(ByteBuffer.allocate(value.length));
      bytes.ifPresent(
          b -> {
            b.put(value);
            ((Buffer)b).flip();
          });
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

  /**
   * Returns the content of this {@code Value}
   *
   * @return an {@code Optional} of the content of this {@code Value}
   */
  public Optional<byte[]> get() {
    return bytes.map(b -> Arrays.copyOf(b.array(), b.limit()));
  }

  @Override
  @Nonnull
  public String getName() {
    return name;
  }

  @Override
  public BlobValue copyWith(String name) {
    if (bytes.isPresent()) {
      return new BlobValue(name, bytes.get().array());
    }
    return new BlobValue(name, (byte[]) null);
  }

  @Override
  public void accept(ValueVisitor v) {
    v.visit(this);
  }

  @Override
  public int hashCode() {
    return bytes.hashCode();
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
    return this.name.equals(other.name) && bytes.equals(other.bytes);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("name", name)
        .add("value", bytes.toString())
        .toString();
  }

  @Override
  public int compareTo(BlobValue o) {
    if (bytes.isPresent() && o.bytes.isPresent()) {
      return ComparisonChain.start()
          .compare(bytes.get(), o.bytes.get())
          .compare(this.name, o.name)
          .result();
    } else {
      // either bytes or o.bytes is empty
      if (bytes.isPresent()) {
        return 1;
      } else if (o.bytes.isPresent()) {
        return -1;
      } else {
        return 0;
      }
    }
  }
}
