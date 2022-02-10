package com.scalar.db.io;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ComparisonChain;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

/**
 * A {@code Value} for a string
 *
 * @author Hiroyuki Yamada
 */
@Immutable
public final class TextValue implements Value<Optional<String>> {
  private static final String ANONYMOUS = "";
  private final String name;
  private final Optional<String> value;

  /**
   * Constructs a {@code TextValue} with the specified name and value
   *
   * @param name name of the {@code Value}
   * @param value content of the {@code Value} in byte array
   * @deprecated As of release 3.5.0. Will be removed in release 5.0.0
   */
  @Deprecated
  public TextValue(String name, @Nullable byte[] value) {
    this.name = checkNotNull(name);
    if (value == null) {
      this.value = Optional.empty();
    } else {
      this.value = Optional.of(new String(value, StandardCharsets.UTF_8));
    }
  }

  /**
   * Constructs a {@code TextValue} with the specified value. The name of this value is anonymous.
   *
   * @param value content of the {@code Value}
   * @deprecated As of release 3.5.0. Will be removed in release 5.0.0
   */
  @Deprecated
  public TextValue(@Nullable byte[] value) {
    this(ANONYMOUS, value);
  }

  /**
   * Constructs a {@code TextValue} with the specified name and value
   *
   * @param name name of the {@code Value}
   * @param value content of the {@code Value} in {@code String}
   */
  public TextValue(String name, @Nullable String value) {
    checkNotNull(name);
    this.name = name;
    if (value == null) {
      this.value = Optional.empty();
    } else {
      this.value = Optional.of(value);
    }
  }

  /**
   * Constructs a {@code TextValue} with the specified value. The name of this value is anonymous.
   *
   * @param value content of the {@code Value}
   */
  public TextValue(@Nullable String value) {
    this(ANONYMOUS, value);
  }

  @Override
  @Nonnull
  public Optional<String> get() {
    return value;
  }

  /**
   * Returns the content of this {@code Value}
   *
   * @return an {@code Optional} of the content of this {@code Value} in byte array
   * @deprecated As of release 3.2.0, replaced by {@link #getAsBytes()}. Will be removed in release
   *     5.0.0
   */
  @SuppressWarnings("InlineMeSuggester")
  @Deprecated
  @Nonnull
  public Optional<byte[]> getBytes() {
    return getAsBytes();
  }

  /**
   * Returns the content of this {@code Value}
   *
   * @return an {@code Optional} of the content of this {@code Value} in {@code String}
   * @deprecated As of release 3.2.0, replaced by {@link #getAsString()}. Will be removed in release
   *     5.0.0
   */
  @SuppressWarnings("InlineMeSuggester")
  @Deprecated
  @Nonnull
  public Optional<String> getString() {
    return get();
  }

  @Override
  public Optional<String> getAsString() {
    return get();
  }

  @Override
  public Optional<byte[]> getAsBytes() {
    return value.map(v -> v.getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public Optional<ByteBuffer> getAsByteBuffer() {
    return getAsBytes().map(ByteBuffer::wrap);
  }

  @Override
  @Nonnull
  public String getName() {
    return name;
  }

  @Override
  public TextValue copyWith(String name) {
    return value
        .map(s -> new TextValue(name, s))
        .orElseGet(() -> new TextValue(name, (String) null));
  }

  @Override
  public void accept(ValueVisitor v) {
    v.visit(this);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, value);
  }

  /**
   * Indicates whether some other object is "equal to" this object. The other object is considered
   * equal if:
   *
   * <ul>
   *   <li>both super class instances are equal and
   *   <li>it is also an {@code TextValue} and
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
    if (!(o instanceof TextValue)) {
      return false;
    }
    TextValue other = (TextValue) o;
    return name.equals(other.name) && value.equals(other.value);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("name", name)
        .add("value", get().toString())
        .toString();
  }

  @Override
  public int compareTo(@Nonnull Value<Optional<String>> o) {
    if (value.isPresent() && o.get().isPresent()) {
      return ComparisonChain.start()
          .compare(value.get(), o.get().get())
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
