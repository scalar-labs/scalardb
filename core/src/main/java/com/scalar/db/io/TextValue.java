package com.scalar.db.io;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ComparisonChain;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import javax.annotation.Nonnull;
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
   */
  public TextValue(String name, byte[] value) {
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
   */
  public TextValue(byte[] value) {
    this(ANONYMOUS, value);
  }

  /**
   * Constructs a {@code TextValue} with the specified name and value
   *
   * @param name name of the {@code Value}
   * @param value content of the {@code Value} in {@code String}
   */
  public TextValue(String name, String value) {
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
  public TextValue(String value) {
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
    return value.hashCode();
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
    return MoreObjects.toStringHelper(this).add("name", name).add("value", getString()).toString();
  }

  @Override
  public int compareTo(Value<Optional<String>> o) {
    TextValue other = (TextValue) o;
    if (value.isPresent() && other.value.isPresent()) {
      return ComparisonChain.start()
          .compare(value.get(), other.value.get())
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
