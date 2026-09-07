package com.scalar.db.io;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ComparisonChain;
import java.util.Comparator;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

/** A {@code Column} for an TEXT type. */
@Immutable
public class TextColumn implements Column<String> {

  private static final Comparator<String> NULLS_FIRST_CODE_POINT_ORDER =
      Comparator.nullsFirst(TextColumn::compareByCodePoint);

  private final String name;
  @Nullable private final String value;

  private TextColumn(String name, @Nullable String value) {
    this.name = Objects.requireNonNull(name);
    this.value = value;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Optional<String> getValue() {
    return Optional.ofNullable(value);
  }

  @Override
  @Nullable
  public String getTextValue() {
    return value;
  }

  @Override
  public TextColumn copyWith(String name) {
    return new TextColumn(name, value);
  }

  @Override
  public DataType getDataType() {
    return DataType.TEXT;
  }

  @Override
  public boolean hasNullValue() {
    return value == null;
  }

  @Override
  @Nullable
  public Object getValueAsObject() {
    return value;
  }

  @Override
  public int compareTo(Column<String> o) {
    return ComparisonChain.start()
        .compare(getName(), o.getName())
        .compareTrueFirst(hasNullValue(), o.hasNullValue())
        .compare(getTextValue(), o.getTextValue(), NULLS_FIRST_CODE_POINT_ORDER)
        .result();
  }

  /**
   * Orders two strings by Unicode code point, which for well-formed text is the unsigned UTF-8 byte
   * order that byte-order backends use. {@link String#compareTo} orders by UTF-16 code unit and so
   * places supplementary characters before U+E000..U+FFFF; comparing the UTF-8 encodings instead
   * would conflate distinct strings whose unpaired surrogates all encode to the replacement byte.
   * This order returns 0 only for {@link String#equals equal} strings, ill-formed ones included.
   */
  static int compareByCodePoint(String left, String right) {
    int i = 0;
    int j = 0;
    while (i < left.length() && j < right.length()) {
      int l = left.codePointAt(i);
      int r = right.codePointAt(j);
      if (l != r) {
        return Integer.compare(l, r);
      }
      i += Character.charCount(l);
      j += Character.charCount(r);
    }
    return Integer.compare(left.length() - i, right.length() - j);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TextColumn)) {
      return false;
    }
    TextColumn that = (TextColumn) o;
    return Objects.equals(name, that.name) && Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, value);
  }

  @Override
  public void accept(ColumnVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("name", name).add("value", value).toString();
  }

  /**
   * Returns a Text column instance with the specified column name and value.
   *
   * @param columnName a column name
   * @param value a column value
   * @return a Text column instance with the specified column name and value
   */
  public static TextColumn of(String columnName, @Nullable String value) {
    return new TextColumn(columnName, value);
  }

  /**
   * Returns a Text column instance with the specified column name and a null value.
   *
   * @param columnName a column name
   * @return a Text column instance with the specified column name and a null value
   */
  public static TextColumn ofNull(String columnName) {
    return of(columnName, null);
  }
}
