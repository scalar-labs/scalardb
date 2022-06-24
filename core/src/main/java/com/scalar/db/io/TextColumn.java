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
        .compare(getTextValue(), o.getTextValue(), Comparator.nullsFirst(Comparator.naturalOrder()))
        .result();
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
