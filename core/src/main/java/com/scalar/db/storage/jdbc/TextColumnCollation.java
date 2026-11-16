package com.scalar.db.storage.jdbc;

import com.google.common.base.MoreObjects;
import java.util.Objects;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

/** The collation and charset the catalog records for a character column. */
@Immutable
final class TextColumnCollation {
  private final String collation;
  @Nullable private final String charset;
  @Nullable private final String resolvedDefault;

  TextColumnCollation(String collation, @Nullable String charset) {
    this(collation, charset, null);
  }

  /**
   * @param resolvedDefault for a collation that points to a database or session setting (PostgreSQL
   *     {@code default}, Oracle {@code USING_NLS_COMP}), the value of that setting. Null otherwise.
   */
  TextColumnCollation(
      String collation, @Nullable String charset, @Nullable String resolvedDefault) {
    this.collation = collation;
    this.charset = charset;
    this.resolvedDefault = resolvedDefault;
  }

  String getCollation() {
    return collation;
  }

  @Nullable
  String getCharset() {
    return charset;
  }

  @Nullable
  String getResolvedDefault() {
    return resolvedDefault;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TextColumnCollation)) {
      return false;
    }
    TextColumnCollation that = (TextColumnCollation) o;
    return collation.equals(that.collation)
        && Objects.equals(charset, that.charset)
        && Objects.equals(resolvedDefault, that.resolvedDefault);
  }

  @Override
  public int hashCode() {
    return Objects.hash(collation, charset, resolvedDefault);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("collation", collation)
        .add("charset", charset)
        .add("resolvedDefault", resolvedDefault)
        .toString();
  }
}
