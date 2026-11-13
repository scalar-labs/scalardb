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

  TextColumnCollation(String collation, @Nullable String charset) {
    this.collation = collation;
    this.charset = charset;
  }

  String getCollation() {
    return collation;
  }

  @Nullable
  String getCharset() {
    return charset;
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
    return collation.equals(that.collation) && Objects.equals(charset, that.charset);
  }

  @Override
  public int hashCode() {
    return Objects.hash(collation, charset);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("collation", collation)
        .add("charset", charset)
        .toString();
  }
}
