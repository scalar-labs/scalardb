package com.scalar.db.storage.jdbc;

import com.google.common.base.MoreObjects;
import java.util.Objects;
import javax.annotation.concurrent.Immutable;

/** A database setting under which TEXT columns do not behave as ScalarDB expects. */
@Immutable
public final class DatabaseTextIssue {

  enum Kind {
    /** The encoding cannot store every Unicode character. */
    PARTIAL_CHARACTER_SET,
    /** The encoding stores every Unicode character, but its byte order is not code point order. */
    ENCODING_BYTE_ORDER,
    /** The database collation does not compare by code point. */
    NON_BINARY_COLLATION
  }

  private final String setting;
  private final Kind kind;

  DatabaseTextIssue(String setting, Kind kind) {
    this.setting = setting;
    this.kind = kind;
  }

  String getSetting() {
    return setting;
  }

  Kind getKind() {
    return kind;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DatabaseTextIssue)) {
      return false;
    }
    DatabaseTextIssue that = (DatabaseTextIssue) o;
    return setting.equals(that.setting) && kind == that.kind;
  }

  @Override
  public int hashCode() {
    return Objects.hash(setting, kind);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("setting", setting).add("kind", kind).toString();
  }
}
