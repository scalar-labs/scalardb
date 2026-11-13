package com.scalar.db.storage.jdbc;

import com.google.common.base.MoreObjects;
import java.util.Objects;
import javax.annotation.concurrent.Immutable;

/** A database encoding under which TEXT columns do not behave as ScalarDB expects. */
@Immutable
final class DatabaseEncodingIssue {

  enum Kind {
    /** The encoding cannot store every Unicode character. */
    PARTIAL_CHARACTER_SET,
    /** The encoding stores every Unicode character but orders values by UTF-16 code unit. */
    UTF16_CODE_UNIT_ORDER
  }

  private final String encoding;
  private final Kind kind;

  DatabaseEncodingIssue(String encoding, Kind kind) {
    this.encoding = encoding;
    this.kind = kind;
  }

  String getEncoding() {
    return encoding;
  }

  Kind getKind() {
    return kind;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof DatabaseEncodingIssue)) {
      return false;
    }
    DatabaseEncodingIssue that = (DatabaseEncodingIssue) o;
    return encoding.equals(that.encoding) && kind == that.kind;
  }

  @Override
  public int hashCode() {
    return Objects.hash(encoding, kind);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("encoding", encoding).add("kind", kind).toString();
  }
}
