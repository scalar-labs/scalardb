package com.scalar.db.storage.jdbc;

/** How a TEXT column orders and stores values, compared with how ScalarDB expects it to. */
public enum TextColumnCollationKind {
  /** The character set stores every Unicode character and the collation orders by code point. */
  CODE_POINT_ORDER,
  /** The character set stores every Unicode character, but the collation orders otherwise. */
  OTHER_ORDER,
  /**
   * The character set cannot store every Unicode character. A binary collation over such a
   * character set orders by the bytes of that character set, so the order is not code point order
   * either.
   */
  PARTIAL_CHARACTER_SET;

  static TextColumnCollationKind of(boolean fullUnicodeCharset, boolean binaryCollation) {
    if (!fullUnicodeCharset) {
      return PARTIAL_CHARACTER_SET;
    }
    return binaryCollation ? CODE_POINT_ORDER : OTHER_ORDER;
  }
}
