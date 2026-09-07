package com.scalar.db.io;

import com.scalar.db.config.DatabaseConfig;
import java.util.Properties;

/** Collation comparators shared by unit tests. */
public final class CollationComparators {

  public static final CollationComparator BINARY = from("BINARY", null);

  /** Primary strength only, so values differing in case or accents collate equal. */
  public static final CollationComparator CASE_INSENSITIVE_ICU = from("ICU", "[strength 1]");

  private CollationComparators() {}

  private static CollationComparator from(String collation, String icuRules) {
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, "localhost");
    props.setProperty(DatabaseConfig.COLLATION, collation);
    if (icuRules != null) {
      props.setProperty(DatabaseConfig.COLLATION_ICU_RULES, icuRules);
    }
    return CollationComparator.from(new DatabaseConfig(props));
  }
}
