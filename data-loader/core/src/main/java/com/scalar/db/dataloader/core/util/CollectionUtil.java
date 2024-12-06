package com.scalar.db.dataloader.core.util;

import java.util.Collection;

/** Utils for collection classes */
public class CollectionUtil {

  /**
   * Check if lists are of same length
   *
   * @param collections List of collections
   * @return collections are same length or not
   */
  public static boolean areSameLength(Collection<?>... collections) {
    int N = collections[0].size();
    for (Collection<?> a : collections) if (a.size() != N) return false;
    return true;
  }
}
