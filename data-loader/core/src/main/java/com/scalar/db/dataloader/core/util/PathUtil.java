package com.scalar.db.dataloader.core.util;

import java.io.File;

/**
 * A utility class for common operations related to file system paths.
 *
 * <p>Provides helper methods such as ensuring a trailing path separator for directory paths.
 */
public class PathUtil {

  /**
   * Ensures the specified path has a trailing path separator.
   *
   * @param path the path
   * @return the path with a trailing path separator.
   */
  public static String ensureTrailingSeparator(String path) {
    if (path == null || path.isEmpty()) {
      return "";
    }

    if (!path.endsWith(File.separator)) {
      return path + File.separator;
    }

    return path;
  }
}
