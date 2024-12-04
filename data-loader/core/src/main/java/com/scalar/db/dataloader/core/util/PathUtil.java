package com.scalar.db.dataloader.core.util;

public class PathUtil {

  /**
   * Ensures the specified path has a trailing slash.
   *
   * <p>java.nio.file.Path is not used because this is also used for virtual paths.
   *
   * @param path the path
   * @return the path with a trailing slash
   */
  public static String ensureTrailingSlash(String path) {
    if (path == null || path.isEmpty()) {
      return "";
    }

    if (!path.endsWith("/")) {
      return path + "/";
    }

    return path;
  }
}
