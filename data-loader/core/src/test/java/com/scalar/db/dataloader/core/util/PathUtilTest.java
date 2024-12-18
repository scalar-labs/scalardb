package com.scalar.db.dataloader.core.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class PathUtilTest {

  @Test
  void ensureTrailingSeparator_nullPath_returnsEmptyString() {
    String result = PathUtil.ensureTrailingSeparator(null);
    Assertions.assertEquals("", result);
  }

  @Test
  void ensureTrailingSeparator_emptyPath_returnsEmptyString() {
    String result = PathUtil.ensureTrailingSeparator("");
    Assertions.assertEquals("", result);
  }

  @Test
  void ensureTrailingSlash_pathWithoutTrailingSlash_addsTrailingSeparator() {
    String path = "/path/to/directory";
    String expectedResult = "/path/to/directory/";
    String result = PathUtil.ensureTrailingSeparator(path);
    Assertions.assertEquals(expectedResult, result);
  }

  @Test
  void ensureTrailingSlash_pathWithTrailingSeparator_returnsOriginalPath() {
    String path = "/path/to/directory/";
    String expectedResult = "/path/to/directory/";
    String result = PathUtil.ensureTrailingSeparator(path);
    Assertions.assertEquals(expectedResult, result);
  }
}
