package com.scalar.db.dataloader.core.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class PathUtilTest {

  @Test
  void ensureTrailingSlash_nullPath_returnsEmptyString() {
    String path = null;
    String result = PathUtil.ensureTrailingSlash(path);
    Assertions.assertEquals("", result);
  }

  @Test
  void ensureTrailingSlash_emptyPath_returnsEmptyString() {
    String path = "";
    String result = PathUtil.ensureTrailingSlash(path);
    Assertions.assertEquals("", result);
  }

  @Test
  void ensureTrailingSlash_pathWithoutTrailingSlash_addsTrailingSlash() {
    String path = "/path/to/directory";
    String expectedResult = "/path/to/directory/";
    String result = PathUtil.ensureTrailingSlash(path);
    Assertions.assertEquals(expectedResult, result);
  }

  @Test
  void ensureTrailingSlash_pathWithTrailingSlash_returnsOriginalPath() {
    String path = "/path/to/directory/";
    String expectedResult = "/path/to/directory/";
    String result = PathUtil.ensureTrailingSlash(path);
    Assertions.assertEquals(expectedResult, result);
  }

  @Test
  void ensureTrailingSlash_virtualPath_addsTrailingSlash() {
    String path = "s3://bucket/path";
    String expectedResult = "s3://bucket/path/";
    String result = PathUtil.ensureTrailingSlash(path);
    Assertions.assertEquals(expectedResult, result);
  }
}
