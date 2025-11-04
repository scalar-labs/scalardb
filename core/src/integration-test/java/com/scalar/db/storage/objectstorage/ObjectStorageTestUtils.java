package com.scalar.db.storage.objectstorage;

import com.scalar.db.io.TextColumn;
import com.scalar.db.util.TestUtils;
import java.util.stream.IntStream;

public class ObjectStorageTestUtils {
  public static TextColumn getMinTextValue(String columnName) {
    // Since ObjectStorage can't handle an empty string correctly, we use "0" as the min value
    return TextColumn.of(columnName, "0");
  }

  public static TextColumn getMaxTextValue(String columnName) {
    // Since ObjectStorage can't handle 0xFF character correctly, we use "ZZZ..." as the max value
    StringBuilder builder = new StringBuilder();
    IntStream.range(0, TestUtils.MAX_TEXT_COUNT).forEach(i -> builder.append('Z'));
    return TextColumn.of(columnName, builder.toString());
  }
}
