package com.scalar.db.config;

import com.google.common.base.Strings;
import java.util.Properties;
import javax.annotation.Nullable;

public final class ConfigUtils {

  private ConfigUtils() {}

  public static String getString(Properties properties, String name, String defaultValue) {
    String value = trim(properties.getProperty(name));
    if (Strings.isNullOrEmpty(value)) {
      return defaultValue;
    }
    return value;
  }

  public static int getInt(Properties properties, String name, int defaultValue) {
    String value = trim(properties.getProperty(name));
    if (Strings.isNullOrEmpty(value)) {
      return defaultValue;
    }
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException ignored) {
      throw new IllegalArgumentException(
          "the specified value of '" + name + "' is not a number. value: " + value);
    }
  }

  public static Integer getInt(Properties properties, String name, Integer defaultValue) {
    String value = trim(properties.getProperty(name));
    if (Strings.isNullOrEmpty(value)) {
      return defaultValue;
    }
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException ignored) {
      throw new IllegalArgumentException(
          "the specified value of '" + name + "' is not a number. value: " + value);
    }
  }

  public static long getLong(Properties properties, String name, long defaultValue) {
    String value = trim(properties.getProperty(name));
    if (Strings.isNullOrEmpty(value)) {
      return defaultValue;
    }
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException ignored) {
      throw new IllegalArgumentException(
          "the specified value of '" + name + "' is not a number. value: " + value);
    }
  }

  public static Long getLong(Properties properties, String name, Long defaultValue) {
    String value = trim(properties.getProperty(name));
    if (Strings.isNullOrEmpty(value)) {
      return defaultValue;
    }
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException ignored) {
      throw new IllegalArgumentException(
          "the specified value of '" + name + "' is not a number. value: " + value);
    }
  }

  public static boolean getBoolean(Properties properties, String name, boolean defaultValue) {
    String value = trim(properties.getProperty(name));
    if (Strings.isNullOrEmpty(value)) {
      return defaultValue;
    }
    if (Boolean.TRUE.toString().equalsIgnoreCase(value)
        || Boolean.FALSE.toString().equalsIgnoreCase(value)) {
      return Boolean.parseBoolean(value);
    } else {
      throw new IllegalArgumentException(
          "the specified value of '" + name + "' is not a boolean value. value: " + value);
    }
  }

  public static Boolean getBoolean(Properties properties, String name, Boolean defaultValue) {
    String value = trim(properties.getProperty(name));
    if (Strings.isNullOrEmpty(value)) {
      return defaultValue;
    }
    if (Boolean.TRUE.toString().equalsIgnoreCase(value)
        || Boolean.FALSE.toString().equalsIgnoreCase(value)) {
      return Boolean.parseBoolean(value);
    } else {
      throw new IllegalArgumentException(
          "the specified value of '" + name + "' is not a boolean value. value: " + value);
    }
  }

  public static String[] getStringArray(Properties properties, String name, String[] defaultValue) {
    String value = trim(properties.getProperty(name));
    if (Strings.isNullOrEmpty(value)) {
      return defaultValue;
    }
    return value.split("\\s*,\\s*");
  }

  @Nullable
  private static String trim(@Nullable String value) {
    if (value == null) {
      return null;
    }
    return value.trim();
  }
}
