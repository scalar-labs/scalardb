package com.scalar.db.config;

import com.google.common.base.Strings;
import java.util.Properties;

public final class ConfigUtils {

  private ConfigUtils() {}

  public static String getString(Properties properties, String name, String defaultValue) {
    String value = properties.getProperty(name);
    if (Strings.isNullOrEmpty(value)) {
      return defaultValue;
    }
    return value;
  }

  public static int getInt(Properties properties, String name, int defaultValue) {
    String value = properties.getProperty(name);
    if (Strings.isNullOrEmpty(value)) {
      return defaultValue;
    }
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException ignored) {
      throw new IllegalArgumentException("the specified value of '" + name + "' is not a number");
    }
  }

  public static long getLong(Properties properties, String name, long defaultValue) {
    String value = properties.getProperty(name);
    if (Strings.isNullOrEmpty(value)) {
      return defaultValue;
    }
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException ignored) {
      throw new IllegalArgumentException("the specified value of '" + name + "' is not a number");
    }
  }

  public static boolean getBoolean(Properties properties, String name, boolean defaultValue) {
    String value = properties.getProperty(name);
    if (Strings.isNullOrEmpty(value)) {
      return defaultValue;
    }
    if (Boolean.TRUE.toString().equals(value) || Boolean.FALSE.toString().equals(value)) {
      return Boolean.parseBoolean(value);
    } else {
      throw new IllegalArgumentException(
          "the specified value of '" + name + "' is not a boolean value");
    }
  }

  public static String[] getStringArray(Properties properties, String name, String[] defaultValue) {
    String value = properties.getProperty(name);
    if (Strings.isNullOrEmpty(value)) {
      return defaultValue;
    }
    return value.split("\\s*,\\s*");
  }
}
