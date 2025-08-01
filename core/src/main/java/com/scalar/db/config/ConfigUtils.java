package com.scalar.db.config;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.scalar.db.common.CoreError;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Properties;
import javax.annotation.Nullable;
import org.apache.commons.text.StringSubstitutor;
import org.apache.commons.text.lookup.StringLookupFactory;

/**
 * A utility class for retrieving configuration values from {@link Properties}. You can use
 * placeholders in the values, and they are replaced with environment variables
 * (${env:&#60;environment variable name&#62;}) or system properties (${sys:&#60;system property
 * name&#62;}). You can also specify default values in placeholders like ${sys:&#60;system property
 * name&#62;:-defaultValue}.
 */
public final class ConfigUtils {

  /**
   * A {@link StringSubstitutor} instance with the environment variable string lookup and system
   * property string lookup.
   */
  private static final StringSubstitutor stringSubstitutor =
      new StringSubstitutor(
          StringLookupFactory.INSTANCE.interpolatorStringLookup(
              ImmutableMap.of(
                  StringLookupFactory.KEY_ENV,
                  StringLookupFactory.INSTANCE.environmentVariableStringLookup(),
                  StringLookupFactory.KEY_SYS,
                  StringLookupFactory.INSTANCE.systemPropertyStringLookup()),
              StringLookupFactory.INSTANCE.nullStringLookup(),
              false));

  private ConfigUtils() {}

  @Nullable
  public static String getString(
      Properties properties, String name, @Nullable String defaultValue) {
    String value = trimAndReplace(properties.getProperty(name));
    if (Strings.isNullOrEmpty(value)) {
      return defaultValue;
    }
    return value;
  }

  public static int getInt(Properties properties, String name, int defaultValue) {
    String value = trimAndReplace(properties.getProperty(name));
    if (Strings.isNullOrEmpty(value)) {
      return defaultValue;
    }
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException ignored) {
      throw new IllegalArgumentException(
          CoreError.CONFIG_UTILS_INVALID_NUMBER_FORMAT.buildMessage(name, value));
    }
  }

  @Nullable
  public static Integer getInt(Properties properties, String name, @Nullable Integer defaultValue) {
    String value = trimAndReplace(properties.getProperty(name));
    if (Strings.isNullOrEmpty(value)) {
      return defaultValue;
    }
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException ignored) {
      throw new IllegalArgumentException(
          CoreError.CONFIG_UTILS_INVALID_NUMBER_FORMAT.buildMessage(name, value));
    }
  }

  public static long getLong(Properties properties, String name, long defaultValue) {
    String value = trimAndReplace(properties.getProperty(name));
    if (Strings.isNullOrEmpty(value)) {
      return defaultValue;
    }
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException ignored) {
      throw new IllegalArgumentException(
          CoreError.CONFIG_UTILS_INVALID_NUMBER_FORMAT.buildMessage(name, value));
    }
  }

  @Nullable
  public static Long getLong(Properties properties, String name, @Nullable Long defaultValue) {
    String value = trimAndReplace(properties.getProperty(name));
    if (Strings.isNullOrEmpty(value)) {
      return defaultValue;
    }
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException ignored) {
      throw new IllegalArgumentException(
          CoreError.CONFIG_UTILS_INVALID_NUMBER_FORMAT.buildMessage(name, value));
    }
  }

  public static boolean getBoolean(Properties properties, String name, boolean defaultValue) {
    String value = trimAndReplace(properties.getProperty(name));
    if (Strings.isNullOrEmpty(value)) {
      return defaultValue;
    }
    if (Boolean.TRUE.toString().equalsIgnoreCase(value)
        || Boolean.FALSE.toString().equalsIgnoreCase(value)) {
      return Boolean.parseBoolean(value);
    } else {
      throw new IllegalArgumentException(
          CoreError.CONFIG_UTILS_INVALID_BOOLEAN_FORMAT.buildMessage(name, value));
    }
  }

  @Nullable
  public static Boolean getBoolean(
      Properties properties, String name, @Nullable Boolean defaultValue) {
    String value = trimAndReplace(properties.getProperty(name));
    if (Strings.isNullOrEmpty(value)) {
      return defaultValue;
    }
    if (Boolean.TRUE.toString().equalsIgnoreCase(value)
        || Boolean.FALSE.toString().equalsIgnoreCase(value)) {
      return Boolean.parseBoolean(value);
    } else {
      throw new IllegalArgumentException(
          CoreError.CONFIG_UTILS_INVALID_BOOLEAN_FORMAT.buildMessage(name, value));
    }
  }

  @Nullable
  public static String[] getStringArray(
      Properties properties, String name, @Nullable String[] defaultValue) {
    String value = trimAndReplace(properties.getProperty(name));
    if (Strings.isNullOrEmpty(value)) {
      return defaultValue;
    }
    // Prevent escaped commas from being used as delimiters for contact points.
    return Arrays.stream(value.split("\\s*(?<!\\\\),\\s*"))
        .map(s -> s.replace("\\", ""))
        .toArray(String[]::new);
  }

  @Nullable
  public static String getStringFromFilePath(
      Properties properties, String pathName, @Nullable String defaultValue) {
    String path = trimAndReplace(properties.getProperty(pathName));
    if (Strings.isNullOrEmpty(path)) {
      return defaultValue;
    }
    try {
      return new String(Files.readAllBytes(new File(path).toPath()), StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new UncheckedIOException(
          CoreError.CONFIG_UTILS_READING_FILE_FAILED.buildMessage(path), e);
    }
  }

  @VisibleForTesting
  static String trimAndReplace(@Nullable String value) {
    if (value == null) {
      return null;
    }
    return stringSubstitutor.replace(value.trim());
  }
}
