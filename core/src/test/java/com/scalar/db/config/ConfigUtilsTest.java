package com.scalar.db.config;

import static com.github.stefanbirkner.systemlambda.SystemLambda.withEnvironmentVariable;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class ConfigUtilsTest {

  @TempDir private Path folder;

  private void writeToFile(File file, String content) throws IOException {
    BufferedWriter writer = Files.newBufferedWriter(file.toPath(), StandardCharsets.UTF_8);
    writer.write(content);
    writer.close();
  }

  @Test
  public void getString_ShouldBehaveCorrectly() {
    // Arrange
    Properties properties = new Properties();
    properties.setProperty("name1", "value");
    properties.setProperty("name2", "   value     ");
    properties.setProperty("name3", "");

    // Act Assert
    assertThat(ConfigUtils.getString(properties, "name1", "def_value")).isEqualTo("value");
    assertThat(ConfigUtils.getString(properties, "name2", "def_value")).isEqualTo("value");
    assertThat(ConfigUtils.getString(properties, "name3", "def_value")).isEqualTo("def_value");
    assertThat(ConfigUtils.getString(properties, "name4", "def_value")).isEqualTo("def_value");
    assertThat(ConfigUtils.getString(properties, "name5", null)).isNull();
  }

  @Test
  public void getInt_ShouldBehaveCorrectly() {
    // Arrange
    Properties properties = new Properties();
    properties.setProperty("name1", "100");
    properties.setProperty("name2", "   100     ");
    properties.setProperty("name3", "");
    properties.setProperty("name5", "aaa");

    // Act Assert
    assertThat(ConfigUtils.getInt(properties, "name1", 10000)).isEqualTo(100);
    assertThat(ConfigUtils.getInt(properties, "name2", 10000)).isEqualTo(100);
    assertThat(ConfigUtils.getInt(properties, "name3", 10000)).isEqualTo(10000);
    assertThat(ConfigUtils.getInt(properties, "name4", 10000)).isEqualTo(10000);
    assertThatThrownBy(() -> ConfigUtils.getInt(properties, "name5", 10000))
        .isInstanceOf(IllegalArgumentException.class);
    assertThat(ConfigUtils.getInt(properties, "name6", null)).isNull();
  }

  @Test
  public void getLong_ShouldBehaveCorrectly() {
    // Arrange
    Properties properties = new Properties();
    properties.setProperty("name1", "100");
    properties.setProperty("name2", "   100     ");
    properties.setProperty("name3", "");
    properties.setProperty("name5", "aaa");

    // Act Assert
    assertThat(ConfigUtils.getLong(properties, "name1", 10000)).isEqualTo(100);
    assertThat(ConfigUtils.getLong(properties, "name2", 10000)).isEqualTo(100);
    assertThat(ConfigUtils.getLong(properties, "name3", 10000)).isEqualTo(10000);
    assertThat(ConfigUtils.getLong(properties, "name4", 10000)).isEqualTo(10000);
    assertThatThrownBy(() -> ConfigUtils.getLong(properties, "name5", 10000))
        .isInstanceOf(IllegalArgumentException.class);
    assertThat(ConfigUtils.getLong(properties, "name6", null)).isNull();
  }

  @Test
  public void getBoolean_ShouldBehaveCorrectly() {
    // Arrange
    Properties properties = new Properties();
    properties.setProperty("name1", "true");
    properties.setProperty("name2", "   true       ");
    properties.setProperty("name3", "TRUE");
    properties.setProperty("name4", "");
    properties.setProperty("name6", "aaa");

    // Act Assert
    assertThat(ConfigUtils.getBoolean(properties, "name1", false)).isEqualTo(true);
    assertThat(ConfigUtils.getBoolean(properties, "name2", false)).isEqualTo(true);
    assertThat(ConfigUtils.getBoolean(properties, "name3", false)).isEqualTo(true);
    assertThat(ConfigUtils.getBoolean(properties, "name4", false)).isEqualTo(false);
    assertThat(ConfigUtils.getBoolean(properties, "name5", false)).isEqualTo(false);
    assertThatThrownBy(() -> ConfigUtils.getBoolean(properties, "name6", false))
        .isInstanceOf(IllegalArgumentException.class);
    assertThat(ConfigUtils.getBoolean(properties, "name7", null)).isNull();
  }

  @Test
  public void getStringArray_ShouldBehaveCorrectly() {
    // Arrange
    Properties properties = new Properties();
    properties.setProperty("name1", "aaa,bbb,ccc");
    properties.setProperty("name2", "   aaa  ,  bbb,  ccc   ");
    properties.setProperty("name3", "aaa,bbb\\,ccc");
    properties.setProperty("name4", "");

    // Act Assert
    assertThat(ConfigUtils.getStringArray(properties, "name1", new String[] {"xxx", "yyy", "zzz"}))
        .isEqualTo(new String[] {"aaa", "bbb", "ccc"});
    assertThat(ConfigUtils.getStringArray(properties, "name2", new String[] {"xxx", "yyy", "zzz"}))
        .isEqualTo(new String[] {"aaa", "bbb", "ccc"});
    assertThat(ConfigUtils.getStringArray(properties, "name3", new String[] {"xxx", "yyy", "zzz"}))
        .isEqualTo(new String[] {"aaa", "bbb,ccc"});
    assertThat(ConfigUtils.getStringArray(properties, "name4", new String[] {"xxx", "yyy", "zzz"}))
        .isEqualTo(new String[] {"xxx", "yyy", "zzz"});
    assertThat(ConfigUtils.getStringArray(properties, "name5", new String[] {"xxx", "yyy", "zzz"}))
        .isEqualTo(new String[] {"xxx", "yyy", "zzz"});
    assertThat(ConfigUtils.getBoolean(properties, "name6", null)).isNull();
  }

  @Test
  public void getStringFromFilePath_ShouldBehaveCorrectly() throws IOException {
    // Arrange
    File filePath = folder.resolve("some_file").toFile();
    writeToFile(filePath.getCanonicalFile(), "value");
    Properties properties = new Properties();
    properties.setProperty("name1", filePath.getCanonicalPath());

    // Act Assert
    assertThat(ConfigUtils.getStringFromFilePath(properties, "name1", null)).isEqualTo("value");
  }

  @Test
  public void trimAndReplace_ShouldBehaveCorrectly() throws Exception {
    // Arrange
    System.setProperty("aaa", "system prop1");
    System.setProperty("bbb", "system prop2");

    // Act Assert
    withEnvironmentVariable("AAA", "env var1")
        .and("BBB", "env var2")
        .execute(
            () -> {
              assertThat(ConfigUtils.trimAndReplace("${env:AAA}")).isEqualTo("env var1");
              assertThat(ConfigUtils.trimAndReplace(" ${env:AAA}    ")).isEqualTo("env var1");
              assertThat(ConfigUtils.trimAndReplace("${env:AAA} value"))
                  .isEqualTo("env var1 value");
              assertThat(ConfigUtils.trimAndReplace("${env:AAA} ${env:BBB}"))
                  .isEqualTo("env var1 env var2");
              assertThat(ConfigUtils.trimAndReplace("${env:CCC}")).isEqualTo("${env:CCC}");
              assertThat(ConfigUtils.trimAndReplace("${env:CCC:-aaa}")).isEqualTo("aaa");
            });

    assertThat(ConfigUtils.trimAndReplace("${sys:aaa}")).isEqualTo("system prop1");
    assertThat(ConfigUtils.trimAndReplace(" ${sys:aaa}    ")).isEqualTo("system prop1");
    assertThat(ConfigUtils.trimAndReplace("${sys:aaa} value")).isEqualTo("system prop1 value");
    assertThat(ConfigUtils.trimAndReplace("${sys:aaa} ${sys:bbb}"))
        .isEqualTo("system prop1 system prop2");
    assertThat(ConfigUtils.trimAndReplace("${sys:ccc}")).isEqualTo("${sys:ccc}");
    assertThat(ConfigUtils.trimAndReplace("${sys:ccc:-aaa}")).isEqualTo("aaa");
  }
}
