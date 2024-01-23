package com.scalar.db.util;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.util.concurrent.Uninterruptibles;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

public class ActiveExpiringMapTest {

  @Test
  public void getAndPut_ShouldBehaveCorrectly() {
    // Arrange
    ActiveExpiringMap<String, String> activeExpiringMap =
        new ActiveExpiringMap<>(0, 0, (k, v) -> {});

    // Act Assert
    activeExpiringMap.put("k1", "v1");
    activeExpiringMap.put("k2", "v2");
    activeExpiringMap.put("k3", "v3");

    assertThat(activeExpiringMap.get("k1")).isEqualTo(Optional.of("v1"));
    assertThat(activeExpiringMap.get("k2")).isEqualTo(Optional.of("v2"));
    assertThat(activeExpiringMap.get("k3")).isEqualTo(Optional.of("v3"));
    assertThat(activeExpiringMap.get("k4")).isEqualTo(Optional.empty());
  }

  @Test
  public void putIfAbsent_ShouldBehaveCorrectly() {
    // Arrange
    ActiveExpiringMap<String, String> activeExpiringMap =
        new ActiveExpiringMap<>(0, 0, (k, v) -> {});

    activeExpiringMap.put("k1", "v1");

    // Act
    Optional<String> actual1 = activeExpiringMap.putIfAbsent("k1", "v2");
    Optional<String> actual2 = activeExpiringMap.putIfAbsent("k2", "v2");

    // Assert
    assertThat(actual1).hasValue("v1");
    assertThat(actual2).isEmpty();
    assertThat(activeExpiringMap.get("k1")).isEqualTo(Optional.of("v1"));
    assertThat(activeExpiringMap.get("k2")).isEqualTo(Optional.of("v2"));
  }

  @Test
  public void containsKey_ShouldBehaveCorrectly() {
    // Arrange
    ActiveExpiringMap<String, String> activeExpiringMap =
        new ActiveExpiringMap<>(0, 0, (k, v) -> {});

    activeExpiringMap.put("k1", "v1");
    activeExpiringMap.put("k2", "v2");
    activeExpiringMap.put("k3", "v3");

    // Act Assert
    assertThat(activeExpiringMap.containsKey("k1")).isTrue();
    assertThat(activeExpiringMap.containsKey("k2")).isTrue();
    assertThat(activeExpiringMap.containsKey("k3")).isTrue();
    assertThat(activeExpiringMap.containsKey("k4")).isFalse();
  }

  @Test
  public void containsValue_ShouldBehaveCorrectly() {
    // Arrange
    ActiveExpiringMap<String, String> activeExpiringMap =
        new ActiveExpiringMap<>(0, 0, (k, v) -> {});

    activeExpiringMap.put("k1", "v1");
    activeExpiringMap.put("k2", "v2");
    activeExpiringMap.put("k3", "v2");

    // Act Assert
    assertThat(activeExpiringMap.containsValue("v1")).isTrue();
    assertThat(activeExpiringMap.containsValue("v2")).isTrue();
    assertThat(activeExpiringMap.containsValue("v3")).isFalse();
  }

  @Test
  public void remove_ShouldBehaveCorrectly() {
    // Arrange
    ActiveExpiringMap<String, String> activeExpiringMap =
        new ActiveExpiringMap<>(0, 0, (k, v) -> {});

    activeExpiringMap.put("k1", "v1");
    activeExpiringMap.put("k2", "v2");
    activeExpiringMap.put("k3", "v3");

    // Act
    Optional<String> actual1 = activeExpiringMap.remove("k2");
    Optional<String> actual2 = activeExpiringMap.remove("k4");

    // Assert
    assertThat(actual1).hasValue("v2");
    assertThat(actual2).isEmpty();
    assertThat(activeExpiringMap.get("k1")).hasValue("v1");
    assertThat(activeExpiringMap.get("k2")).isEmpty();
    assertThat(activeExpiringMap.get("k3")).hasValue("v3");
    assertThat(activeExpiringMap.get("k4")).isEmpty();
  }

  @Test
  public void keySet_ShouldBehaveCorrectly() {
    // Arrange
    ActiveExpiringMap<String, String> activeExpiringMap =
        new ActiveExpiringMap<>(0, 0, (k, v) -> {});

    activeExpiringMap.put("k1", "v1");
    activeExpiringMap.put("k2", "v2");
    activeExpiringMap.put("k3", "v3");

    // Act Assert
    Set<String> keySet = activeExpiringMap.keySet();
    assertThat(keySet).contains("k1", "k2", "k3");

    keySet.remove("k2");
    assertThat(keySet).contains("k1", "k3");
    assertThat(activeExpiringMap.keySet()).contains("k1", "k3");
  }

  @Test
  public void values_ShouldBehaveCorrectly() {
    // Arrange
    ActiveExpiringMap<String, String> activeExpiringMap =
        new ActiveExpiringMap<>(0, 0, (k, v) -> {});

    activeExpiringMap.put("k1", "v1");
    activeExpiringMap.put("k2", "v2");
    activeExpiringMap.put("k3", "v3");
    activeExpiringMap.put("k4", "v4");

    // Act Assert
    Collection<String> values = activeExpiringMap.values();
    assertThat(values).containsExactly("v1", "v2", "v3", "v4");

    values.remove("v3");
    assertThat(values).containsExactly("v1", "v2", "v4");
    assertThat(activeExpiringMap.values()).containsExactly("v1", "v2", "v4");
    assertThat(activeExpiringMap.get("k1")).isEqualTo(Optional.of("v1"));
    assertThat(activeExpiringMap.get("k2")).isEqualTo(Optional.of("v2"));
    assertThat(activeExpiringMap.get("k3")).isEqualTo(Optional.empty());
    assertThat(activeExpiringMap.get("k4")).isEqualTo(Optional.of("v4"));
  }

  @Test
  public void entrySet_ShouldBehaveCorrectly() {
    // Arrange
    ActiveExpiringMap<String, String> activeExpiringMap =
        new ActiveExpiringMap<>(0, 0, (k, v) -> {});

    activeExpiringMap.put("k1", "v1");
    activeExpiringMap.put("k2", "v2");
    activeExpiringMap.put("k3", "v3");
    activeExpiringMap.put("k4", "v4");

    // Act Assert
    Set<Map.Entry<String, String>> entries = activeExpiringMap.entrySet();
    assertThat(entries)
        .containsOnly(
            new AbstractMap.SimpleEntry<>("k1", "v1"),
            new AbstractMap.SimpleEntry<>("k2", "v2"),
            new AbstractMap.SimpleEntry<>("k3", "v3"),
            new AbstractMap.SimpleEntry<>("k4", "v4"));

    entries.remove(new AbstractMap.SimpleEntry<>("k3", "v3"));
    assertThat(entries)
        .containsOnly(
            new AbstractMap.SimpleEntry<>("k1", "v1"),
            new AbstractMap.SimpleEntry<>("k2", "v2"),
            new AbstractMap.SimpleEntry<>("k4", "v4"));
    assertThat(activeExpiringMap.entrySet())
        .containsOnly(
            new AbstractMap.SimpleEntry<>("k1", "v1"),
            new AbstractMap.SimpleEntry<>("k2", "v2"),
            new AbstractMap.SimpleEntry<>("k4", "v4"));
  }

  @Test
  public void updateExpirationTime_ShouldBehaveCorrectly() {
    // Arrange
    ActiveExpiringMap<String, String> activeExpiringMap =
        new ActiveExpiringMap<>(0, 0, (k, v) -> {});

    activeExpiringMap.put("k1", "v1");

    // Act
    ActiveExpiringMap<String, String>.ValueHolder valueBeforeUpdate =
        activeExpiringMap.getValueHolder("k1");
    assertThat(valueBeforeUpdate).isNotNull();
    long lastUpdateTimeBeforeUpdate = valueBeforeUpdate.getLastUpdateTime();

    Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);

    activeExpiringMap.updateExpirationTime("k1");

    ActiveExpiringMap<String, String>.ValueHolder valueAfterUpdate =
        activeExpiringMap.getValueHolder("k1");
    assertThat(valueAfterUpdate).isNotNull();
    long lastUpdateTimeAfterUpdate = valueAfterUpdate.getLastUpdateTime();

    // Assert
    assertThat(lastUpdateTimeAfterUpdate).isGreaterThan(lastUpdateTimeBeforeUpdate);
  }

  @Test
  public void computeIfAbsent_ShouldBehaveCorrectly() {
    // Arrange
    ActiveExpiringMap<String, String> activeExpiringMap =
        new ActiveExpiringMap<>(0, 0, (k, v) -> {});

    // Act
    Optional<String> actual1 = activeExpiringMap.computeIfAbsent("k1", k -> "v1");
    Optional<String> actual2 = activeExpiringMap.computeIfAbsent("k1", k -> "v2");

    // Assert
    assertThat(actual1).hasValue("v1");
    assertThat(actual2).hasValue("v1");
    assertThat(activeExpiringMap.get("k1")).hasValue("v1");
  }

  @Test
  public void computeIfPresent_ShouldBehaveCorrectly() {
    // Arrange
    ActiveExpiringMap<String, String> activeExpiringMap =
        new ActiveExpiringMap<>(0, 0, (k, v) -> {});

    activeExpiringMap.put("k1", "v1");

    // Act
    Optional<String> actual1 = activeExpiringMap.computeIfPresent("k1", (k, v) -> "v2");
    Optional<String> actual2 = activeExpiringMap.computeIfPresent("k2", (k, v) -> "v3");

    // Assert
    assertThat(actual1).hasValue("v2");
    assertThat(actual2).isEmpty();
    assertThat(activeExpiringMap.get("k1")).hasValue("v2");
    assertThat(activeExpiringMap.get("k2")).isEmpty();
  }
}
