package com.scalar.db.storage.cosmos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class CosmosAdminLargePartitionKeyEnabledTest {

  @ParameterizedTest
  @CsvSource({
    "nullOptions, true",
    "emptyOptions, true",
    "explicitTrue, true",
    "explicitTrueUpperCase, true",
    "explicitFalse, false",
    "missingKeyWithOtherOptions, true"
  })
  void isLargePartitionKeyEnabled_ShouldResolveOptions(String scenario, boolean expected) {
    switch (scenario) {
      case "nullOptions":
        assertThat(CosmosAdmin.isLargePartitionKeyEnabled(null)).isEqualTo(expected);
        break;
      case "emptyOptions":
        assertThat(CosmosAdmin.isLargePartitionKeyEnabled(Collections.emptyMap()))
            .isEqualTo(expected);
        break;
      case "explicitTrue":
        assertThat(
                CosmosAdmin.isLargePartitionKeyEnabled(
                    ImmutableMap.of(CosmosAdmin.LARGE_PARTITION_KEY, "true")))
            .isEqualTo(expected);
        break;
      case "explicitTrueUpperCase":
        assertThat(
                CosmosAdmin.isLargePartitionKeyEnabled(
                    ImmutableMap.of(CosmosAdmin.LARGE_PARTITION_KEY, "TRUE")))
            .isEqualTo(expected);
        break;
      case "explicitFalse":
        assertThat(
                CosmosAdmin.isLargePartitionKeyEnabled(
                    ImmutableMap.of(CosmosAdmin.LARGE_PARTITION_KEY, "false")))
            .isEqualTo(expected);
        break;
      case "missingKeyWithOtherOptions":
        assertThat(
                CosmosAdmin.isLargePartitionKeyEnabled(
                    ImmutableMap.of(CosmosAdmin.REQUEST_UNIT, "400")))
            .isEqualTo(expected);
        break;
      default:
        throw new IllegalArgumentException("Unknown scenario: " + scenario);
    }
  }

  @Test
  void isLargePartitionKeyEnabled_WhenValueIsMalformed_ShouldThrowIllegalArgumentException() {
    assertThatThrownBy(
            () ->
                CosmosAdmin.isLargePartitionKeyEnabled(
                    ImmutableMap.of(CosmosAdmin.LARGE_PARTITION_KEY, "maybe")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(CosmosAdmin.LARGE_PARTITION_KEY)
        .hasMessageContaining("maybe");
    assertThatThrownBy(
            () ->
                CosmosAdmin.isLargePartitionKeyEnabled(
                    ImmutableMap.of(CosmosAdmin.LARGE_PARTITION_KEY, "treu")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("treu");
  }
}
