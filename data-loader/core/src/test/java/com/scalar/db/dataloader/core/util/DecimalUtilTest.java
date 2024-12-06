package com.scalar.db.dataloader.core.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class DecimalUtilTest {
  @Test
  void convertToNonScientific_withValidDoubleValue_shouldReturnProperStringValue() {
    String expectedValue = "340.55";
    Double value = 340.55;
    Assertions.assertEquals(expectedValue, DecimalUtil.convertToNonScientific(value));
  }

  @Test
  void convertToNonScientific_withValidFloatValue_shouldReturnProperStringValue() {
    String expectedValue = "356";
    Float value = 356F;
    Assertions.assertEquals(expectedValue, DecimalUtil.convertToNonScientific(value));
  }
}
