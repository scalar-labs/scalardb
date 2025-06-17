package com.scalar.db.dataloader.cli.command.dataexport;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.scalar.db.api.Scan;
import com.scalar.db.common.error.CoreError;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ScanOrderingConverterTest {
  ScanOrderingConverter scanOrderingConverter = new ScanOrderingConverter();

  @Test
  void callConvert_withInvalidValue_shouldThrowException() {
    String value = "id ASC";
    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> scanOrderingConverter.convert(value),
            "Expected to throw exception");
    Assertions.assertEquals(
        CoreError.DATA_LOADER_INVALID_KEY_VALUE_INPUT.buildMessage(value), thrown.getMessage());
  }

  @Test
  void callConvert_withValidValueAndOrderAscending_shouldReturnScanOrdering() {
    String value = "id=ASC,age=DESC";
    List<Scan.Ordering> expectedOrder = new ArrayList<>();
    expectedOrder.add(Scan.Ordering.asc("id"));
    expectedOrder.add(Scan.Ordering.desc("age"));
    Assertions.assertEquals(expectedOrder, scanOrderingConverter.convert(value));
  }

  @Test
  void callConvert_withValidValueAndOrderDescending_shouldReturnScanOrdering() {
    String value = "id=desc";
    List<Scan.Ordering> expectedOrder = Collections.singletonList(Scan.Ordering.desc("id"));
    Assertions.assertEquals(expectedOrder, scanOrderingConverter.convert(value));
  }
}
