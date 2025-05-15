package com.scalar.db.dataloader.cli.command.dataexport;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.scalar.db.api.Scan;
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
    Assertions.assertEquals("Invalid column order format: id ASC", thrown.getMessage());
  }

  @Test
  void callConvert_withValidValueAndOrderAscending_shouldReturnScanOrdering() {
    String value = "id=ASC,age=DESC";
    List<Scan.Ordering> expectedOrder = new ArrayList<>();
    expectedOrder.add(new Scan.Ordering("id", Scan.Ordering.Order.ASC));
    expectedOrder.add(new Scan.Ordering("age", Scan.Ordering.Order.DESC));
    Assertions.assertEquals(expectedOrder, scanOrderingConverter.convert(value));
  }

  @Test
  void callConvert_withValidValueAndOrderDescending_shouldReturnScanOrdering() {
    String value = "id=desc";
    List<Scan.Ordering> expectedOrder =
        Collections.singletonList(new Scan.Ordering("id", Scan.Ordering.Order.DESC));
    Assertions.assertEquals(expectedOrder, scanOrderingConverter.convert(value));
  }
}
