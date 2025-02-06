package com.scalar.db.dataloader.cli.command.dataexport;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.scalar.db.dataloader.core.ColumnKeyValue;
import java.util.Collections;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ColumnKeyValueConverterTest {

  ColumnKeyValueConverter columnKeyValueConverter = new ColumnKeyValueConverter();

  @Test
  public void convert_withInvalidValue_ShouldThrowError() {
    String value = "id 15";
    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> columnKeyValueConverter.convert(value),
            "Expected to throw exception");
    Assertions.assertEquals("Invalid key format: id 15", thrown.getMessage());
  }

  @Test
  public void convert_withValidValue_ShouldReturnColumnKeyValue() {
    String value = "id=15";
    ColumnKeyValue expectedOrder = new ColumnKeyValue("id", "15");
    Assertions.assertEquals(
        Collections.singletonList(expectedOrder).get(0).getColumnName(),
        columnKeyValueConverter.convert(value).get(0).getColumnName());
    Assertions.assertEquals(
        Collections.singletonList(expectedOrder).get(0).getColumnValue(),
        columnKeyValueConverter.convert(value).get(0).getColumnValue());
  }
}
