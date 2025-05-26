package com.scalar.db.dataloader.cli.command.dataexport;

import static org.junit.jupiter.api.Assertions.assertThrows;

import com.scalar.db.common.error.CoreError;
import com.scalar.db.dataloader.core.ColumnKeyValue;
import java.util.Collections;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class MultiColumnKeyValueConverterTest {

  MultiColumnKeyValueConverter multiColumnKeyValueConverter = new MultiColumnKeyValueConverter();

  @Test
  void convert_withInvalidValue_ShouldThrowError() {
    String value = "id 15";
    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () -> multiColumnKeyValueConverter.convert(value),
            "Expected to throw exception");
    Assertions.assertEquals(
        CoreError.DATA_LOADER_INVALID_KEY_VALUE_INPUT.buildMessage("id 15"), thrown.getMessage());
  }

  @Test
  void convert_withValidValue_ShouldReturnColumnKeyValue() {
    String value = "id=15";
    ColumnKeyValue expectedOrder = new ColumnKeyValue("id", "15");
    Assertions.assertEquals(
        Collections.singletonList(expectedOrder).get(0).getColumnName(),
        multiColumnKeyValueConverter.convert(value).get(0).getColumnName());
    Assertions.assertEquals(
        Collections.singletonList(expectedOrder).get(0).getColumnValue(),
        multiColumnKeyValueConverter.convert(value).get(0).getColumnValue());
  }
}
