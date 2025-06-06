package com.scalar.db.dataloader.cli.command.dataexport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.scalar.db.dataloader.core.ColumnKeyValue;
import org.junit.jupiter.api.Test;

class SingleColumnKeyValueConverterTest {

  private final SingleColumnKeyValueConverter converter = new SingleColumnKeyValueConverter();

  @Test
  void convert_ValidInput_ReturnsColumnKeyValue() {
    String input = "name=John Doe";
    ColumnKeyValue expected = new ColumnKeyValue("name", "John Doe");
    ColumnKeyValue result = converter.convert(input);
    assertEquals(expected.getColumnName(), result.getColumnName());
    assertEquals(expected.getColumnValue(), result.getColumnValue());
  }

  @Test
  void convert_ValidInputWithExtraSpaces_ReturnsColumnKeyValue() {
    String input = "  age  =  25  ";
    ColumnKeyValue expected = new ColumnKeyValue("age", "25");
    ColumnKeyValue result = converter.convert(input);
    assertEquals(expected.getColumnName(), result.getColumnName());
    assertEquals(expected.getColumnValue(), result.getColumnValue());
  }

  @Test
  void convert_InvalidInputMissingValue_ThrowsIllegalArgumentException() {
    String input = "name=";
    assertThrows(IllegalArgumentException.class, () -> converter.convert(input));
  }

  @Test
  void convert_InvalidInputMissingKey_ThrowsIllegalArgumentException() {
    String input = "=John Doe";
    assertThrows(IllegalArgumentException.class, () -> converter.convert(input));
  }

  @Test
  void convert_InvalidInputMissingEquals_ThrowsIllegalArgumentException() {
    String input = "nameJohn Doe";
    assertThrows(IllegalArgumentException.class, () -> converter.convert(input));
  }

  @Test
  void convert_ValidInputMultipleEquals_Returns() {
    String input = "name=John=Doe";
    ColumnKeyValue expected = new ColumnKeyValue("name", "John=Doe");
    ColumnKeyValue result = converter.convert(input);
    assertEquals(expected.getColumnName(), result.getColumnName());
    assertEquals(expected.getColumnValue(), result.getColumnValue());
  }

  @Test
  void convert_NullValue_ThrowsIllegalArgumentException() {
    assertThrows(IllegalArgumentException.class, () -> converter.convert(null));
  }
}
