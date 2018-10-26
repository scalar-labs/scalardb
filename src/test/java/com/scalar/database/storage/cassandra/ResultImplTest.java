package com.scalar.database.storage.cassandra;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TableMetadata;
import com.scalar.database.io.IntValue;
import com.scalar.database.io.Key;
import com.scalar.database.io.TextValue;
import com.scalar.database.io.Value;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** */
public class ResultImplTest {
  private static final String ANY_NAME = "name1";
  private static final String ANY_TEXT = "text1";
  private static final String ANY_COLUMN_NAME_1 = "col1";
  private static final String ANY_COLUMN_NAME_2 = "col2";
  private static final String ANY_COLUMN_NAME_3 = "col3";
  private static final String ANY_COLUMN_NAME_4 = "col4";
  private static final String ANY_COLUMN_NAME_5 = "col5";
  private static final String ANY_COLUMN_NAME_6 = "col6";
  private static final String ANY_COLUMN_NAME_7 = "col7";
  private static final int ANY_INT = 10;
  private Definitions definitions;
  @Mock private Row row;
  @Mock private TableMetadata tableMetadata;
  @Mock private ColumnMetadata columnMetadata;

  @Before
  public void setUp() throws Exception {
    definitions =
        new Definitions()
            .add(ANY_COLUMN_NAME_1, DataType.cboolean())
            .add(ANY_COLUMN_NAME_2, DataType.cint())
            .add(ANY_COLUMN_NAME_3, DataType.bigint())
            .add(ANY_COLUMN_NAME_4, DataType.cfloat())
            .add(ANY_COLUMN_NAME_5, DataType.cdouble())
            .add(ANY_COLUMN_NAME_6, DataType.text())
            .add(ANY_COLUMN_NAME_7, DataType.blob());

    MockitoAnnotations.initMocks(this);
    when(row.getBool(ANY_COLUMN_NAME_1)).thenReturn(true);
    when(row.getInt(ANY_COLUMN_NAME_2)).thenReturn(Integer.MAX_VALUE);
    when(row.getLong(ANY_COLUMN_NAME_3)).thenReturn(Long.MAX_VALUE);
    when(row.getFloat(ANY_COLUMN_NAME_4)).thenReturn(Float.MAX_VALUE);
    when(row.getDouble(ANY_COLUMN_NAME_5)).thenReturn(Double.MAX_VALUE);
    when(row.getString(ANY_COLUMN_NAME_6)).thenReturn("string");
    when(row.getBytes(ANY_COLUMN_NAME_7))
        .thenReturn((ByteBuffer) ByteBuffer.allocate(64).put("string".getBytes()).flip());
  }

  private List<Value> prepareValues() {
    return Arrays.asList(new IntValue("k1", 1), new TextValue("k2", "2"));
  }

  @Test
  public void getValue_ProperValuesGivenInConstructor_ShouldReturnWhatsSet() {
    // Arrange
    String expectedText = ANY_NAME;
    int expectedInt = ANY_INT;
    definitions.add(expectedText, DataType.cint());
    when(row.getInt(expectedText)).thenReturn(expectedInt);
    ResultImpl spy = spy(new ResultImpl(new ArrayList<>(), null));
    doReturn(definitions.get()).when(spy).getColumnDefinitions(row);
    spy.interpret(row);

    // Act
    Optional<Value> actual = spy.getValue(expectedText);

    // Assert
    assertThat(actual).isEqualTo(Optional.of(new IntValue(expectedText, expectedInt)));
  }

  @Test
  public void getValues_ProperValuesGivenInConstructor_ShouldReturnWhatsSet() {
    // Arrange
    String expectedText = ANY_NAME;
    int expectedInt = ANY_INT;
    definitions.add(expectedText, DataType.cint()).get();
    when(row.getInt(expectedText)).thenReturn(expectedInt);
    ResultImpl spy = spy(new ResultImpl(new ArrayList<>(), null));
    doReturn(definitions.get()).when(spy).getColumnDefinitions(row);
    spy.interpret(row);

    // Act
    Map<String, Value> actual = spy.getValues();

    // Assert
    assertThat(actual.get(expectedText)).isEqualTo(new IntValue(expectedText, expectedInt));
  }

  @Test
  public void getValue_getValueCalledBefore_ShouldNotLoadAgain() {
    // Arrange
    ResultImpl spy = spy(new ResultImpl(new ArrayList<>(), null));
    doReturn(definitions.get()).when(spy).getColumnDefinitions(row);
    spy.interpret(row);
    spy.getValue("a");

    // Act
    spy.getValue("a");

    // Assert
    verify(spy).interpret(row);
  }

  @Test
  public void getValues_TryToModifyReturned_ShouldThrowException() {
    // Arrange
    ResultImpl spy = spy(new ResultImpl(new ArrayList<>(), null));
    doReturn(definitions.get()).when(spy).getColumnDefinitions(row);
    spy.interpret(row);
    Map<String, Value> values = spy.getValues();

    // Act Assert
    assertThatThrownBy(
            () -> {
              values.put("new", new TextValue(ANY_NAME, ANY_TEXT));
            })
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  public void equals_DifferentObjectsSameValuesGiven_ShouldReturnTrue() {
    // Arrange
    ResultImpl r1 = new ResultImpl(prepareValues(), null);
    ResultImpl r2 = new ResultImpl(prepareValues(), null);

    // Act Assert
    assertThat(r1.equals(r2)).isTrue();
  }

  @Test
  public void equals_DifferentObjectsDifferentValuesGiven_ShouldReturnFalse() {
    // Arrange
    ResultImpl r1 = new ResultImpl(Collections.singletonList(new IntValue("k1", 1)), null);
    ResultImpl r2 = new ResultImpl(Collections.singletonList(new IntValue("k2", 2)), null);

    // Act Assert
    assertThat(r1.equals(r2)).isFalse();
  }

  @Test
  public void constructor_NullGiven_ShouldThrowNullPointerException() {
    // Act Assert
    assertThatThrownBy(
            () -> {
              new ResultImpl((Row) null, null);
            })
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void getPartitionKey_RequiredValuesGiven_ShouldReturnPartitionKey() {
    // Arrange
    ResultImpl spy = spy(new ResultImpl(new ArrayList<>(), tableMetadata));
    when(columnMetadata.getName()).thenReturn(ANY_COLUMN_NAME_2);
    when(tableMetadata.getPartitionKey()).thenReturn(Arrays.asList(columnMetadata));
    doReturn(definitions.get()).when(spy).getColumnDefinitions(row);
    spy.interpret(row);

    // Act
    Optional<Key> key = spy.getPartitionKey();

    // Assert
    assertThat(key.get().get().size()).isEqualTo(1);
    assertThat(key.get().get().get(0))
        .isEqualTo(new IntValue(ANY_COLUMN_NAME_2, Integer.MAX_VALUE));
  }

  @Test
  public void getPartitionKey_RequiredValuesNotGiven_ShouldReturnEmpty() {
    // Arrange
    ResultImpl spy = spy(new ResultImpl(new ArrayList<>(), tableMetadata));
    when(columnMetadata.getName()).thenReturn("another");
    when(tableMetadata.getPartitionKey()).thenReturn(Arrays.asList(columnMetadata));
    doReturn(definitions.get()).when(spy).getColumnDefinitions(row);
    spy.interpret(row);

    // Act
    Optional<Key> key = spy.getPartitionKey();

    // Assert
    assertThat(key.isPresent()).isFalse();
  }

  @Test
  public void getClusteringKey_RequiredValuesGiven_ShouldReturnClusteringKey() {
    // Arrange
    ResultImpl spy = spy(new ResultImpl(new ArrayList<>(), tableMetadata));
    when(columnMetadata.getName()).thenReturn(ANY_COLUMN_NAME_2);
    when(tableMetadata.getClusteringColumns())
        .thenReturn(Collections.singletonList(columnMetadata));
    doReturn(definitions.get()).when(spy).getColumnDefinitions(row);
    spy.interpret(row);

    // Act
    Optional<Key> key = spy.getClusteringKey();

    // Assert
    assertThat(key.get().get().size()).isEqualTo(1);
    assertThat(key.get().get().get(0))
        .isEqualTo(new IntValue(ANY_COLUMN_NAME_2, Integer.MAX_VALUE));
  }

  @Test
  public void getClusteringKey_RequiredValuesNotGiven_ShouldReturnEmpty() {
    // Arrange
    ResultImpl spy = spy(new ResultImpl(new ArrayList<>(), tableMetadata));
    when(columnMetadata.getName()).thenReturn("another");
    when(tableMetadata.getClusteringColumns())
        .thenReturn(Collections.singletonList(columnMetadata));
    doReturn(definitions.get()).when(spy).getColumnDefinitions(row);
    spy.interpret(row);

    // Act
    Optional<Key> key = spy.getClusteringKey();

    // Assert
    assertThat(key.isPresent()).isFalse();
  }

  private class Definitions {
    Map<String, DataType> definitions;

    public Definitions() {
      definitions = new HashMap<>();
    }

    public Definitions add(String name, DataType type) {
      definitions.put(name, type);
      return this;
    }

    public Map<String, DataType> get() {
      return Collections.unmodifiableMap(definitions);
    }
  }
}
