package com.scalar.db.schemaloader;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;

public class SchemaLoaderTest {

  private AutoCloseable closeable;
  private MockedStatic<SchemaLoader> schemaLoaderMockedStatic;

  @Mock private SchemaOperator operator;
  @Mock private SchemaParser parser;

  @Mock private Path configFilePath;
  @Mock private Properties configProperties;
  @Mock private Path schemaFilePath;
  @Mock private Path serializedSchemaJson;
  @Mock private Map<String, String> options;

  @Before
  public void setUp() throws SchemaLoaderException {
    closeable = MockitoAnnotations.openMocks(this);

    // Arrange
    schemaLoaderMockedStatic = mockStatic(SchemaLoader.class, CALLS_REAL_METHODS);
    schemaLoaderMockedStatic.when(() -> SchemaLoader.getSchemaOperator(any())).thenReturn(operator);
    schemaLoaderMockedStatic
        .when(() -> SchemaLoader.getSchemaParser(any(), anyMap()))
        .thenReturn(parser);

    when(parser.parse()).thenReturn(Collections.emptyList());
  }

  @After
  public void tearDown() throws Exception {
    schemaLoaderMockedStatic.close();
    closeable.close();
  }

  @Test
  public void
      load_WithConfigFileAndSchemaFileWithCreateCoordinatorTable_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.load(configFilePath, schemaFilePath, options, true);

    // Assert
    verify(parser).parse();
    verify(operator).createTables(anyList());
    verify(operator).createCoordinatorTable(options);
  }

  @Test
  public void
      load_WithConfigFileAndSchemaFileWithoutCreateCoordinatorTable_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.load(configFilePath, schemaFilePath, options, false);

    // Assert
    verify(parser).parse();
    verify(operator).createTables(anyList());
    verify(operator, never()).createCoordinatorTable(options);
  }

  @Test
  public void
      load_WithConfigFileAndNullSchemaFileWithCreateCoordinatorTable_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.load(configFilePath, (Path) null, options, true);

    // Assert
    verify(parser, never()).parse();
    verify(operator).createTables(anyList());
    verify(operator).createCoordinatorTable(options);
  }

  @Test
  public void
      load_WithConfigFileAndNullSchemaFileWithoutCreateCoordinatorTable_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.load(configFilePath, (Path) null, options, false);

    // Assert
    verify(parser, never()).parse();
    verify(operator).createTables(anyList());
    verify(operator, never()).createCoordinatorTable(options);
  }

  @Test
  public void
      load_WithConfigFileAndSerializedSchemaJsonWithCreateCoordinatorTable_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.load(configFilePath, serializedSchemaJson, options, true);

    // Assert
    verify(parser).parse();
    verify(operator).createTables(anyList());
    verify(operator).createCoordinatorTable(options);
  }

  @Test
  public void
      load_WithConfigFileAndSerializedSchemaJsonWithoutCreateCoordinatorTable_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.load(configFilePath, serializedSchemaJson, options, false);

    // Assert
    verify(parser).parse();
    verify(operator).createTables(anyList());
    verify(operator, never()).createCoordinatorTable(options);
  }

  @Test
  public void
      load_WithConfigFileAndNullSerializedSchemaJsonWithCreateCoordinatorTable_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.load(configFilePath, (String) null, options, true);

    // Assert
    verify(parser, never()).parse();
    verify(operator).createTables(anyList());
    verify(operator).createCoordinatorTable(options);
  }

  @Test
  public void
      load_WithConfigFileAndNullSerializedSchemaJsonWithoutCreateCoordinatorTable_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.load(configFilePath, (String) null, options, false);

    // Assert
    verify(parser, never()).parse();
    verify(operator).createTables(anyList());
    verify(operator, never()).createCoordinatorTable(options);
  }

  @Test
  public void
      load_WithConfigPropertiesAndSchemaFileWithCreateCoordinatorTable_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.load(configProperties, schemaFilePath, options, true);

    // Assert
    verify(parser).parse();
    verify(operator).createTables(anyList());
    verify(operator).createCoordinatorTable(options);
  }

  @Test
  public void
      load_WithConfigPropertiesAndSchemaFileWithoutCreateCoordinatorTable_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.load(configProperties, schemaFilePath, options, false);

    // Assert
    verify(parser).parse();
    verify(operator).createTables(anyList());
    verify(operator, never()).createCoordinatorTable(options);
  }

  @Test
  public void
      load_WithConfigPropertiesAndNullSchemaFileWithCreateCoordinatorTable_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.load(configProperties, (Path) null, options, true);

    // Assert
    verify(parser, never()).parse();
    verify(operator).createTables(anyList());
    verify(operator).createCoordinatorTable(options);
  }

  @Test
  public void
      load_WithConfigPropertiesAndNullSchemaFileWithoutCreateCoordinatorTable_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.load(configProperties, (Path) null, options, false);

    // Assert
    verify(parser, never()).parse();
    verify(operator).createTables(anyList());
    verify(operator, never()).createCoordinatorTable(options);
  }

  @Test
  public void
      load_WithConfigPropertiesAndSerializedSchemaJsonWithCreateCoordinatorTable_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.load(configProperties, serializedSchemaJson, options, true);

    // Assert
    verify(parser).parse();
    verify(operator).createTables(anyList());
    verify(operator).createCoordinatorTable(options);
  }

  @Test
  public void
      load_WithConfigPropertiesAndSerializedSchemaJsonWithoutCreateCoordinatorTable_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.load(configProperties, serializedSchemaJson, options, false);

    // Assert
    verify(parser).parse();
    verify(operator).createTables(anyList());
    verify(operator, never()).createCoordinatorTable(options);
  }

  @Test
  public void
      load_WithConfigPropertiesAndNullSerializedSchemaJsonWithCreateCoordinatorTable_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.load(configProperties, (String) null, options, true);

    // Assert
    verify(parser, never()).parse();
    verify(operator).createTables(anyList());
    verify(operator).createCoordinatorTable(options);
  }

  @Test
  public void
      load_WithConfigPropertiesAndNullSerializedSchemaJsonWithoutCreateCoordinatorTable_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.load(configProperties, (String) null, options, false);

    // Assert
    verify(parser, never()).parse();
    verify(operator).createTables(anyList());
    verify(operator, never()).createCoordinatorTable(options);
  }

  @Test
  public void
      unload_WithConfigFileAndSchemaFileWithCreateCoordinatorTable_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.unload(configFilePath, schemaFilePath, true);

    // Assert
    verify(parser).parse();
    verify(operator).deleteTables(anyList());
    verify(operator).dropCoordinatorTable();
  }

  @Test
  public void
      unload_WithConfigFileAndSchemaFileWithoutCreateCoordinatorTable_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.unload(configFilePath, schemaFilePath, false);

    // Assert
    verify(parser).parse();
    verify(operator).deleteTables(anyList());
    verify(operator, never()).dropCoordinatorTable();
  }

  @Test
  public void
      unload_WithConfigFileAndNullSchemaFileWithCreateCoordinatorTable_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.unload(configFilePath, (Path) null, true);

    // Assert
    verify(parser, never()).parse();
    verify(operator).deleteTables(anyList());
    verify(operator).dropCoordinatorTable();
  }

  @Test
  public void
      unload_WithConfigFileAndNullSchemaFileWithoutCreateCoordinatorTable_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.unload(configFilePath, (Path) null, false);

    // Assert
    verify(parser, never()).parse();
    verify(operator).deleteTables(anyList());
    verify(operator, never()).dropCoordinatorTable();
  }

  @Test
  public void
      unload_WithConfigFileAndSerializedSchemaJsonWithCreateCoordinatorTable_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.unload(configFilePath, serializedSchemaJson, true);

    // Assert
    verify(parser).parse();
    verify(operator).deleteTables(anyList());
    verify(operator).dropCoordinatorTable();
  }

  @Test
  public void
      unload_WithConfigFileAndSerializedSchemaJsonWithoutCreateCoordinatorTable_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.unload(configFilePath, serializedSchemaJson, false);

    // Assert
    verify(parser).parse();
    verify(operator).deleteTables(anyList());
    verify(operator, never()).dropCoordinatorTable();
  }

  @Test
  public void
      unload_WithConfigFileAndNullSerializedSchemaJsonWithCreateCoordinatorTable_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.unload(configFilePath, (String) null, true);

    // Assert
    verify(parser, never()).parse();
    verify(operator).deleteTables(anyList());
    verify(operator).dropCoordinatorTable();
  }

  @Test
  public void
      unload_WithConfigFileAndNullSerializedSchemaJsonWithoutCreateCoordinatorTable_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.unload(configFilePath, (String) null, false);

    // Assert
    verify(parser, never()).parse();
    verify(operator).deleteTables(anyList());
    verify(operator, never()).dropCoordinatorTable();
  }

  @Test
  public void
      unload_WithConfigPropertiesAndSchemaFileWithCreateCoordinatorTable_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.unload(configProperties, schemaFilePath, true);

    // Assert
    verify(parser).parse();
    verify(operator).deleteTables(anyList());
    verify(operator).dropCoordinatorTable();
  }

  @Test
  public void
      unload_WithConfigPropertiesAndSchemaFileWithoutCreateCoordinatorTable_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.unload(configProperties, schemaFilePath, false);

    // Assert
    verify(parser).parse();
    verify(operator).deleteTables(anyList());
    verify(operator, never()).dropCoordinatorTable();
  }

  @Test
  public void
      unload_WithConfigPropertiesAndNullSchemaFileWithCreateCoordinatorTable_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.unload(configProperties, (Path) null, true);

    // Assert
    verify(parser, never()).parse();
    verify(operator).deleteTables(anyList());
    verify(operator).dropCoordinatorTable();
  }

  @Test
  public void
      unload_WithConfigPropertiesAndNullSchemaFileWithoutCreateCoordinatorTable_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.unload(configProperties, (Path) null, false);

    // Assert
    verify(parser, never()).parse();
    verify(operator).deleteTables(anyList());
    verify(operator, never()).dropCoordinatorTable();
  }

  @Test
  public void
      unload_WithConfigPropertiesAndSerializedSchemaJsonWithCreateCoordinatorTable_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.unload(configProperties, serializedSchemaJson, true);

    // Assert
    verify(parser).parse();
    verify(operator).deleteTables(anyList());
    verify(operator).dropCoordinatorTable();
  }

  @Test
  public void
      unload_WithConfigPropertiesAndSerializedSchemaJsonWithoutCreateCoordinatorTable_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.unload(configProperties, serializedSchemaJson, false);

    // Assert
    verify(parser).parse();
    verify(operator).deleteTables(anyList());
    verify(operator, never()).dropCoordinatorTable();
  }

  @Test
  public void
      unload_WithConfigPropertiesAndNullSerializedSchemaJsonWithCreateCoordinatorTable_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.unload(configProperties, (String) null, true);

    // Assert
    verify(parser, never()).parse();
    verify(operator).deleteTables(anyList());
    verify(operator).dropCoordinatorTable();
  }

  @Test
  public void
      unload_WithConfigPropertiesAndNullSerializedSchemaJsonWithoutCreateCoordinatorTable_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.unload(configProperties, (String) null, false);

    // Assert
    verify(parser, never()).parse();
    verify(operator).deleteTables(anyList());
    verify(operator, never()).dropCoordinatorTable();
  }
}
