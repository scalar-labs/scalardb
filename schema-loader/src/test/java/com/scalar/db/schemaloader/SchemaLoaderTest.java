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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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

  @BeforeEach
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

  @AfterEach
  public void tearDown() throws Exception {
    schemaLoaderMockedStatic.close();
    closeable.close();
  }

  @Test
  public void
      load_WithConfigFileAndSchemaFileWithCreateCoordinatorTables_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.load(configFilePath, schemaFilePath, options, true);

    // Assert
    verify(parser).parse();
    verify(operator).createTables(anyList());
    verify(operator).createCoordinatorTables(options);
  }

  @Test
  public void
      load_WithConfigFileAndSchemaFileWithoutCreateCoordinatorTables_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.load(configFilePath, schemaFilePath, options, false);

    // Assert
    verify(parser).parse();
    verify(operator).createTables(anyList());
    verify(operator, never()).createCoordinatorTables(options);
  }

  @Test
  public void
      load_WithConfigFileAndNullSchemaFileWithCreateCoordinatorTables_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.load(configFilePath, (Path) null, options, true);

    // Assert
    verify(parser, never()).parse();
    verify(operator).createTables(anyList());
    verify(operator).createCoordinatorTables(options);
  }

  @Test
  public void
      load_WithConfigFileAndNullSchemaFileWithoutCreateCoordinatorTables_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.load(configFilePath, (Path) null, options, false);

    // Assert
    verify(parser, never()).parse();
    verify(operator).createTables(anyList());
    verify(operator, never()).createCoordinatorTables(options);
  }

  @Test
  public void
      load_WithConfigFileAndSerializedSchemaJsonWithCreateCoordinatorTables_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.load(configFilePath, serializedSchemaJson, options, true);

    // Assert
    verify(parser).parse();
    verify(operator).createTables(anyList());
    verify(operator).createCoordinatorTables(options);
  }

  @Test
  public void
      load_WithConfigFileAndSerializedSchemaJsonWithoutCreateCoordinatorTables_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.load(configFilePath, serializedSchemaJson, options, false);

    // Assert
    verify(parser).parse();
    verify(operator).createTables(anyList());
    verify(operator, never()).createCoordinatorTables(options);
  }

  @Test
  public void
      load_WithConfigFileAndNullSerializedSchemaJsonWithCreateCoordinatorTables_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.load(configFilePath, (String) null, options, true);

    // Assert
    verify(parser, never()).parse();
    verify(operator).createTables(anyList());
    verify(operator).createCoordinatorTables(options);
  }

  @Test
  public void
      load_WithConfigFileAndNullSerializedSchemaJsonWithoutCreateCoordinatorTables_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.load(configFilePath, (String) null, options, false);

    // Assert
    verify(parser, never()).parse();
    verify(operator).createTables(anyList());
    verify(operator, never()).createCoordinatorTables(options);
  }

  @Test
  public void
      load_WithConfigPropertiesAndSchemaFileWithCreateCoordinatorTables_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.load(configProperties, schemaFilePath, options, true);

    // Assert
    verify(parser).parse();
    verify(operator).createTables(anyList());
    verify(operator).createCoordinatorTables(options);
  }

  @Test
  public void
      load_WithConfigPropertiesAndSchemaFileWithoutCreateCoordinatorTables_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.load(configProperties, schemaFilePath, options, false);

    // Assert
    verify(parser).parse();
    verify(operator).createTables(anyList());
    verify(operator, never()).createCoordinatorTables(options);
  }

  @Test
  public void
      load_WithConfigPropertiesAndNullSchemaFileWithCreateCoordinatorTables_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.load(configProperties, (Path) null, options, true);

    // Assert
    verify(parser, never()).parse();
    verify(operator).createTables(anyList());
    verify(operator).createCoordinatorTables(options);
  }

  @Test
  public void
      load_WithConfigPropertiesAndNullSchemaFileWithoutCreateCoordinatorTables_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.load(configProperties, (Path) null, options, false);

    // Assert
    verify(parser, never()).parse();
    verify(operator).createTables(anyList());
    verify(operator, never()).createCoordinatorTables(options);
  }

  @Test
  public void
      load_WithConfigPropertiesAndSerializedSchemaJsonWithCreateCoordinatorTables_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.load(configProperties, serializedSchemaJson, options, true);

    // Assert
    verify(parser).parse();
    verify(operator).createTables(anyList());
    verify(operator).createCoordinatorTables(options);
  }

  @Test
  public void
      load_WithConfigPropertiesAndSerializedSchemaJsonWithoutCreateCoordinatorTables_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.load(configProperties, serializedSchemaJson, options, false);

    // Assert
    verify(parser).parse();
    verify(operator).createTables(anyList());
    verify(operator, never()).createCoordinatorTables(options);
  }

  @Test
  public void
      load_WithConfigPropertiesAndNullSerializedSchemaJsonWithCreateCoordinatorTables_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.load(configProperties, (String) null, options, true);

    // Assert
    verify(parser, never()).parse();
    verify(operator).createTables(anyList());
    verify(operator).createCoordinatorTables(options);
  }

  @Test
  public void
      load_WithConfigPropertiesAndNullSerializedSchemaJsonWithoutCreateCoordinatorTables_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.load(configProperties, (String) null, options, false);

    // Assert
    verify(parser, never()).parse();
    verify(operator).createTables(anyList());
    verify(operator, never()).createCoordinatorTables(options);
  }

  @Test
  public void
      unload_WithConfigFileAndSchemaFileWithDeleteCoordinatorTables_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.unload(configFilePath, schemaFilePath, true);

    // Assert
    verify(parser).parse();
    verify(operator).deleteTables(anyList());
    verify(operator).dropCoordinatorTables();
  }

  @Test
  public void
      unload_WithConfigFileAndSchemaFileWithoutDeleteCoordinatorTables_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.unload(configFilePath, schemaFilePath, false);

    // Assert
    verify(parser).parse();
    verify(operator).deleteTables(anyList());
    verify(operator, never()).dropCoordinatorTables();
  }

  @Test
  public void
      unload_WithConfigFileAndNullSchemaFileWithDeleteCoordinatorTables_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.unload(configFilePath, (Path) null, true);

    // Assert
    verify(parser, never()).parse();
    verify(operator).deleteTables(anyList());
    verify(operator).dropCoordinatorTables();
  }

  @Test
  public void
      unload_WithConfigFileAndNullSchemaFileWithoutDeleteCoordinatorTables_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.unload(configFilePath, (Path) null, false);

    // Assert
    verify(parser, never()).parse();
    verify(operator).deleteTables(anyList());
    verify(operator, never()).dropCoordinatorTables();
  }

  @Test
  public void
      unload_WithConfigFileAndSerializedSchemaJsonWithDeleteCoordinatorTables_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.unload(configFilePath, serializedSchemaJson, true);

    // Assert
    verify(parser).parse();
    verify(operator).deleteTables(anyList());
    verify(operator).dropCoordinatorTables();
  }

  @Test
  public void
      unload_WithConfigFileAndSerializedSchemaJsonWithoutDeleteCoordinatorTables_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.unload(configFilePath, serializedSchemaJson, false);

    // Assert
    verify(parser).parse();
    verify(operator).deleteTables(anyList());
    verify(operator, never()).dropCoordinatorTables();
  }

  @Test
  public void
      unload_WithConfigFileAndNullSerializedSchemaJsonWithDeleteCoordinatorTables_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.unload(configFilePath, (String) null, true);

    // Assert
    verify(parser, never()).parse();
    verify(operator).deleteTables(anyList());
    verify(operator).dropCoordinatorTables();
  }

  @Test
  public void
      unload_WithConfigFileAndNullSerializedSchemaJsonWithoutDeleteCoordinatorTables_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.unload(configFilePath, (String) null, false);

    // Assert
    verify(parser, never()).parse();
    verify(operator).deleteTables(anyList());
    verify(operator, never()).dropCoordinatorTables();
  }

  @Test
  public void
      unload_WithConfigPropertiesAndSchemaFileWithDeleteCoordinatorTables_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.unload(configProperties, schemaFilePath, true);

    // Assert
    verify(parser).parse();
    verify(operator).deleteTables(anyList());
    verify(operator).dropCoordinatorTables();
  }

  @Test
  public void
      unload_WithConfigPropertiesAndSchemaFileWithoutDeleteCoordinatorTables_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.unload(configProperties, schemaFilePath, false);

    // Assert
    verify(parser).parse();
    verify(operator).deleteTables(anyList());
    verify(operator, never()).dropCoordinatorTables();
  }

  @Test
  public void
      unload_WithConfigPropertiesAndNullSchemaFileWithDeleteCoordinatorTables_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.unload(configProperties, (Path) null, true);

    // Assert
    verify(parser, never()).parse();
    verify(operator).deleteTables(anyList());
    verify(operator).dropCoordinatorTables();
  }

  @Test
  public void
      unload_WithConfigPropertiesAndNullSchemaFileWithoutDeleteCoordinatorTables_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.unload(configProperties, (Path) null, false);

    // Assert
    verify(parser, never()).parse();
    verify(operator).deleteTables(anyList());
    verify(operator, never()).dropCoordinatorTables();
  }

  @Test
  public void
      unload_WithConfigPropertiesAndSerializedSchemaJsonWithDeleteCoordinatorTables_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.unload(configProperties, serializedSchemaJson, true);

    // Assert
    verify(parser).parse();
    verify(operator).deleteTables(anyList());
    verify(operator).dropCoordinatorTables();
  }

  @Test
  public void
      unload_WithConfigPropertiesAndSerializedSchemaJsonWithoutDeleteCoordinatorTables_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.unload(configProperties, serializedSchemaJson, false);

    // Assert
    verify(parser).parse();
    verify(operator).deleteTables(anyList());
    verify(operator, never()).dropCoordinatorTables();
  }

  @Test
  public void
      unload_WithConfigPropertiesAndNullSerializedSchemaJsonWithDeleteCoordinatorTables_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.unload(configProperties, (String) null, true);

    // Assert
    verify(parser, never()).parse();
    verify(operator).deleteTables(anyList());
    verify(operator).dropCoordinatorTables();
  }

  @Test
  public void
      unload_WithConfigPropertiesAndNullSerializedSchemaJsonWithoutDeleteCoordinatorTables_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.unload(configProperties, (String) null, false);

    // Assert
    verify(parser, never()).parse();
    verify(operator).deleteTables(anyList());
    verify(operator, never()).dropCoordinatorTables();
  }
}
