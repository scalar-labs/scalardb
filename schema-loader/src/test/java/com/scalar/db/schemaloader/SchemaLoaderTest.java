package com.scalar.db.schemaloader;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;

public class SchemaLoaderTest {

  private AutoCloseable closeable;
  private MockedStatic<SchemaLoader> schemaLoaderMockedStatic;

  @Mock private SchemaOperator operator;
  @Mock private SchemaParser parser;
  @Mock private ImportSchemaParser importSchemaParser;

  @Mock private Path configFilePath;
  @Mock private Properties configProperties;
  @Mock private Path schemaFilePath;
  private static final String SERIALIZED_SCHEMA_JSON = "some_schema";
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
    schemaLoaderMockedStatic
        .when(() -> SchemaLoader.getImportSchemaParser(any(), anyMap()))
        .thenReturn(importSchemaParser);
    when(parser.parse()).thenReturn(Collections.emptyList());
    when(importSchemaParser.parse()).thenReturn(Collections.emptyList());
  }

  @AfterEach
  public void tearDown() throws Exception {
    schemaLoaderMockedStatic.close();
    closeable.close();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void
      load_WithConfigFileAndSchemaFileWithCreateCoordinatorTables_ShouldCallParserAndOperatorProperly(
          boolean withReplicationTables) throws Exception {
    // Arrange

    // Act
    SchemaLoader.load(configFilePath, schemaFilePath, options, true, withReplicationTables);

    // Assert
    verify(parser).parse();
    verify(operator).createTables(anyList());
    verify(operator).createCoordinatorTables(options);
    if (withReplicationTables) {
      verify(operator).createReplicationTables(options);
    } else {
      verify(operator, never()).createReplicationTables(options);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void
      load_WithConfigFileAndSchemaFileWithoutCreateCoordinatorTables_ShouldCallParserAndOperatorProperly(
          boolean withReplicationTables) throws Exception {
    // Arrange

    // Act
    SchemaLoader.load(configFilePath, schemaFilePath, options, false, withReplicationTables);

    // Assert
    verify(parser).parse();
    verify(operator).createTables(anyList());
    verify(operator, never()).createCoordinatorTables(options);
    if (withReplicationTables) {
      verify(operator).createReplicationTables(options);
    } else {
      verify(operator, never()).createReplicationTables(options);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void
      load_WithConfigFileAndNullSchemaFileWithCreateCoordinatorTables_ShouldCallParserAndOperatorProperly(
          boolean withReplicationTables) throws Exception {
    // Arrange

    // Act
    SchemaLoader.load(configFilePath, (Path) null, options, true, withReplicationTables);

    // Assert
    verify(parser, never()).parse();
    verify(operator).createTables(anyList());
    verify(operator).createCoordinatorTables(options);
    if (withReplicationTables) {
      verify(operator).createReplicationTables(options);
    } else {
      verify(operator, never()).createReplicationTables(options);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void
      load_WithConfigFileAndNullSchemaFileWithoutCreateCoordinatorTables_ShouldCallParserAndOperatorProperly(
          boolean withReplicationTables) throws Exception {
    // Arrange

    // Act
    SchemaLoader.load(configFilePath, (Path) null, options, false, withReplicationTables);

    // Assert
    verify(parser, never()).parse();
    verify(operator).createTables(anyList());
    verify(operator, never()).createCoordinatorTables(options);
    if (withReplicationTables) {
      verify(operator).createReplicationTables(options);
    } else {
      verify(operator, never()).createReplicationTables(options);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void
      load_WithConfigFileAndSerializedSchemaJsonWithCreateCoordinatorTables_ShouldCallParserAndOperatorProperly(
          boolean withReplicationTables) throws Exception {
    // Arrange

    // Act
    SchemaLoader.load(configFilePath, SERIALIZED_SCHEMA_JSON, options, true, withReplicationTables);

    // Assert
    verify(parser).parse();
    verify(operator).createTables(anyList());
    verify(operator).createCoordinatorTables(options);
    if (withReplicationTables) {
      verify(operator).createReplicationTables(options);
    } else {
      verify(operator, never()).createReplicationTables(options);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void
      load_WithConfigFileAndSerializedSchemaJsonWithoutCreateCoordinatorTables_ShouldCallParserAndOperatorProperly(
          boolean withReplicationTables) throws Exception {
    // Arrange

    // Act
    SchemaLoader.load(
        configFilePath, SERIALIZED_SCHEMA_JSON, options, false, withReplicationTables);

    // Assert
    verify(parser).parse();
    verify(operator).createTables(anyList());
    verify(operator, never()).createCoordinatorTables(options);
    if (withReplicationTables) {
      verify(operator).createReplicationTables(options);
    } else {
      verify(operator, never()).createReplicationTables(options);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void
      load_WithConfigFileAndNullSerializedSchemaJsonWithCreateCoordinatorTables_ShouldCallParserAndOperatorProperly(
          boolean withReplicationTables) throws Exception {
    // Arrange

    // Act
    SchemaLoader.load(configFilePath, (String) null, options, true, withReplicationTables);

    // Assert
    verify(parser, never()).parse();
    verify(operator).createTables(anyList());
    verify(operator).createCoordinatorTables(options);
    if (withReplicationTables) {
      verify(operator).createReplicationTables(options);
    } else {
      verify(operator, never()).createReplicationTables(options);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void
      load_WithConfigFileAndNullSerializedSchemaJsonWithoutCreateCoordinatorTables_ShouldCallParserAndOperatorProperly(
          boolean withReplicationTables) throws Exception {
    // Arrange

    // Act
    SchemaLoader.load(configFilePath, (String) null, options, false, withReplicationTables);

    // Assert
    verify(parser, never()).parse();
    verify(operator).createTables(anyList());
    verify(operator, never()).createCoordinatorTables(options);
    if (withReplicationTables) {
      verify(operator).createReplicationTables(options);
    } else {
      verify(operator, never()).createReplicationTables(options);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void
      load_WithConfigPropertiesAndSchemaFileWithCreateCoordinatorTables_ShouldCallParserAndOperatorProperly(
          boolean withReplicationTables) throws Exception {
    // Arrange

    // Act
    SchemaLoader.load(configProperties, schemaFilePath, options, true, withReplicationTables);

    // Assert
    verify(parser).parse();
    verify(operator).createTables(anyList());
    verify(operator).createCoordinatorTables(options);
    if (withReplicationTables) {
      verify(operator).createReplicationTables(options);
    } else {
      verify(operator, never()).createReplicationTables(options);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void
      load_WithConfigPropertiesAndSchemaFileWithoutCreateCoordinatorTables_ShouldCallParserAndOperatorProperly(
          boolean withReplicationTables) throws Exception {
    // Arrange

    // Act
    SchemaLoader.load(configProperties, schemaFilePath, options, false, withReplicationTables);

    // Assert
    verify(parser).parse();
    verify(operator).createTables(anyList());
    verify(operator, never()).createCoordinatorTables(options);
    if (withReplicationTables) {
      verify(operator).createReplicationTables(options);
    } else {
      verify(operator, never()).createReplicationTables(options);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void
      load_WithConfigPropertiesAndNullSchemaFileWithCreateCoordinatorTables_ShouldCallParserAndOperatorProperly(
          boolean withReplicationTables) throws Exception {
    // Arrange

    // Act
    SchemaLoader.load(configProperties, (Path) null, options, true, withReplicationTables);

    // Assert
    verify(parser, never()).parse();
    verify(operator).createTables(anyList());
    verify(operator).createCoordinatorTables(options);
    if (withReplicationTables) {
      verify(operator).createReplicationTables(options);
    } else {
      verify(operator, never()).createReplicationTables(options);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void
      load_WithConfigPropertiesAndNullSchemaFileWithoutCreateCoordinatorTables_ShouldCallParserAndOperatorProperly(
          boolean withReplicationTables) throws Exception {
    // Arrange

    // Act
    SchemaLoader.load(configProperties, (Path) null, options, false, withReplicationTables);

    // Assert
    verify(parser, never()).parse();
    verify(operator).createTables(anyList());
    verify(operator, never()).createCoordinatorTables(options);
    if (withReplicationTables) {
      verify(operator).createReplicationTables(options);
    } else {
      verify(operator, never()).createReplicationTables(options);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void
      load_WithConfigPropertiesAndSerializedSchemaJsonWithCreateCoordinatorTables_ShouldCallParserAndOperatorProperly(
          boolean withReplicationTables) throws Exception {
    // Arrange

    // Act
    SchemaLoader.load(
        configProperties, SERIALIZED_SCHEMA_JSON, options, true, withReplicationTables);

    // Assert
    verify(parser).parse();
    verify(operator).createTables(anyList());
    verify(operator).createCoordinatorTables(options);
    if (withReplicationTables) {
      verify(operator).createReplicationTables(options);
    } else {
      verify(operator, never()).createReplicationTables(options);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void
      load_WithConfigPropertiesAndSerializedSchemaJsonWithoutCreateCoordinatorTables_ShouldCallParserAndOperatorProperly(
          boolean withReplicationTables) throws Exception {
    // Arrange

    // Act
    SchemaLoader.load(
        configProperties, SERIALIZED_SCHEMA_JSON, options, false, withReplicationTables);

    // Assert
    verify(parser).parse();
    verify(operator).createTables(anyList());
    verify(operator, never()).createCoordinatorTables(options);
    if (withReplicationTables) {
      verify(operator).createReplicationTables(options);
    } else {
      verify(operator, never()).createReplicationTables(options);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void
      load_WithConfigPropertiesAndNullSerializedSchemaJsonWithCreateCoordinatorTables_ShouldCallParserAndOperatorProperly(
          boolean withReplicationTables) throws Exception {
    // Arrange

    // Act
    SchemaLoader.load(configProperties, (String) null, options, true, withReplicationTables);

    // Assert
    verify(parser, never()).parse();
    verify(operator).createTables(anyList());
    verify(operator).createCoordinatorTables(options);
    if (withReplicationTables) {
      verify(operator).createReplicationTables(options);
    } else {
      verify(operator, never()).createReplicationTables(options);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void
      load_WithConfigPropertiesAndNullSerializedSchemaJsonWithoutCreateCoordinatorTables_ShouldCallParserAndOperatorProperly(
          boolean withReplicationTables) throws Exception {
    // Arrange

    // Act
    SchemaLoader.load(configProperties, (String) null, options, false, withReplicationTables);

    // Assert
    verify(parser, never()).parse();
    verify(operator).createTables(anyList());
    verify(operator, never()).createCoordinatorTables(options);
    if (withReplicationTables) {
      verify(operator).createReplicationTables(options);
    } else {
      verify(operator, never()).createReplicationTables(options);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void
      unload_WithConfigFileAndSchemaFileWithDeleteCoordinatorTables_ShouldCallParserAndOperatorProperly(
          boolean withReplicationTables) throws Exception {
    // Arrange

    // Act
    SchemaLoader.unload(configFilePath, schemaFilePath, true, withReplicationTables);

    // Assert
    verify(parser).parse();
    verify(operator).deleteTables(anyList());
    verify(operator).dropCoordinatorTables();
    if (withReplicationTables) {
      verify(operator).dropReplicationTables();
    } else {
      verify(operator, never()).dropReplicationTables();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void
      unload_WithConfigFileAndSchemaFileWithoutDeleteCoordinatorTables_ShouldCallParserAndOperatorProperly(
          boolean withReplicationTables) throws Exception {
    // Arrange

    // Act
    SchemaLoader.unload(configFilePath, schemaFilePath, false, withReplicationTables);

    // Assert
    verify(parser).parse();
    verify(operator).deleteTables(anyList());
    verify(operator, never()).dropCoordinatorTables();
    if (withReplicationTables) {
      verify(operator).dropReplicationTables();
    } else {
      verify(operator, never()).dropReplicationTables();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void
      unload_WithConfigFileAndNullSchemaFileWithDeleteCoordinatorTables_ShouldCallParserAndOperatorProperly(
          boolean withReplicationTables) throws Exception {
    // Arrange

    // Act
    SchemaLoader.unload(configFilePath, (Path) null, true, withReplicationTables);

    // Assert
    verify(parser, never()).parse();
    verify(operator).deleteTables(anyList());
    verify(operator).dropCoordinatorTables();
    if (withReplicationTables) {
      verify(operator).dropReplicationTables();
    } else {
      verify(operator, never()).dropReplicationTables();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void
      unload_WithConfigFileAndNullSchemaFileWithoutDeleteCoordinatorTables_ShouldCallParserAndOperatorProperly(
          boolean withReplicationTables) throws Exception {
    // Arrange

    // Act
    SchemaLoader.unload(configFilePath, (Path) null, false, withReplicationTables);

    // Assert
    verify(parser, never()).parse();
    verify(operator).deleteTables(anyList());
    verify(operator, never()).dropCoordinatorTables();
    if (withReplicationTables) {
      verify(operator).dropReplicationTables();
    } else {
      verify(operator, never()).dropReplicationTables();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void
      unload_WithConfigFileAndSerializedSchemaJsonWithDeleteCoordinatorTables_ShouldCallParserAndOperatorProperly(
          boolean withReplicationTables) throws Exception {
    // Arrange

    // Act
    SchemaLoader.unload(configFilePath, SERIALIZED_SCHEMA_JSON, true, withReplicationTables);

    // Assert
    verify(parser).parse();
    verify(operator).deleteTables(anyList());
    verify(operator).dropCoordinatorTables();
    if (withReplicationTables) {
      verify(operator).dropReplicationTables();
    } else {
      verify(operator, never()).dropReplicationTables();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void
      unload_WithConfigFileAndSerializedSchemaJsonWithoutDeleteCoordinatorTables_ShouldCallParserAndOperatorProperly(
          boolean withReplicationTables) throws Exception {
    // Arrange

    // Act
    SchemaLoader.unload(configFilePath, SERIALIZED_SCHEMA_JSON, false, withReplicationTables);

    // Assert
    verify(parser).parse();
    verify(operator).deleteTables(anyList());
    verify(operator, never()).dropCoordinatorTables();
    if (withReplicationTables) {
      verify(operator).dropReplicationTables();
    } else {
      verify(operator, never()).dropReplicationTables();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void
      unload_WithConfigFileAndNullSerializedSchemaJsonWithDeleteCoordinatorTables_ShouldCallParserAndOperatorProperly(
          boolean withReplicationTables) throws Exception {
    // Arrange

    // Act
    SchemaLoader.unload(configFilePath, (String) null, true, withReplicationTables);

    // Assert
    verify(parser, never()).parse();
    verify(operator).deleteTables(anyList());
    verify(operator).dropCoordinatorTables();
    if (withReplicationTables) {
      verify(operator).dropReplicationTables();
    } else {
      verify(operator, never()).dropReplicationTables();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void
      unload_WithConfigFileAndNullSerializedSchemaJsonWithoutDeleteCoordinatorTables_ShouldCallParserAndOperatorProperly(
          boolean withReplicationTables) throws Exception {
    // Arrange

    // Act
    SchemaLoader.unload(configFilePath, (String) null, false, withReplicationTables);

    // Assert
    verify(parser, never()).parse();
    verify(operator).deleteTables(anyList());
    verify(operator, never()).dropCoordinatorTables();
    if (withReplicationTables) {
      verify(operator).dropReplicationTables();
    } else {
      verify(operator, never()).dropReplicationTables();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void
      unload_WithConfigPropertiesAndSchemaFileWithDeleteCoordinatorTables_ShouldCallParserAndOperatorProperly(
          boolean withReplicationTables) throws Exception {
    // Arrange

    // Act
    SchemaLoader.unload(configProperties, schemaFilePath, true, withReplicationTables);

    // Assert
    verify(parser).parse();
    verify(operator).deleteTables(anyList());
    verify(operator).dropCoordinatorTables();
    if (withReplicationTables) {
      verify(operator).dropReplicationTables();
    } else {
      verify(operator, never()).dropReplicationTables();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void
      unload_WithConfigPropertiesAndSchemaFileWithoutDeleteCoordinatorTables_ShouldCallParserAndOperatorProperly(
          boolean withReplicationTables) throws Exception {
    // Arrange

    // Act
    SchemaLoader.unload(configProperties, schemaFilePath, false, withReplicationTables);

    // Assert
    verify(parser).parse();
    verify(operator).deleteTables(anyList());
    verify(operator, never()).dropCoordinatorTables();
    if (withReplicationTables) {
      verify(operator).dropReplicationTables();
    } else {
      verify(operator, never()).dropReplicationTables();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void
      unload_WithConfigPropertiesAndNullSchemaFileWithDeleteCoordinatorTables_ShouldCallParserAndOperatorProperly(
          boolean withReplicationTables) throws Exception {
    // Arrange

    // Act
    SchemaLoader.unload(configProperties, (Path) null, true, withReplicationTables);

    // Assert
    verify(parser, never()).parse();
    verify(operator).deleteTables(anyList());
    verify(operator).dropCoordinatorTables();
    if (withReplicationTables) {
      verify(operator).dropReplicationTables();
    } else {
      verify(operator, never()).dropReplicationTables();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void
      unload_WithConfigPropertiesAndNullSchemaFileWithoutDeleteCoordinatorTables_ShouldCallParserAndOperatorProperly(
          boolean withReplicationTables) throws Exception {
    // Arrange

    // Act
    SchemaLoader.unload(configProperties, (Path) null, false, withReplicationTables);

    // Assert
    verify(parser, never()).parse();
    verify(operator).deleteTables(anyList());
    verify(operator, never()).dropCoordinatorTables();
    if (withReplicationTables) {
      verify(operator).dropReplicationTables();
    } else {
      verify(operator, never()).dropReplicationTables();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void
      unload_WithConfigPropertiesAndSerializedSchemaJsonWithDeleteCoordinatorTables_ShouldCallParserAndOperatorProperly(
          boolean withReplicationTables) throws Exception {
    // Arrange

    // Act
    SchemaLoader.unload(configProperties, SERIALIZED_SCHEMA_JSON, true, withReplicationTables);

    // Assert
    verify(parser).parse();
    verify(operator).deleteTables(anyList());
    verify(operator).dropCoordinatorTables();
    if (withReplicationTables) {
      verify(operator).dropReplicationTables();
    } else {
      verify(operator, never()).dropReplicationTables();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void
      unload_WithConfigPropertiesAndSerializedSchemaJsonWithoutDeleteCoordinatorTables_ShouldCallParserAndOperatorProperly(
          boolean withReplicationTables) throws Exception {
    // Arrange

    // Act
    SchemaLoader.unload(configProperties, SERIALIZED_SCHEMA_JSON, false, withReplicationTables);

    // Assert
    verify(parser).parse();
    verify(operator).deleteTables(anyList());
    verify(operator, never()).dropCoordinatorTables();
    if (withReplicationTables) {
      verify(operator).dropReplicationTables();
    } else {
      verify(operator, never()).dropReplicationTables();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void
      unload_WithConfigPropertiesAndNullSerializedSchemaJsonWithDeleteCoordinatorTables_ShouldCallParserAndOperatorProperly(
          boolean withReplicationTables) throws Exception {
    // Arrange

    // Act
    SchemaLoader.unload(configProperties, (String) null, true, withReplicationTables);

    // Assert
    verify(parser, never()).parse();
    verify(operator).deleteTables(anyList());
    verify(operator).dropCoordinatorTables();
    if (withReplicationTables) {
      verify(operator).dropReplicationTables();
    } else {
      verify(operator, never()).dropReplicationTables();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void
      unload_WithConfigPropertiesAndNullSerializedSchemaJsonWithoutDeleteCoordinatorTables_ShouldCallParserAndOperatorProperly(
          boolean withReplicationTables) throws Exception {
    // Arrange

    // Act
    SchemaLoader.unload(configProperties, (String) null, false, withReplicationTables);

    // Assert
    verify(parser, never()).parse();
    verify(operator).deleteTables(anyList());
    verify(operator, never()).dropCoordinatorTables();
    if (withReplicationTables) {
      verify(operator).dropReplicationTables();
    } else {
      verify(operator, never()).dropReplicationTables();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void
      repairAll_WithConfigFilePathAndSerializedSchemaAndDoRepairCoordinatorTables_ShouldCallParserAndOperatorProperly(
          boolean withReplicationTables) throws Exception {
    // Arrange

    // Act
    SchemaLoader.repairAll(
        configFilePath, SERIALIZED_SCHEMA_JSON, options, true, withReplicationTables);

    // Assert
    verify(parser).parse();
    verify(operator).repairNamespaces(anyList());
    verify(operator).repairTables(anyList());
    verify(operator).repairCoordinatorTables(options);
    if (withReplicationTables) {
      verify(operator).repairReplicationTables(options);
    } else {
      verify(operator, never()).repairReplicationTables(options);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void
      repairAll_WithConfigFilePathAndSerializedSchemaAndDoNotRepairCoordinatorTables_ShouldCallParserAndOperatorProperly(
          boolean withReplicationTables) throws Exception {
    // Arrange

    // Act
    SchemaLoader.repairAll(
        configFilePath, SERIALIZED_SCHEMA_JSON, options, false, withReplicationTables);

    // Assert
    verify(parser).parse();
    verify(operator).repairNamespaces(anyList());
    verify(operator).repairTables(anyList());
    verify(operator, never()).repairCoordinatorTables(anyMap());
    if (withReplicationTables) {
      verify(operator).repairReplicationTables(options);
    } else {
      verify(operator, never()).repairReplicationTables(options);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void
      repairAll_WithConfigPropertiesAndSerializedSchemaAndDoRepairCoordinatorTables_ShouldCallParserAndOperatorProperly(
          boolean withReplicationTables) throws Exception {
    // Arrange

    // Act
    SchemaLoader.repairAll(
        configProperties, SERIALIZED_SCHEMA_JSON, options, true, withReplicationTables);

    // Assert
    verify(parser).parse();
    verify(operator).repairNamespaces(anyList());
    verify(operator).repairTables(anyList());
    verify(operator).repairCoordinatorTables(options);
    if (withReplicationTables) {
      verify(operator).repairReplicationTables(options);
    } else {
      verify(operator, never()).repairReplicationTables(options);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void
      repairAll_WithConfigPropertiesAndSerializedSchemaAndDoNotRepairCoordinatorTables_ShouldCallParserAndOperatorProperly(
          boolean withReplicationTables) throws Exception {
    // Arrange

    // Act
    SchemaLoader.repairAll(
        configProperties, SERIALIZED_SCHEMA_JSON, options, false, withReplicationTables);

    // Assert
    verify(parser).parse();
    verify(operator).repairNamespaces(anyList());
    verify(operator).repairTables(anyList());
    verify(operator, never()).repairCoordinatorTables(anyMap());
    if (withReplicationTables) {
      verify(operator).repairReplicationTables(options);
    } else {
      verify(operator, never()).repairReplicationTables(options);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void
      repairAll_WithConfigPropertiesAndSchemaFilePathAndDoRepairCoordinatorTables_ShouldCallParserAndOperatorProperly(
          boolean withReplicationTables) throws Exception {
    // Arrange

    // Act
    SchemaLoader.repairAll(configProperties, schemaFilePath, options, true, withReplicationTables);

    // Assert
    verify(parser).parse();
    verify(operator).repairNamespaces(anyList());
    verify(operator).repairTables(anyList());
    verify(operator).repairCoordinatorTables(options);
    if (withReplicationTables) {
      verify(operator).repairReplicationTables(options);
    } else {
      verify(operator, never()).repairReplicationTables(options);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void
      repairAll_WithConfigPropertiesAndSchemaFilePathAndDoNotRepairCoordinatorTables_ShouldCallParserAndOperatorProperly(
          boolean withReplicationTables) throws Exception {
    // Arrange

    // Act
    SchemaLoader.repairAll(configProperties, schemaFilePath, options, false, withReplicationTables);

    // Assert
    verify(parser).parse();
    verify(operator).repairNamespaces(anyList());
    verify(operator).repairTables(anyList());
    verify(operator, never()).repairCoordinatorTables(anyMap());
    if (withReplicationTables) {
      verify(operator).repairReplicationTables(options);
    } else {
      verify(operator, never()).repairReplicationTables(options);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void
      repairAll_WithConfigFilePathAndSchemaFilePathAndDoRepairCoordinatorTables_ShouldCallParserAndOperatorProperly(
          boolean withReplicationTables) throws Exception {
    // Arrange

    // Act
    SchemaLoader.repairAll(configFilePath, schemaFilePath, options, true, withReplicationTables);

    // Assert
    verify(parser).parse();
    verify(operator).repairNamespaces(anyList());
    verify(operator).repairTables(anyList());
    verify(operator).repairCoordinatorTables(options);
    if (withReplicationTables) {
      verify(operator).repairReplicationTables(options);
    } else {
      verify(operator, never()).repairReplicationTables(options);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void
      repairAll_WithConfigFilePathAndSchemaFilePathAndDoNotRepairCoordinatorTables_ShouldCallParserAndOperatorProperly(
          boolean withReplicationTables) throws Exception {
    // Arrange

    // Act
    SchemaLoader.repairAll(configFilePath, schemaFilePath, options, false, withReplicationTables);

    // Assert
    verify(parser).parse();
    verify(operator).repairNamespaces(anyList());
    verify(operator).repairTables(anyList());
    verify(operator, never()).repairCoordinatorTables(anyMap());
    if (withReplicationTables) {
      verify(operator).repairReplicationTables(options);
    } else {
      verify(operator, never()).repairReplicationTables(options);
    }
  }

  @Test
  public void
      alterTables_WithConfigFilePathAndSerializedSchema_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.alterTables(configFilePath, SERIALIZED_SCHEMA_JSON, options);

    // Assert
    verify(parser).parse();
    verify(operator).alterTables(anyList(), anyMap());
  }

  @Test
  public void
      alterTable_WithConfigPropertiesAndSerializedSchema_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.alterTables(configProperties, SERIALIZED_SCHEMA_JSON, options);

    // Assert
    verify(parser).parse();
    verify(operator).alterTables(anyList(), anyMap());
  }

  @Test
  public void alterTables_WithConfigFilePathAndSchemaFilePath_ShouldCallParserAndOperatorProperly()
      throws Exception {
    // Arrange

    // Act
    SchemaLoader.alterTables(configFilePath, schemaFilePath, options);

    // Assert
    verify(parser).parse();
    verify(operator).alterTables(anyList(), anyMap());
  }

  @Test
  public void alterTable_WithConfigPropertiesAndSchemaFilePath_ShouldCallParserAndOperatorProperly()
      throws Exception {
    // Arrange

    // Act
    SchemaLoader.alterTables(configProperties, schemaFilePath, options);

    // Assert
    verify(parser).parse();
    verify(operator).alterTables(anyList(), anyMap());
  }

  @Test
  public void
      importTable_WithConfigPropertiesAndSerializedSchema_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.importTables(configProperties, SERIALIZED_SCHEMA_JSON, options);

    // Assert
    verify(importSchemaParser).parse();
    verify(operator).importTables(anyList(), eq(options));
  }

  @Test
  public void
      importTable_WithConfigFilePathAndSerializedSchema_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.importTables(configFilePath, SERIALIZED_SCHEMA_JSON, options);

    // Assert
    verify(importSchemaParser).parse();
    verify(operator).importTables(anyList(), eq(options));
  }

  @Test
  public void
      importTable_WithConfigPropertiesAndSchemaFilePath_ShouldCallParserAndOperatorProperly()
          throws Exception {
    // Arrange

    // Act
    SchemaLoader.importTables(configProperties, schemaFilePath, options);

    // Assert
    verify(importSchemaParser).parse();
    verify(operator).importTables(anyList(), eq(options));
  }

  @Test
  public void importTable_WithConfigFilePathAndSchemaFilePath_ShouldCallParserAndOperatorProperly()
      throws Exception {
    // Arrange

    // Act
    SchemaLoader.importTables(configFilePath, schemaFilePath, options);

    // Assert
    verify(importSchemaParser).parse();
    verify(operator).importTables(anyList(), eq(options));
  }

  @Test
  public void upgrade_WithConfigProperties_ShouldCallOperatorProperly() throws Exception {
    // Arrange

    // Act
    SchemaLoader.upgrade(configProperties, options);

    // Assert
    verify(operator).upgrade(options);
  }

  @Test
  public void upgrade_WithConfigFilePath_ShouldCallOperatorProperly() throws Exception {
    // Arrange

    // Act
    SchemaLoader.upgrade(configFilePath, options);

    // Assert
    verify(operator).upgrade(options);
  }
}
