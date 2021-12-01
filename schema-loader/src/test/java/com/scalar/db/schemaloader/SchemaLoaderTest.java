package com.scalar.db.schemaloader;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.scalar.db.schemaloader.core.SchemaOperator;
import com.scalar.db.schemaloader.core.SchemaOperatorFactory;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Properties;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class SchemaLoaderTest {
  @Mock private SchemaOperator operator;
  @Mock private Path configFilePath;
  @Mock private Properties configProperties;
  @Mock private Path schemaFilePath;

  private AutoCloseable closeable;
  private MockedStatic<SchemaOperatorFactory> schemaOperatorFactoryMockedStatic;

  @Before
  public void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
    schemaOperatorFactoryMockedStatic = Mockito.mockStatic(SchemaOperatorFactory.class);
    schemaOperatorFactoryMockedStatic
        .when(
            () ->
                SchemaOperatorFactory.getSchemaOperator(
                    Mockito.any(Path.class), Mockito.anyBoolean()))
        .thenReturn(operator);
    schemaOperatorFactoryMockedStatic
        .when(
            () ->
                SchemaOperatorFactory.getSchemaOperator(
                    Mockito.any(Properties.class), Mockito.anyBoolean()))
        .thenReturn(operator);
  }

  @After
  public void tearDown() throws Exception {
    schemaOperatorFactoryMockedStatic.close();
    closeable.close();
  }

  @Test
  public void load_WithProperFilePathsArguments_ShouldCallCreateTables() throws Exception {
    // Act
    SchemaLoader.load(configFilePath, schemaFilePath, Collections.emptyMap(), true);

    // Assert
    verify(operator).createTables(schemaFilePath, Collections.emptyMap());
    verify(operator).createCoordinatorTable(Collections.emptyMap());
  }

  @Test
  public void
      load_WithProperConfigFilePathAndNullPathSchemaArguments_ShouldJustCallCreateCoordinator()
          throws Exception {
    // Act
    SchemaLoader.load(configFilePath, (Path) null, Collections.emptyMap(), true);

    // Assert
    verify(operator, times(0)).createTables(schemaFilePath, Collections.emptyMap());
    verify(operator).createCoordinatorTable(Collections.emptyMap());
  }

  @Test
  public void
      load_WithProperConfigFilePathAndNullStringSchemaArguments_ShouldJustCallCreateCoordinator()
          throws Exception {
    // Act
    SchemaLoader.load(configFilePath, (String) null, Collections.emptyMap(), true);

    // Assert
    verify(operator, times(0)).createTables(schemaFilePath, Collections.emptyMap());
    verify(operator).createCoordinatorTable(Collections.emptyMap());
  }

  @Test
  public void load_WithProperPropertiesAndFilePathArguments_ShouldCallCreateTables()
      throws Exception {
    // Act
    SchemaLoader.load(configProperties, schemaFilePath, Collections.emptyMap(), true);

    // Assert
    verify(operator).createTables(schemaFilePath, Collections.emptyMap());
    verify(operator).createCoordinatorTable(Collections.emptyMap());
  }

  @Test
  public void
      load_WithProperPropertiesConfigAndNullPathSchemaArguments_ShouldJustCallCreateCoordinator()
          throws Exception {
    // Act
    SchemaLoader.load(configProperties, (Path) null, Collections.emptyMap(), true);

    // Assert
    verify(operator, times(0)).createTables(schemaFilePath, Collections.emptyMap());
    verify(operator).createCoordinatorTable(Collections.emptyMap());
  }

  @Test
  public void
      load_WithProperPropertiesConfigAndNullStringSchemaArguments_ShouldJustCallCreateCoordinator()
          throws Exception {
    // Act
    SchemaLoader.load(configProperties, (String) null, Collections.emptyMap(), true);

    // Assert
    verify(operator, times(0)).createTables(schemaFilePath, Collections.emptyMap());
    verify(operator).createCoordinatorTable(Collections.emptyMap());
  }

  @Test
  public void load_WithProperFilePathAndJsonSchemaArguments_ShouldCallCreateTables()
      throws Exception {
    // Arrange
    String schema =
        "{\n"
            + "  \"sample_db.sample_table\": {\n"
            + "    \"transaction\": false,\n"
            + "    \"partition-key\": [\n"
            + "      \"c1\"\n"
            + "    ],\n"
            + "    \"clustering-key\": [],\n"
            + "    \"columns\": {\n"
            + "      \"c1\": \"INT\",\n"
            + "      \"c2\": \"TEXT\",\n"
            + "    }"
            + "}";

    // Act
    SchemaLoader.load(configFilePath, schema, Collections.emptyMap(), true);

    // Assert
    verify(operator).createTables(schema, Collections.emptyMap());
    verify(operator).createCoordinatorTable(Collections.emptyMap());
  }

  @Test
  public void load_WithProperPropertiesAndJsonSchemaArguments_ShouldCallCreateTables()
      throws Exception {
    // Arrange
    String schema =
        "{\n"
            + "  \"sample_db.sample_table\": {\n"
            + "    \"transaction\": false,\n"
            + "    \"partition-key\": [\n"
            + "      \"c1\"\n"
            + "    ],\n"
            + "    \"clustering-key\": [],\n"
            + "    \"columns\": {\n"
            + "      \"c1\": \"INT\",\n"
            + "      \"c2\": \"TEXT\",\n"
            + "    }"
            + "}";

    // Act
    SchemaLoader.load(configProperties, schema, Collections.emptyMap(), true);

    // Assert
    verify(operator).createTables(schema, Collections.emptyMap());
    verify(operator).createCoordinatorTable(Collections.emptyMap());
  }

  @Test
  public void unload_WithProperFilePathsArguments_ShouldCallDeleteTables() throws Exception {
    // Act
    SchemaLoader.unload(configFilePath, schemaFilePath, Collections.emptyMap(), true);

    // Assert
    verify(operator).deleteTables(schemaFilePath, Collections.emptyMap());
    verify(operator).dropCoordinatorTable();
  }

  @Test
  public void
      unload_WithProperConfigFilePathAndNullPathSchemaArguments_ShouldJustCallDropCoordinator()
          throws Exception {
    // Act
    SchemaLoader.unload(configFilePath, (Path) null, Collections.emptyMap(), true);

    // Assert
    verify(operator, times(0)).createTables(schemaFilePath, Collections.emptyMap());
    verify(operator).dropCoordinatorTable();
  }

  @Test
  public void
      unload_WithProperConfigFilePathAndNullStringSchemaArguments_ShouldJustCallDropCoordinator()
          throws Exception {
    // Act
    SchemaLoader.unload(configFilePath, (String) null, Collections.emptyMap(), true);

    // Assert
    verify(operator, times(0)).createTables(schemaFilePath, Collections.emptyMap());
    verify(operator).dropCoordinatorTable();
  }

  @Test
  public void unload_WithProperPropertiesAndFilePathArguments_ShouldCallDeleteTables()
      throws Exception {
    // Act
    SchemaLoader.unload(configProperties, schemaFilePath, Collections.emptyMap(), true);

    // Assert
    verify(operator).deleteTables(schemaFilePath, Collections.emptyMap());
    verify(operator).dropCoordinatorTable();
  }

  @Test
  public void
      unload_WithProperConfigPropertiesAndNullPathSchemaArguments_ShouldJustCallDropCoordinator()
          throws Exception {
    // Act
    SchemaLoader.unload(configProperties, (Path) null, Collections.emptyMap(), true);

    // Assert
    verify(operator, times(0)).createTables(schemaFilePath, Collections.emptyMap());
    verify(operator).dropCoordinatorTable();
  }

  @Test
  public void
      unload_WithProperConfigPropertiesAndNullStringSchemaArguments_ShouldJustCallDropCoordinator()
          throws Exception {
    // Act
    SchemaLoader.unload(configProperties, (String) null, Collections.emptyMap(), true);

    // Assert
    verify(operator, times(0)).createTables(schemaFilePath, Collections.emptyMap());
    verify(operator).dropCoordinatorTable();
  }

  @Test
  public void unload_WithProperFilePathAndJsonSchemaArguments_ShouldCallDeleteTables()
      throws Exception {
    // Arrange
    String schema =
        "{\n"
            + "  \"sample_db.sample_table\": {\n"
            + "    \"transaction\": false,\n"
            + "    \"partition-key\": [\n"
            + "      \"c1\"\n"
            + "    ],\n"
            + "    \"clustering-key\": [],\n"
            + "    \"columns\": {\n"
            + "      \"c1\": \"INT\",\n"
            + "      \"c2\": \"TEXT\",\n"
            + "    }"
            + "}";

    // Act
    SchemaLoader.unload(configFilePath, schema, Collections.emptyMap(), true);

    // Assert
    verify(operator).deleteTables(schema, Collections.emptyMap());
    verify(operator).dropCoordinatorTable();
  }

  @Test
  public void unload_WithProperPropertiesAndJsonSchemaArguments_ShouldCallDeleteTables()
      throws Exception {
    // Arrange
    String schema =
        "{\n"
            + "  \"sample_db.sample_table\": {\n"
            + "    \"transaction\": false,\n"
            + "    \"partition-key\": [\n"
            + "      \"c1\"\n"
            + "    ],\n"
            + "    \"clustering-key\": [],\n"
            + "    \"columns\": {\n"
            + "      \"c1\": \"INT\",\n"
            + "      \"c2\": \"TEXT\",\n"
            + "    }"
            + "}";

    // Act
    SchemaLoader.unload(configProperties, schema, Collections.emptyMap(), true);

    // Assert
    verify(operator).deleteTables(schema, Collections.emptyMap());
    verify(operator).dropCoordinatorTable();
  }
}
