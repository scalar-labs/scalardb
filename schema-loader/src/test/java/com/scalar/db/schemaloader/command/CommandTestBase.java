package com.scalar.db.schemaloader.command;

import com.scalar.db.schemaloader.core.SchemaOperator;
import com.scalar.db.schemaloader.core.SchemaOperatorFactory;
import com.scalar.db.schemaloader.schema.SchemaParser;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Properties;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import picocli.CommandLine;

public abstract class CommandTestBase {
  @Mock protected SchemaOperator operator;
  protected CommandLine commandLine;
  protected StringWriter stringWriter;

  private AutoCloseable closeable;
  private MockedStatic<SchemaParser> schemaParserMockedStatic;
  private MockedStatic<SchemaOperatorFactory> schemaOperatorFactoryMockedStatic;

  @Before
  public void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
    schemaParserMockedStatic = Mockito.mockStatic(SchemaParser.class);
    schemaParserMockedStatic
        .when(() -> SchemaParser.parse(Mockito.anyString(), Mockito.anyMap()))
        .thenReturn(Collections.emptyList());

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
    schemaParserMockedStatic.close();
    schemaOperatorFactoryMockedStatic.close();
    closeable.close();
  }

  protected void setCommandLineOutput() {
    stringWriter = new StringWriter();
    PrintWriter printWriter = new PrintWriter(stringWriter);
    commandLine.setOut(printWriter);
    commandLine.setErr(printWriter);
  }
}
