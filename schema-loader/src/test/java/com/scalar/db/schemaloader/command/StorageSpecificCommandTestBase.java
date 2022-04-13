package com.scalar.db.schemaloader.command;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import com.scalar.db.schemaloader.SchemaOperator;
import com.scalar.db.schemaloader.SchemaParser;
import java.io.PrintWriter;
import java.io.StringWriter;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import picocli.CommandLine;

public abstract class StorageSpecificCommandTestBase {

  @Mock protected SchemaParser parser;
  @Mock protected SchemaOperator operator;
  protected StorageSpecificCommand command;
  protected CommandLine commandLine;
  protected StringWriter stringWriter;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    command = spy(getCommand());
    doReturn(parser).when(command).getSchemaParser(any());
    doReturn(operator).when(command).getSchemaOperator(any());

    commandLine = new CommandLine(command);
    setCommandLineOutput();
  }

  protected abstract StorageSpecificCommand getCommand();

  private void setCommandLineOutput() {
    stringWriter = new StringWriter();
    PrintWriter printWriter = new PrintWriter(stringWriter);
    commandLine.setOut(printWriter);
    commandLine.setErr(printWriter);
  }
}
