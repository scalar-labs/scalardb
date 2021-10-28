package com.scalar.db.schemaloader.command;

import com.scalar.db.schemaloader.core.SchemaOperator;
import com.scalar.db.schemaloader.schema.SchemaParser;
import java.io.PrintWriter;
import java.io.StringWriter;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;
import picocli.CommandLine;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
public abstract class CommandTestBase {
  @Mock protected SchemaOperator operator;
  @Mock protected SchemaParser schemaParser;
  protected CommandLine commandLine;
  protected StringWriter stringWriter;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);

    PowerMockito.whenNew(SchemaOperator.class).withAnyArguments().thenReturn(operator);
    PowerMockito.whenNew(SchemaParser.class).withAnyArguments().thenReturn(schemaParser);
  }

  protected void setCommandLineOutput() {
    stringWriter = new StringWriter();
    PrintWriter printWriter = new PrintWriter(stringWriter);
    commandLine.setOut(printWriter);
    commandLine.setErr(printWriter);
  }
}
