package com.scalar.db.schemaloader.command;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.schemaloader.core.SchemaOperatorException;
import com.scalar.db.storage.cosmos.CosmosAdmin;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.mockito.Mockito;
import picocli.CommandLine;
import picocli.CommandLine.ExitCode;

public class CosmosCommandTest extends CommandTestBase {

  private static final String host = "cosmos_uri";
  private static final String password = "cosmos_key";
  private static final String ru = "10";
  private static final Boolean noScaling = true;
  private static final String schemaFile = "path_to_file";

  @Override
  public void setUp() {
    super.setUp();
    commandLine = new CommandLine(new CosmosCommand());
    setCommandLineOutput();
  }

  @Test
  public void
      call_WithProperCommandLineArgumentsForCreatingTables_ShouldCallCreateTableWithProperParams()
          throws SchemaOperatorException {
    // Arrange
    Map<String, String> metaOptions =
        ImmutableMap.<String, String>builder()
            .put(CosmosAdmin.REQUEST_UNIT, ru)
            .put(CosmosAdmin.NO_SCALING, noScaling.toString())
            .build();

    // Act
    commandLine.execute("-h", host, "-p", password, "--no-scaling", "-r", ru, "-f", schemaFile);

    // Assert
    verify(operator).createTables(Mockito.any(), eq(metaOptions));
  }

  @Test
  public void call_WithProperCommandLineArgumentsForDeletingTables_ShouldCallDeleteTables()
      throws SchemaOperatorException {
    // Arrange

    // Act
    commandLine.execute("-h", host, "-p", password, "-f", schemaFile, "-D");

    // Assert
    verify(operator).deleteTables(Mockito.any());
  }

  @Test
  public void call_MissingSchemaFile_ShouldExitWithErrorCode() {
    // Arrange

    // Act
    int exitCode = commandLine.execute("-h", host, "-p", password);

    // Assert
    Assertions.assertThat(exitCode).isEqualTo(ExitCode.USAGE);
    Assertions.assertThat(stringWriter.toString())
        .contains("Missing required option '--schema-file=<schemaFile>'");
  }
}
