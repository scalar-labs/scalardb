package com.scalar.db.schemaloader.command;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

import com.scalar.db.schemaloader.core.SchemaOperatorException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.mockito.Mockito;
import picocli.CommandLine;
import picocli.CommandLine.ExitCode;

public class JdbcCommandTest extends CommandTestBase {

  private static final String jdbcUrl = "jdbc_url";
  private static final String user = "user";
  private static final String password = "cosmos_key";
  private static final String schemaFile = "path_to_file";

  @Override
  public void setUp() {
    super.setUp();
    commandLine = new CommandLine(new JdbcCommand());
    setCommandLineOutput();
  }

  @Test
  public void
      call_WithProperCommandLineArgumentsForCreatingTables_ShouldCallCreateTableWithProperParams()
          throws SchemaOperatorException {
    // Arrange

    // Act
    commandLine.execute("-j", jdbcUrl, "-u", user, "-p", password, "-f", schemaFile);

    // Assert
    verify(operator).createTables(Mockito.any(Path.class), eq(Collections.emptyMap()));
  }

  @Test
  public void call_WithProperCommandLineArgumentsForDeletingTables_ShouldCallDeleteTables()
      throws SchemaOperatorException {
    // Arrange

    // Act
    commandLine.execute("-j", jdbcUrl, "-u", user, "-p", password, "-f", schemaFile, "-D");

    // Assert
    verify(operator).deleteTables(Paths.get(schemaFile), Collections.emptyMap());
  }

  @Test
  public void call_MissingSchemaFile_ShouldExitWithErrorCode() {
    // Arrange

    // Act
    int exitCode = commandLine.execute("-j", jdbcUrl, "-u", user, "-p", password);

    // Assert
    Assertions.assertThat(exitCode).isEqualTo(ExitCode.USAGE);
    Assertions.assertThat(stringWriter.toString())
        .contains("Missing required option '--schema-file=<schemaFile>'");
  }
}
