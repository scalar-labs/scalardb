package com.scalar.db.schemaloader.command;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.storage.cassandra.CassandraAdmin;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.mockito.Mockito;
import picocli.CommandLine;
import picocli.CommandLine.ExitCode;

public class CassandraCommandTest extends CommandTestBase {

  private static final String host = "cassandra";
  private static final String port = "9042";
  private static final String user = "user";
  private static final String password = "password";
  private static final String replicationStrategy = "SimpleStrategy";
  private static final String compactionStrategy = "LCS";
  private static final String replicationFactor = "1";
  private static final String schemaFile = "path_to_file";

  @Override
  public void setUp() throws Exception {
    super.setUp();
    commandLine = new CommandLine(new CassandraCommand());
    setCommandLineOutput();
  }

  @Test
  public void
      call_WithProperCommandLineArgumentsForCreatingTables_ShouldCallCreateTableWithProperParams()
          throws ExecutionException {
    // Arrange
    Map<String, String> metaOptions =
        ImmutableMap.<String, String>builder()
            .put(CassandraAdmin.REPLICATION_STRATEGY, replicationStrategy)
            .put(CassandraAdmin.COMPACTION_STRATEGY, compactionStrategy)
            .put(CassandraAdmin.REPLICATION_FACTOR, replicationFactor)
            .build();

    // Act
    commandLine.execute(
        "-h",
        host,
        "-P",
        port,
        "-u",
        user,
        "-p",
        password,
        "-n",
        replicationStrategy,
        "-c",
        compactionStrategy,
        "-R",
        replicationFactor,
        "-f",
        schemaFile);

    // Assert
    verify(operator).createTables(Mockito.any(), eq(metaOptions));
  }

  @Test
  public void call_WithInvalidReplicationStrategy_ShouldExitWithErrorCode() {
    // Arrange
    String replicationStrategy = "InvalidStrategy";

    // Act
    int exitCode =
        commandLine.execute(
            "-h",
            host,
            "-P",
            port,
            "-u",
            user,
            "-p",
            password,
            "-n",
            replicationStrategy,
            "-c",
            compactionStrategy,
            "-R",
            replicationFactor,
            "-f",
            schemaFile);

    // Assert
    Assertions.assertThat(exitCode).isEqualTo(ExitCode.USAGE);
    Assertions.assertThat(stringWriter.toString())
        .contains("Invalid value for option '--network-strategy'");
  }

  @Test
  public void call_WithInvalidCompactionStrategy_ShouldExitWithErrorCode() {
    // Arrange
    String compactionStrategy = "INVALID";

    // Act
    int exitCode =
        commandLine.execute(
            "-h",
            host,
            "-P",
            port,
            "-u",
            user,
            "-p",
            password,
            "-n",
            replicationStrategy,
            "-c",
            compactionStrategy,
            "-R",
            replicationFactor,
            "-f",
            schemaFile);

    // Assert
    Assertions.assertThat(exitCode).isEqualTo(ExitCode.USAGE);
    Assertions.assertThat(stringWriter.toString())
        .contains("Invalid value for option '--compaction-strategy'");
  }

  @Test
  public void call_WithProperCommandLineArgumentsForDeletingTables_ShouldCallDeleteTables()
      throws ExecutionException {
    // Arrange

    // Act
    commandLine.execute("-h", host, "-P", port, "-u", user, "-p", password, "-f", schemaFile, "-D");

    // Assert
    verify(operator).deleteTables(Mockito.any());
  }

  @Test
  public void call_MissingSchemaFile_ShouldExitWithErrorCode() {
    // Arrange

    // Act
    int exitCode = commandLine.execute("-h", host, "-P", port, "-u", user, "-p", password);

    // Assert
    Assertions.assertThat(exitCode).isEqualTo(ExitCode.USAGE);
    Assertions.assertThat(stringWriter.toString())
        .contains("Missing required option '--schema-file=<schemaFile>'");
  }
}
