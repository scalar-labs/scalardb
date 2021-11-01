package com.scalar.db.schemaloader.command;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.schemaloader.core.SchemaOperatorException;
import com.scalar.db.storage.cassandra.CassandraAdmin;
import com.scalar.db.storage.dynamo.DynamoAdmin;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.mockito.Mockito;
import picocli.CommandLine;
import picocli.CommandLine.ExitCode;

public class SchemaLoaderCommandTest extends CommandTestBase {

  private static final String replicationStrategy = "SimpleStrategy";
  private static final String compactionStrategy = "LCS";
  private static final String replicationFactor = "1";
  private static final String ru = "10";
  private static final Boolean noScaling = true;
  private static final Boolean noBackup = true;
  private static final String schemaFile = "path_to_file";
  private static final String configFile = "path_to_config_file";

  @Override
  public void setUp() throws Exception {
    super.setUp();
    commandLine = new CommandLine(new SchemaLoaderCommand());
    setCommandLineOutput();
  }

  @Test
  public void
      call_WithProperCommandLineArgumentsForCreatingTables_ShouldCallCreateTableWithProperParams()
          throws SchemaOperatorException {
    // Arrange

    Map<String, String> metaOptions =
        ImmutableMap.<String, String>builder()
            .put(CassandraAdmin.REPLICATION_STRATEGY, replicationStrategy)
            .put(CassandraAdmin.COMPACTION_STRATEGY, compactionStrategy)
            .put(CassandraAdmin.REPLICATION_FACTOR, replicationFactor)
            .put(DynamoAdmin.REQUEST_UNIT, ru)
            .put(DynamoAdmin.NO_SCALING, noScaling.toString())
            .put(DynamoAdmin.NO_BACKUP, noBackup.toString())
            .build();

    // Act
    commandLine.execute(
        "--config",
        configFile,
        "--replication-strategy",
        replicationStrategy,
        "--compaction-strategy",
        compactionStrategy,
        "--replication-factor",
        replicationFactor,
        "--ru",
        ru,
        "--no-scaling",
        "--no-backup",
        "-f",
        schemaFile,
        "--coordinator");

    // Assert
    verify(operator).createTables(Mockito.any(), eq(metaOptions));
    verify(operator).createCoordinatorTable(Mockito.any());
  }

  @Test
  public void call_WithInvalidReplicationStrategy_ShouldExitWithErrorCode() {
    // Arrange
    String replicationStrategy = "InvalidStrategy";

    // Act
    int exitCode =
        commandLine.execute(
            "--config",
            configFile,
            "--replication-strategy",
            replicationStrategy,
            "--compaction- strategy",
            compactionStrategy,
            "--replication-factor",
            replicationFactor,
            "-f",
            schemaFile);

    // Assert
    Assertions.assertThat(exitCode).isEqualTo(ExitCode.USAGE);
    Assertions.assertThat(stringWriter.toString())
        .contains("Invalid value for option '--replication-strategy'");
  }

  @Test
  public void call_WithInvalidCompactionStrategy_ShouldExitWithErrorCode() {
    // Arrange
    String compactionStrategy = "INVALID";

    // Act
    int exitCode =
        commandLine.execute(
            "--config",
            configFile,
            "--replication-strategy",
            replicationStrategy,
            "--compaction-strategy",
            compactionStrategy,
            "--replication-factor",
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
      throws SchemaOperatorException {
    // Arrange
    String schemaFile = "path_to_file";
    String configFile = "path_to_config_file";

    // Act
    commandLine.execute("-f", schemaFile, "-D", "--config", configFile);

    // Assert
    verify(operator).deleteTables(Mockito.any());
  }

  @Test
  public void call_WithCoordinatorAndDeleteTable_ShouldCallDropCoordinatorTable()
      throws SchemaOperatorException {
    // Arrange

    // Act
    commandLine.execute("--coordinator", "-D", "-c", configFile);

    // Assert
    verify(operator).dropCoordinatorTable();
  }

  @Test
  public void call_WithDeleteTableButNotHaveCoordinatorArgument_ShouldNotCallDropCoordinatorTable()
      throws SchemaOperatorException {
    // Arrange

    // Act
    commandLine.execute("-c", configFile, "-D");

    // Assert
    verify(operator, never()).dropCoordinatorTable();
  }
}
