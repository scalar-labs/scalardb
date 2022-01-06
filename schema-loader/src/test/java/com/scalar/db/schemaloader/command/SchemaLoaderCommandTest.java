package com.scalar.db.schemaloader.command;

import static org.mockito.Mockito.mockStatic;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.schemaloader.SchemaLoader;
import com.scalar.db.storage.cassandra.CassandraAdmin;
import com.scalar.db.storage.dynamo.DynamoAdmin;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import picocli.CommandLine;
import picocli.CommandLine.ExitCode;

public class SchemaLoaderCommandTest {

  private static final String replicationStrategy = "SimpleStrategy";
  private static final String compactionStrategy = "LCS";
  private static final String replicationFactor = "1";
  private static final String ru = "10";
  private static final Boolean noScaling = true;
  private static final Boolean noBackup = true;
  private static final String schemaFile = "path_to_file";
  private static final String configFile = "path_to_config_file";

  private AutoCloseable closeable;
  private MockedStatic<SchemaLoader> schemaLoaderMockedStatic;

  private CommandLine commandLine;
  private StringWriter stringWriter;

  @Before
  public void setUp() {
    closeable = MockitoAnnotations.openMocks(this);
    schemaLoaderMockedStatic = mockStatic(SchemaLoader.class);
    commandLine = new CommandLine(new SchemaLoaderCommand());

    stringWriter = new StringWriter();
    PrintWriter printWriter = new PrintWriter(stringWriter);
    commandLine.setOut(printWriter);
    commandLine.setErr(printWriter);
  }

  @After
  public void tearDown() throws Exception {
    schemaLoaderMockedStatic.close();
    closeable.close();
  }

  @Test
  public void call_WithProperArgumentsForCreatingTables_ShouldCallLoadProperly() {
    // Arrange
    Map<String, String> options =
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
    schemaLoaderMockedStatic.verify(
        () -> SchemaLoader.load(Paths.get(configFile), Paths.get(schemaFile), options, true));
  }

  @Test
  public void
      call_WithProperArgumentsForCreatingTablesWithoutCoordinatorArgument_ShouldCallLoadProperly() {
    // Arrange
    Map<String, String> options =
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
        schemaFile);

    // Assert
    schemaLoaderMockedStatic.verify(
        () -> SchemaLoader.load(Paths.get(configFile), Paths.get(schemaFile), options, false));
  }

  @Test
  public void
      call_WithProperArgumentsForCreatingTablesWithoutSchemaFileArgument_ShouldCallLoadProperly() {
    // Arrange
    Map<String, String> options =
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
        "--coordinator");

    // Assert
    schemaLoaderMockedStatic.verify(
        () -> SchemaLoader.load(Paths.get(configFile), (Path) null, options, true));
  }

  @Test
  public void call_WithProperArgumentsForDeletingTables_ShouldCallUnloadProperly() {
    // Arrange
    String schemaFile = "path_to_file";
    String configFile = "path_to_config_file";

    // Act
    commandLine.execute("-f", schemaFile, "-D", "--config", configFile, "--coordinator");

    // Assert
    schemaLoaderMockedStatic.verify(
        () -> SchemaLoader.unload(Paths.get(configFile), Paths.get(schemaFile), true));
  }

  @Test
  public void
      call_WithProperArgumentsForDeletingTablesWithoutCoordinatorArgument_ShouldCallUnloadProperly() {
    // Arrange
    String schemaFile = "path_to_file";
    String configFile = "path_to_config_file";

    // Act
    commandLine.execute("-f", schemaFile, "-D", "--config", configFile);

    // Assert
    schemaLoaderMockedStatic.verify(
        () -> SchemaLoader.unload(Paths.get(configFile), Paths.get(schemaFile), false));
  }

  @Test
  public void
      call_WithProperArgumentsForDeletingTablesWithoutSchemaFileArgument_ShouldCallUnloadProperly() {
    // Arrange
    String configFile = "path_to_config_file";

    // Act
    commandLine.execute("-D", "--config", configFile, "--coordinator");

    // Assert
    schemaLoaderMockedStatic.verify(
        () -> SchemaLoader.unload(Paths.get(configFile), (Path) null, true));
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
}
