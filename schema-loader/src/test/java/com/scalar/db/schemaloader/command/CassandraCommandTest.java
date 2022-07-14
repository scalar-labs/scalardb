package com.scalar.db.schemaloader.command;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.schemaloader.SchemaLoaderException;
import com.scalar.db.schemaloader.TableSchema;
import com.scalar.db.storage.cassandra.CassandraAdmin;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import picocli.CommandLine.ExitCode;

public class CassandraCommandTest extends StorageSpecificCommandTestBase {

  private static final String host = "cassandra";
  private static final String port = "9042";
  private static final String user = "user";
  private static final String password = "password";
  private static final String replicationStrategy = "SimpleStrategy";
  private static final String compactionStrategy = "LCS";
  private static final String replicationFactor = "1";
  private static final String schemaFile = "path_to_file";

  @Override
  protected StorageSpecificCommand getCommand() {
    return new CassandraCommand();
  }

  @Test
  public void
      call_ProperArgumentsForCreatingTablesGivenWithTransactionTableSchema_ShouldCallCreateTablesProperly()
          throws SchemaLoaderException {
    // Arrange
    Map<String, String> options =
        ImmutableMap.<String, String>builder()
            .put(CassandraAdmin.REPLICATION_STRATEGY, replicationStrategy)
            .put(CassandraAdmin.COMPACTION_STRATEGY, compactionStrategy)
            .put(CassandraAdmin.REPLICATION_FACTOR, replicationFactor)
            .build();

    TableSchema tableSchema = mock(TableSchema.class);
    when(tableSchema.isTransactionTable()).thenReturn(true);
    when(parser.parse()).thenReturn(Collections.singletonList(tableSchema));

    Properties properties = new Properties();
    properties.setProperty(DatabaseConfig.CONTACT_POINTS, host);
    properties.setProperty(DatabaseConfig.CONTACT_PORT, port);
    properties.setProperty(DatabaseConfig.USERNAME, user);
    properties.setProperty(DatabaseConfig.PASSWORD, password);
    properties.setProperty(DatabaseConfig.STORAGE, "cassandra");

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
    verify(command).getSchemaParser(options);
    verify(parser).parse();
    verify(command).getSchemaOperator(properties);
    verify(operator).createTables(anyList());
    verify(operator).createCoordinatorTables(options);
  }

  @Test
  public void
      call_ProperArgumentsForCreatingTablesGivenWithNonTransactionTableSchema_ShouldCallCreateTablesProperly()
          throws SchemaLoaderException {
    // Arrange
    Map<String, String> options =
        ImmutableMap.<String, String>builder()
            .put(CassandraAdmin.REPLICATION_STRATEGY, replicationStrategy)
            .put(CassandraAdmin.COMPACTION_STRATEGY, compactionStrategy)
            .put(CassandraAdmin.REPLICATION_FACTOR, replicationFactor)
            .build();

    TableSchema tableSchema = mock(TableSchema.class);
    when(tableSchema.isTransactionTable()).thenReturn(false);
    when(parser.parse()).thenReturn(Collections.singletonList(tableSchema));

    Properties properties = new Properties();
    properties.setProperty(DatabaseConfig.CONTACT_POINTS, host);
    properties.setProperty(DatabaseConfig.CONTACT_PORT, port);
    properties.setProperty(DatabaseConfig.USERNAME, user);
    properties.setProperty(DatabaseConfig.PASSWORD, password);
    properties.setProperty(DatabaseConfig.STORAGE, "cassandra");

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
    verify(command).getSchemaParser(options);
    verify(parser).parse();
    verify(command).getSchemaOperator(properties);
    verify(operator).createTables(anyList());
    verify(operator, never()).createCoordinatorTables(options);
  }

  @Test
  public void
      call_WithProperArgumentsForDeletingTablesWithTransactionTableSchema_ShouldCallDeleteTablesProperly()
          throws SchemaLoaderException {
    // Arrange
    TableSchema tableSchema = mock(TableSchema.class);
    when(tableSchema.isTransactionTable()).thenReturn(true);
    when(parser.parse()).thenReturn(Collections.singletonList(tableSchema));

    Properties properties = new Properties();
    properties.setProperty(DatabaseConfig.CONTACT_POINTS, host);
    properties.setProperty(DatabaseConfig.CONTACT_PORT, port);
    properties.setProperty(DatabaseConfig.USERNAME, user);
    properties.setProperty(DatabaseConfig.PASSWORD, password);
    properties.setProperty(DatabaseConfig.STORAGE, "cassandra");

    // Act
    commandLine.execute("-h", host, "-P", port, "-u", user, "-p", password, "-f", schemaFile, "-D");

    // Assert
    verify(command).getSchemaParser(Collections.emptyMap());
    verify(parser).parse();
    verify(command).getSchemaOperator(properties);
    verify(operator).deleteTables(anyList());
    verify(operator).dropCoordinatorTables();
  }

  @Test
  public void
      call_WithProperArgumentsForDeletingTablesWithNonTransactionTableSchema_ShouldCallDeleteTablesProperly()
          throws SchemaLoaderException {
    // Arrange
    TableSchema tableSchema = mock(TableSchema.class);
    when(tableSchema.isTransactionTable()).thenReturn(false);
    when(parser.parse()).thenReturn(Collections.singletonList(tableSchema));

    Properties properties = new Properties();
    properties.setProperty(DatabaseConfig.CONTACT_POINTS, host);
    properties.setProperty(DatabaseConfig.CONTACT_PORT, port);
    properties.setProperty(DatabaseConfig.USERNAME, user);
    properties.setProperty(DatabaseConfig.PASSWORD, password);
    properties.setProperty(DatabaseConfig.STORAGE, "cassandra");

    // Act
    commandLine.execute("-h", host, "-P", port, "-u", user, "-p", password, "-f", schemaFile, "-D");

    // Assert
    verify(command).getSchemaParser(Collections.emptyMap());
    verify(parser).parse();
    verify(command).getSchemaOperator(properties);
    verify(operator).deleteTables(anyList());
    verify(operator, never()).dropCoordinatorTables();
  }

  @Test
  public void
      call_ProperArgumentsForRepairingTablesGivenWithNonTransactionTableSchema_ShouldCallRepairTablesProperly()
          throws SchemaLoaderException {
    callProperArgumentsForRepairingTables(false);
  }

  @Test
  public void
      call_ProperArgumentsForRepairingTablesGivenWithTransactionTableSchema_ShouldCallRepairTablesProperly()
          throws SchemaLoaderException {
    callProperArgumentsForRepairingTables(true);
  }

  private void callProperArgumentsForRepairingTables(boolean hasTransactionTables)
      throws SchemaLoaderException {
    // Arrange
    Map<String, String> options = ImmutableMap.of();

    TableSchema tableSchema = mock(TableSchema.class);
    if (hasTransactionTables) {
      when(tableSchema.isTransactionTable()).thenReturn(true);
    } else {
      when(tableSchema.isTransactionTable()).thenReturn(false);
    }
    when(parser.parse()).thenReturn(Collections.singletonList(tableSchema));

    Properties properties = new Properties();
    properties.setProperty(DatabaseConfig.CONTACT_POINTS, host);
    properties.setProperty(DatabaseConfig.CONTACT_PORT, port);
    properties.setProperty(DatabaseConfig.USERNAME, user);
    properties.setProperty(DatabaseConfig.PASSWORD, password);
    properties.setProperty(DatabaseConfig.STORAGE, "cassandra");

    // Act
    commandLine.execute(
        "-h", host, "-P", port, "-u", user, "-p", password, "-f", schemaFile, "--repair-all");

    // Assert
    verify(command).getSchemaParser(options);
    verify(parser).parse();
    verify(command).getSchemaOperator(properties);
    verify(operator).repairTables(anyList());
    if (hasTransactionTables) {
      verify(operator).repairCoordinatorTables(options);
    } else {
      verify(operator, never()).repairCoordinatorTables(options);
    }
  }

  @Test
  public void call_ProperArgumentsForAlteringTables_ShouldCallAlterTablesProperly()
      throws SchemaLoaderException {
    // Arrange
    Map<String, String> options = ImmutableMap.of();
    TableSchema tableSchema = mock(TableSchema.class);
    when(parser.parse()).thenReturn(Collections.singletonList(tableSchema));

    Properties properties = new Properties();
    properties.setProperty(DatabaseConfig.CONTACT_POINTS, host);
    properties.setProperty(DatabaseConfig.CONTACT_PORT, port);
    properties.setProperty(DatabaseConfig.USERNAME, user);
    properties.setProperty(DatabaseConfig.PASSWORD, password);
    properties.setProperty(DatabaseConfig.STORAGE, "cassandra");

    // Act
    commandLine.execute(
        "-h", host, "-P", port, "-u", user, "-p", password, "-f", schemaFile, "--alter");

    // Assert
    verify(command).getSchemaParser(options);
    verify(parser).parse();
    verify(command).getSchemaOperator(properties);
    verify(operator).alterTables(Collections.singletonList(tableSchema), options);
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
