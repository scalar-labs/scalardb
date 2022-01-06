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
import com.scalar.db.storage.dynamo.DynamoAdmin;
import com.scalar.db.storage.dynamo.DynamoConfig;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import picocli.CommandLine.ExitCode;

public class DynamoCommandTest extends StorageSpecificCommandTestBase {

  private static final String region = "region";
  private static final String user = "aws_access_key";
  private static final String password = "aws_secret_key";
  private static final String endpointOverride = "endpoint";
  private static final String ru = "10";
  private static final Boolean noScaling = true;
  private static final Boolean noBackup = true;
  private static final String schemaFile = "path_to_file";

  @Override
  protected StorageSpecificCommand getCommand() {
    return new DynamoCommand();
  }

  @Test
  public void
      call_WithProperArgumentsForCreatingTablesWithTransactionalTableSchema_ShouldCallCreateTablesProperly()
          throws SchemaLoaderException {
    // Arrange
    Map<String, String> options =
        ImmutableMap.<String, String>builder()
            .put(DynamoAdmin.REQUEST_UNIT, ru)
            .put(DynamoAdmin.NO_SCALING, noScaling.toString())
            .put(DynamoAdmin.NO_BACKUP, noBackup.toString())
            .build();

    TableSchema tableSchema = mock(TableSchema.class);
    when(tableSchema.isTransactionalTable()).thenReturn(true);
    when(parser.parse()).thenReturn(Collections.singletonList(tableSchema));

    Properties properties = new Properties();
    properties.setProperty(DatabaseConfig.CONTACT_POINTS, region);
    properties.setProperty(DatabaseConfig.USERNAME, user);
    properties.setProperty(DatabaseConfig.PASSWORD, password);
    properties.setProperty(DatabaseConfig.STORAGE, "dynamo");
    properties.setProperty(DynamoConfig.ENDPOINT_OVERRIDE, endpointOverride);

    // Act
    commandLine.execute(
        "-u",
        user,
        "-p",
        password,
        "--region",
        region,
        "--endpoint-override",
        endpointOverride,
        "--no-scaling",
        "--no-backup",
        "-r",
        ru,
        "-f",
        schemaFile);

    // Assert
    verify(command).getSchemaParser(options);
    verify(parser).parse();
    verify(command).getSchemaOperator(properties);
    verify(operator).createTables(anyList());
    verify(operator).createCoordinatorTable(options);
  }

  @Test
  public void
      call_WithProperArgumentsForCreatingTablesWithNonTransactionalTableSchema_ShouldCallCreateTablesProperly()
          throws SchemaLoaderException {
    // Arrange
    Map<String, String> options =
        ImmutableMap.<String, String>builder()
            .put(DynamoAdmin.REQUEST_UNIT, ru)
            .put(DynamoAdmin.NO_SCALING, noScaling.toString())
            .put(DynamoAdmin.NO_BACKUP, noBackup.toString())
            .build();

    TableSchema tableSchema = mock(TableSchema.class);
    when(tableSchema.isTransactionalTable()).thenReturn(false);
    when(parser.parse()).thenReturn(Collections.singletonList(tableSchema));

    Properties properties = new Properties();
    properties.setProperty(DatabaseConfig.CONTACT_POINTS, region);
    properties.setProperty(DatabaseConfig.USERNAME, user);
    properties.setProperty(DatabaseConfig.PASSWORD, password);
    properties.setProperty(DatabaseConfig.STORAGE, "dynamo");
    properties.setProperty(DynamoConfig.ENDPOINT_OVERRIDE, endpointOverride);

    // Act
    commandLine.execute(
        "-u",
        user,
        "-p",
        password,
        "--region",
        region,
        "--endpoint-override",
        endpointOverride,
        "--no-scaling",
        "--no-backup",
        "-r",
        ru,
        "-f",
        schemaFile);

    // Assert
    verify(command).getSchemaParser(options);
    verify(parser).parse();
    verify(command).getSchemaOperator(properties);
    verify(operator).createTables(anyList());
    verify(operator, never()).createCoordinatorTable(options);
  }

  @Test
  public void
      call_WithProperArgumentsForDeletingTablesWithTransactionalTableSchema_ShouldCallDeleteTablesProperly()
          throws SchemaLoaderException {
    // Arrange
    TableSchema tableSchema = mock(TableSchema.class);
    when(tableSchema.isTransactionalTable()).thenReturn(true);
    when(parser.parse()).thenReturn(Collections.singletonList(tableSchema));

    Properties properties = new Properties();
    properties.setProperty(DatabaseConfig.CONTACT_POINTS, region);
    properties.setProperty(DatabaseConfig.USERNAME, user);
    properties.setProperty(DatabaseConfig.PASSWORD, password);
    properties.setProperty(DatabaseConfig.STORAGE, "dynamo");
    properties.setProperty(DynamoConfig.ENDPOINT_OVERRIDE, endpointOverride);

    // Act
    commandLine.execute(
        "-u",
        user,
        "--region",
        region,
        "--endpoint-override",
        endpointOverride,
        "-p",
        password,
        "-f",
        schemaFile,
        "-D");

    // Assert
    verify(command).getSchemaParser(Collections.emptyMap());
    verify(parser).parse();
    verify(command).getSchemaOperator(properties);
    verify(operator).deleteTables(anyList());
    verify(operator).dropCoordinatorTable();
  }

  @Test
  public void
      call_WithProperArgumentsForDeletingTablesWithNonTransactionalTableSchema_ShouldCallDeleteTablesProperly()
          throws SchemaLoaderException {
    // Arrange
    TableSchema tableSchema = mock(TableSchema.class);
    when(tableSchema.isTransactionalTable()).thenReturn(false);
    when(parser.parse()).thenReturn(Collections.singletonList(tableSchema));

    Properties properties = new Properties();
    properties.setProperty(DatabaseConfig.CONTACT_POINTS, region);
    properties.setProperty(DatabaseConfig.USERNAME, user);
    properties.setProperty(DatabaseConfig.PASSWORD, password);
    properties.setProperty(DatabaseConfig.STORAGE, "dynamo");
    properties.setProperty(DynamoConfig.ENDPOINT_OVERRIDE, endpointOverride);

    // Act
    commandLine.execute(
        "-u",
        user,
        "--region",
        region,
        "--endpoint-override",
        endpointOverride,
        "-p",
        password,
        "-f",
        schemaFile,
        "-D");

    // Assert
    verify(command).getSchemaParser(Collections.emptyMap());
    verify(parser).parse();
    verify(command).getSchemaOperator(properties);
    verify(operator).deleteTables(anyList());
    verify(operator, never()).dropCoordinatorTable();
  }

  @Test
  public void call_MissingSchemaFile_ShouldExitWithErrorCode() {
    // Arrange

    // Act
    int exitCode = commandLine.execute("-u", user, "-p", password, "--region", region);

    // Assert
    Assertions.assertThat(exitCode).isEqualTo(ExitCode.USAGE);
    Assertions.assertThat(stringWriter.toString())
        .contains("Missing required option '--schema-file=<schemaFile>'");
  }
}
