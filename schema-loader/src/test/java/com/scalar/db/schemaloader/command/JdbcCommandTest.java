package com.scalar.db.schemaloader.command;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.schemaloader.SchemaLoaderException;
import com.scalar.db.schemaloader.TableSchema;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import picocli.CommandLine.ExitCode;

public class JdbcCommandTest extends StorageSpecificCommandTestBase {

  private static final String jdbcUrl = "jdbc_url";
  private static final String user = "user";
  private static final String password = "cosmos_key";
  private static final String schemaFile = "path_to_file";

  @Override
  protected StorageSpecificCommand getCommand() {
    return new JdbcCommand();
  }

  @Test
  public void
      call_WithProperArgumentsForCreatingTablesWithTransactionalTableSchema_ShouldCallCreateTableProperly()
          throws SchemaLoaderException {
    // Arrange
    Map<String, String> options = Collections.emptyMap();

    TableSchema tableSchema = mock(TableSchema.class);
    when(tableSchema.isTransactionalTable()).thenReturn(true);
    when(parser.parse()).thenReturn(Collections.singletonList(tableSchema));

    Properties properties = new Properties();
    properties.setProperty(DatabaseConfig.CONTACT_POINTS, jdbcUrl);
    properties.setProperty(DatabaseConfig.USERNAME, user);
    properties.setProperty(DatabaseConfig.PASSWORD, password);
    properties.setProperty(DatabaseConfig.STORAGE, "jdbc");

    // Act
    commandLine.execute("-j", jdbcUrl, "-u", user, "-p", password, "-f", schemaFile);

    // Assert
    verify(command).getSchemaParser(options);
    verify(parser).parse();
    verify(command).getSchemaOperator(properties);
    verify(operator).createTables(anyList());
    verify(operator).createCoordinatorTable(options);
  }

  @Test
  public void
      call_WithProperArgumentsForCreatingTablesWithNonTransactionalTableSchema_ShouldCallCreateTableProperly()
          throws SchemaLoaderException {
    // Arrange
    Map<String, String> options = Collections.emptyMap();

    TableSchema tableSchema = mock(TableSchema.class);
    when(tableSchema.isTransactionalTable()).thenReturn(false);
    when(parser.parse()).thenReturn(Collections.singletonList(tableSchema));

    Properties properties = new Properties();
    properties.setProperty(DatabaseConfig.CONTACT_POINTS, jdbcUrl);
    properties.setProperty(DatabaseConfig.USERNAME, user);
    properties.setProperty(DatabaseConfig.PASSWORD, password);
    properties.setProperty(DatabaseConfig.STORAGE, "jdbc");

    // Act
    commandLine.execute("-j", jdbcUrl, "-u", user, "-p", password, "-f", schemaFile);

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
    properties.setProperty(DatabaseConfig.CONTACT_POINTS, jdbcUrl);
    properties.setProperty(DatabaseConfig.USERNAME, user);
    properties.setProperty(DatabaseConfig.PASSWORD, password);
    properties.setProperty(DatabaseConfig.STORAGE, "jdbc");

    // Act
    commandLine.execute("-j", jdbcUrl, "-u", user, "-p", password, "-f", schemaFile, "-D");

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
    properties.setProperty(DatabaseConfig.CONTACT_POINTS, jdbcUrl);
    properties.setProperty(DatabaseConfig.USERNAME, user);
    properties.setProperty(DatabaseConfig.PASSWORD, password);
    properties.setProperty(DatabaseConfig.STORAGE, "jdbc");

    // Act
    commandLine.execute("-j", jdbcUrl, "-u", user, "-p", password, "-f", schemaFile, "-D");

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
    int exitCode = commandLine.execute("-j", jdbcUrl, "-u", user, "-p", password);

    // Assert
    Assertions.assertThat(exitCode).isEqualTo(ExitCode.USAGE);
    Assertions.assertThat(stringWriter.toString())
        .contains("Missing required option '--schema-file=<schemaFile>'");
  }
}
