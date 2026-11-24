package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.Statement;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class TableMetadataServiceTest {

  private static final String METADATA_SCHEMA = "scalardb";

  @Test
  public void createTableMetadataTableIfNotExists_BinaryClauseGiven_ShouldDeclareCollation()
      throws Exception {
    // Arrange
    RdbEngineStrategy rdbEngine = new RdbEngineMysql();
    Connection connection = mock(Connection.class);
    Statement statement = mock(Statement.class);
    when(connection.createStatement()).thenReturn(statement);
    TableMetadataService service =
        new TableMetadataService(
            METADATA_SCHEMA, rdbEngine, rdbEngine.getBinaryTextCollationClause(), false);

    // Act
    service.createTableMetadataTableIfNotExists(connection);

    // Assert
    ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
    verify(statement).execute(captor.capture());
    assertThat(captor.getValue()).contains("COLLATE utf8mb4_0900_bin");
  }

  @Test
  public void
      createTableMetadataTableIfNotExists_NoCreatedTextCollationClause_ShouldUseBareTextTypes()
          throws Exception {
    // Arrange
    RdbEngineStrategy rdbEngine = new RdbEngineMysql();
    Connection connection = mock(Connection.class);
    Statement statement = mock(Statement.class);
    when(connection.createStatement()).thenReturn(statement);
    TableMetadataService service = new TableMetadataService(METADATA_SCHEMA, rdbEngine, "", false);

    // Act
    service.createTableMetadataTableIfNotExists(connection);

    // Assert
    ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
    verify(statement).execute(captor.capture());
    assertThat(captor.getValue()).doesNotContain("COLLATE").contains("VARCHAR(128)");
  }
}
