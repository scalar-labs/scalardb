package com.scalar.db.storage.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class NamespaceMetadataServiceTest {

  private static final String METADATA_SCHEMA = "scalardb";

  @Test
  public void createNamespacesTableIfNotExists_BinaryClauseGiven_ShouldDeclareCollation()
      throws Exception {
    // Arrange
    RdbEngineStrategy rdbEngine = new RdbEngineMysql();
    Connection connection = mock(Connection.class);
    Statement statement = mock(Statement.class);
    when(connection.createStatement()).thenReturn(statement);
    PreparedStatement existsStatement = mock(PreparedStatement.class);
    ResultSet existsResultSet = mock(ResultSet.class);
    when(existsResultSet.next()).thenReturn(false);
    when(existsStatement.executeQuery()).thenReturn(existsResultSet);
    when(connection.prepareStatement(anyString())).thenReturn(existsStatement);
    NamespaceMetadataService service =
        new NamespaceMetadataService(
            METADATA_SCHEMA, rdbEngine, rdbEngine.getBinaryTextCollationClause(), false);

    // Act
    service.createNamespacesTableIfNotExists(connection);

    // Assert
    ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
    verify(statement).execute(captor.capture());
    assertThat(captor.getValue()).contains("COLLATE utf8mb4_0900_bin");
  }

  @Test
  public void createNamespacesTableIfNotExists_NoCreatedTextCollationClause_ShouldUseBareTextTypes()
      throws Exception {
    // Arrange
    RdbEngineStrategy rdbEngine = new RdbEngineMysql();
    Connection connection = mock(Connection.class);
    Statement statement = mock(Statement.class);
    when(connection.createStatement()).thenReturn(statement);
    PreparedStatement existsStatement = mock(PreparedStatement.class);
    ResultSet existsResultSet = mock(ResultSet.class);
    when(existsResultSet.next()).thenReturn(false);
    when(existsStatement.executeQuery()).thenReturn(existsResultSet);
    when(connection.prepareStatement(anyString())).thenReturn(existsStatement);
    NamespaceMetadataService service =
        new NamespaceMetadataService(METADATA_SCHEMA, rdbEngine, "", false);

    // Act
    service.createNamespacesTableIfNotExists(connection);

    // Assert
    ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
    verify(statement).execute(captor.capture());
    assertThat(captor.getValue()).doesNotContain("COLLATE").contains("VARCHAR(128)");
  }
}
