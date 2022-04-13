package com.scalar.db.sql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.TwoPhaseCommitTransaction;
import com.scalar.db.api.TwoPhaseCommitTransactionManager;
import com.scalar.db.exception.transaction.CommitConflictException;
import com.scalar.db.exception.transaction.CommitException;
import com.scalar.db.exception.transaction.PreparationConflictException;
import com.scalar.db.exception.transaction.PreparationException;
import com.scalar.db.exception.transaction.RollbackException;
import com.scalar.db.exception.transaction.TransactionException;
import com.scalar.db.exception.transaction.ValidationConflictException;
import com.scalar.db.exception.transaction.ValidationException;
import com.scalar.db.sql.exception.SqlException;
import com.scalar.db.sql.exception.TransactionConflictException;
import com.scalar.db.sql.exception.UnknownTransactionStatusException;
import com.scalar.db.sql.metadata.Metadata;
import com.scalar.db.sql.statement.CreateTableStatement;
import com.scalar.db.sql.statement.SelectStatement;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class TwoPhaseCommitTransactionSessionTest {

  @Mock private TwoPhaseCommitTransactionManager manager;
  @Mock private Metadata metadata;
  @Mock private StatementValidator statementValidator;
  @Mock private DmlStatementExecutor dmlStatementExecutor;
  @Mock private DdlStatementExecutor ddlStatementExecutor;
  @Mock private TwoPhaseCommitTransaction transaction;

  private TwoPhaseCommitTransactionSession twoPhaseCommitTransactionSession;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    twoPhaseCommitTransactionSession =
        new TwoPhaseCommitTransactionSession(
            manager, metadata, statementValidator, dmlStatementExecutor, ddlStatementExecutor);
  }

  @Test
  public void begin_CalledOnce_ShouldNotThrowAnyException() throws TransactionException {
    // Arrange
    when(manager.start()).thenReturn(transaction);

    // Act Assert
    assertThatCode(() -> twoPhaseCommitTransactionSession.begin()).doesNotThrowAnyException();
    verify(manager).start();
  }

  @Test
  public void begin_CalledTwice_ShouldThrowIllegalStateException() throws TransactionException {
    // Arrange
    when(manager.start()).thenReturn(transaction);

    // Act Assert
    twoPhaseCommitTransactionSession.begin();
    assertThatThrownBy(() -> twoPhaseCommitTransactionSession.begin())
        .isInstanceOf(IllegalStateException.class);
    verify(manager).start();
  }

  @Test
  public void begin_WhenManagerThrowsTransactionException_ShouldThrowSqlException()
      throws TransactionException {
    // Arrange
    when(manager.start()).thenThrow(TransactionException.class);

    // Act Assert
    assertThatThrownBy(() -> twoPhaseCommitTransactionSession.begin())
        .isInstanceOf(SqlException.class);
    verify(manager).start();
  }

  @Test
  public void join_CalledOnce_ShouldNotThrowAnyException() throws TransactionException {
    // Arrange
    when(manager.join("txId")).thenReturn(transaction);

    // Act Assert
    assertThatCode(() -> twoPhaseCommitTransactionSession.join("txId")).doesNotThrowAnyException();
    verify(manager).join("txId");
  }

  @Test
  public void join_CalledTwice_ShouldIllegalStateException() throws TransactionException {
    // Arrange
    when(manager.join("txId")).thenReturn(transaction);

    // Act Assert
    twoPhaseCommitTransactionSession.join("txId");
    assertThatThrownBy(() -> twoPhaseCommitTransactionSession.join("txId"))
        .isInstanceOf(IllegalStateException.class);
    verify(manager).join("txId");
  }

  @Test
  public void join_WhenManagerThrowsTransactionException_ShouldThrowSqlException()
      throws TransactionException {
    // Arrange
    when(manager.join("txId")).thenThrow(TransactionException.class);

    // Act Assert
    assertThatThrownBy(() -> twoPhaseCommitTransactionSession.join("txId"))
        .isInstanceOf(SqlException.class);
    verify(manager).join("txId");
  }

  @Test
  public void resume_CalledOnce_ShouldNotThrowAnyException() throws TransactionException {
    // Arrange
    when(manager.resume("txId")).thenReturn(transaction);

    // Act Assert
    assertThatCode(() -> twoPhaseCommitTransactionSession.resume("txId"))
        .doesNotThrowAnyException();
    verify(manager).resume("txId");
  }

  @Test
  public void resume_CalledTwice_ShouldIllegalStateException() throws TransactionException {
    // Arrange
    when(manager.resume("txId")).thenReturn(transaction);

    // Act Assert
    twoPhaseCommitTransactionSession.resume("txId");
    assertThatThrownBy(() -> twoPhaseCommitTransactionSession.resume("txId"))
        .isInstanceOf(IllegalStateException.class);
    verify(manager).resume("txId");
  }

  @Test
  public void resume_WhenManagerThrowsTransactionException_ShouldThrowSqlException()
      throws TransactionException {
    // Arrange
    when(manager.resume("txId")).thenThrow(TransactionException.class);

    // Act Assert
    assertThatThrownBy(() -> twoPhaseCommitTransactionSession.resume("txId"))
        .isInstanceOf(SqlException.class);
    verify(manager).resume("txId");
  }

  @Test
  public void execute_DdlStatementGiven_ShouldNotThrowAnyException() {
    // Arrange
    CreateTableStatement ddlStatement = mock(CreateTableStatement.class);

    // Act Assert
    ResultSet actual = twoPhaseCommitTransactionSession.execute(ddlStatement);

    verify(statementValidator).validate(ddlStatement);
    verify(ddlStatementExecutor).execute(ddlStatement);
    assertThat(actual).isEqualTo(EmptyResultSet.INSTANCE);
  }

  @Test
  public void execute_DdlStatementGivenAfterBeginningTransaction_ShouldThrowIllegalStateException()
      throws TransactionException {
    // Arrange
    CreateTableStatement ddlStatement = mock(CreateTableStatement.class);

    when(manager.start()).thenReturn(transaction);

    // Act Assert
    twoPhaseCommitTransactionSession.begin();
    assertThatThrownBy(() -> twoPhaseCommitTransactionSession.execute(ddlStatement))
        .isInstanceOf(IllegalStateException.class);

    verify(statementValidator, never()).validate(ddlStatement);
    verify(ddlStatementExecutor, never()).execute(ddlStatement);
  }

  @Test
  public void execute_DmlStatementGiven_ShouldNotThrowAnyException() throws TransactionException {
    // Arrange
    SelectStatement dmlStatement = mock(SelectStatement.class);

    when(manager.start()).thenReturn(transaction);

    ResultSet resultSet = mock(ResultSet.class);
    when(dmlStatementExecutor.execute(transaction, dmlStatement)).thenReturn(resultSet);

    // Act Assert
    twoPhaseCommitTransactionSession.begin();
    ResultSet actual = twoPhaseCommitTransactionSession.execute(dmlStatement);

    verify(statementValidator).validate(dmlStatement);
    verify(dmlStatementExecutor).execute(transaction, dmlStatement);
    assertThat(actual).isEqualTo(resultSet);
  }

  @Test
  public void
      execute_DmlStatementGivenBeforeBeginningTransaction_ShouldThrowIllegalStateException() {
    // Arrange
    SelectStatement dmlStatement = mock(SelectStatement.class);

    ResultSet resultSet = mock(ResultSet.class);
    when(dmlStatementExecutor.execute(transaction, dmlStatement)).thenReturn(resultSet);

    // Act Assert
    assertThatThrownBy(() -> twoPhaseCommitTransactionSession.execute(dmlStatement))
        .isInstanceOf(IllegalStateException.class);

    verify(statementValidator, never()).validate(dmlStatement);
    verify(dmlStatementExecutor, never()).execute(transaction, dmlStatement);
  }

  @Test
  public void prepare_ShouldNotThrowAnyException() throws TransactionException {
    // Arrange
    when(manager.start()).thenReturn(transaction);

    // Act Assert
    twoPhaseCommitTransactionSession.begin();
    assertThatCode(() -> twoPhaseCommitTransactionSession.prepare()).doesNotThrowAnyException();

    verify(transaction).prepare();
  }

  @Test
  public void prepare_CalledBeforeBeginningTransaction_ShouldThrowIllegalStateException()
      throws TransactionException {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> twoPhaseCommitTransactionSession.prepare())
        .isInstanceOf(IllegalStateException.class);

    verify(transaction, never()).prepare();
  }

  @Test
  public void
      prepare_WhenTransactionThrowsPreparationConflictException_ShouldThrowTransactionConflictException()
          throws TransactionException {
    // Arrange
    when(manager.start()).thenReturn(transaction);
    doThrow(PreparationConflictException.class).when(transaction).prepare();

    // Act Assert
    twoPhaseCommitTransactionSession.begin();
    assertThatThrownBy(() -> twoPhaseCommitTransactionSession.prepare())
        .isInstanceOf(TransactionConflictException.class);

    verify(transaction).prepare();
  }

  @Test
  public void prepare_WhenTransactionThrowsPreparationException_ShouldThrowSqlException()
      throws TransactionException {
    // Arrange
    when(manager.start()).thenReturn(transaction);
    doThrow(PreparationException.class).when(transaction).prepare();

    // Act Assert
    twoPhaseCommitTransactionSession.begin();
    assertThatThrownBy(() -> twoPhaseCommitTransactionSession.prepare())
        .isInstanceOf(SqlException.class);

    verify(transaction).prepare();
  }

  @Test
  public void validate_ShouldNotThrowAnyException() throws TransactionException {
    // Arrange
    when(manager.start()).thenReturn(transaction);

    // Act Assert
    twoPhaseCommitTransactionSession.begin();
    assertThatCode(() -> twoPhaseCommitTransactionSession.validate()).doesNotThrowAnyException();

    verify(transaction).validate();
  }

  @Test
  public void validate_CalledBeforeBeginningTransaction_ShouldThrowIllegalStateException()
      throws TransactionException {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> twoPhaseCommitTransactionSession.validate())
        .isInstanceOf(IllegalStateException.class);

    verify(transaction, never()).validate();
  }

  @Test
  public void
      validate_WhenTransactionThrowsValidationConflictException_ShouldThrowTransactionConflictException()
          throws TransactionException {
    // Arrange
    when(manager.start()).thenReturn(transaction);
    doThrow(ValidationConflictException.class).when(transaction).validate();

    // Act Assert
    twoPhaseCommitTransactionSession.begin();
    assertThatThrownBy(() -> twoPhaseCommitTransactionSession.validate())
        .isInstanceOf(TransactionConflictException.class);

    verify(transaction).validate();
  }

  @Test
  public void validate_WhenTransactionThrowsValidationException_ShouldThrowSqlException()
      throws TransactionException {
    // Arrange
    when(manager.start()).thenReturn(transaction);
    doThrow(ValidationException.class).when(transaction).validate();

    // Act Assert
    twoPhaseCommitTransactionSession.begin();
    assertThatThrownBy(() -> twoPhaseCommitTransactionSession.validate())
        .isInstanceOf(SqlException.class);

    verify(transaction).validate();
  }

  @Test
  public void commit_ShouldNotThrowAnyException() throws TransactionException {
    // Arrange
    when(manager.start()).thenReturn(transaction);

    // Act Assert
    twoPhaseCommitTransactionSession.begin();
    assertThatCode(() -> twoPhaseCommitTransactionSession.commit()).doesNotThrowAnyException();

    verify(transaction).commit();
  }

  @Test
  public void commit_CalledBeforeBeginningTransaction_ShouldThrowIllegalStateException()
      throws TransactionException {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> twoPhaseCommitTransactionSession.commit())
        .isInstanceOf(IllegalStateException.class);

    verify(transaction, never()).commit();
  }

  @Test
  public void
      commit_WhenTransactionThrowsCommitConflictException_ShouldThrowTransactionConflictException()
          throws TransactionException {
    // Arrange
    when(manager.start()).thenReturn(transaction);
    doThrow(CommitConflictException.class).when(transaction).commit();

    // Act Assert
    twoPhaseCommitTransactionSession.begin();
    assertThatThrownBy(() -> twoPhaseCommitTransactionSession.commit())
        .isInstanceOf(TransactionConflictException.class);

    verify(transaction).commit();
  }

  @Test
  public void commit_WhenTransactionThrowsCommitException_ShouldThrowSqlException()
      throws TransactionException {
    // Arrange
    when(manager.start()).thenReturn(transaction);
    doThrow(CommitException.class).when(transaction).commit();

    // Act Assert
    twoPhaseCommitTransactionSession.begin();
    assertThatThrownBy(() -> twoPhaseCommitTransactionSession.commit())
        .isInstanceOf(SqlException.class);

    verify(transaction).commit();
  }

  @Test
  public void
      commit_WhenTransactionThrowsUnknownTransactionStatusException_ShouldThrowUnknownTransactionStatusException()
          throws TransactionException {
    // Arrange
    when(manager.start()).thenReturn(transaction);
    doThrow(com.scalar.db.exception.transaction.UnknownTransactionStatusException.class)
        .when(transaction)
        .commit();

    // Act Assert
    twoPhaseCommitTransactionSession.begin();
    assertThatThrownBy(() -> twoPhaseCommitTransactionSession.commit())
        .isInstanceOf(UnknownTransactionStatusException.class);

    verify(transaction).commit();
  }

  @Test
  public void rollback_ShouldNotThrowAnyException() throws TransactionException {
    // Arrange
    when(manager.start()).thenReturn(transaction);

    // Act Assert
    twoPhaseCommitTransactionSession.begin();
    assertThatCode(() -> twoPhaseCommitTransactionSession.rollback()).doesNotThrowAnyException();

    verify(transaction).rollback();
  }

  @Test
  public void rollback_CalledBeforeBeginningTransaction_ShouldThrowIllegalStateException()
      throws TransactionException {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> twoPhaseCommitTransactionSession.rollback())
        .isInstanceOf(IllegalStateException.class);

    verify(transaction, never()).rollback();
  }

  @Test
  public void rollback_WhenTransactionThrowsRollbackException_ShouldThrowSqlException()
      throws TransactionException {
    // Arrange
    when(manager.start()).thenReturn(transaction);
    doThrow(RollbackException.class).when(transaction).rollback();

    // Act Assert
    twoPhaseCommitTransactionSession.begin();
    assertThatThrownBy(() -> twoPhaseCommitTransactionSession.rollback())
        .isInstanceOf(SqlException.class);

    verify(transaction).rollback();
  }

  @Test
  public void getTransactionId_ShouldNotThrowAnyException() throws TransactionException {
    // Arrange
    when(manager.start()).thenReturn(transaction);
    when(transaction.getId()).thenReturn("txId");

    // Act
    twoPhaseCommitTransactionSession.begin();
    String transactionId = twoPhaseCommitTransactionSession.getTransactionId();

    // Assert
    assertThat(transactionId).isEqualTo("txId");
  }

  @Test
  public void getTransactionId_CalledBeforeBeginningTransaction_ShouldThrowIllegalStateException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> twoPhaseCommitTransactionSession.getTransactionId())
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void getMetadata_ShouldNotThrowAnyException() throws TransactionException {
    // Arrange
    when(manager.start()).thenReturn(transaction);

    // Act Assert
    assertThatCode(() -> twoPhaseCommitTransactionSession.getMetadata()).doesNotThrowAnyException();
  }

  @Test
  public void getMetadata_CalledAfterBeginningTransaction_ShouldThrowIllegalStateException()
      throws TransactionException {
    // Arrange
    when(manager.start()).thenReturn(transaction);

    // Act Assert
    twoPhaseCommitTransactionSession.begin();
    assertThatThrownBy(() -> twoPhaseCommitTransactionSession.getMetadata())
        .isInstanceOf(IllegalStateException.class);
  }
}
