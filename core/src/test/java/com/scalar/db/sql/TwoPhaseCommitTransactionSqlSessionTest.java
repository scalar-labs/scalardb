package com.scalar.db.sql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.scalar.db.api.DistributedTransactionAdmin;
import com.scalar.db.api.TwoPhaseCommitTransaction;
import com.scalar.db.api.TwoPhaseCommitTransactionManager;
import com.scalar.db.common.TableMetadataManager;
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
import com.scalar.db.sql.statement.CreateTableStatement;
import com.scalar.db.sql.statement.SelectStatement;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class TwoPhaseCommitTransactionSqlSessionTest {

  @Mock private DistributedTransactionAdmin admin;
  @Mock private TwoPhaseCommitTransactionManager manager;
  @Mock private TableMetadataManager tableMetadataManager;
  @Mock private StatementValidator statementValidator;
  @Mock private DdlStatementExecutor ddlStatementExecutor;
  @Mock private TwoPhaseCommitTransaction transaction;

  private TwoPhaseCommitTransactionSqlSession twoPhaseCommitTransactionSqlSession;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    // Arrange
    twoPhaseCommitTransactionSqlSession =
        spy(
            new TwoPhaseCommitTransactionSqlSession(
                admin,
                manager,
                null,
                tableMetadataManager,
                statementValidator,
                ddlStatementExecutor));
  }

  @Test
  public void beginTransaction_CalledOnce_ShouldNotThrowAnyException() throws TransactionException {
    // Arrange
    when(manager.start()).thenReturn(transaction);

    // Act Assert
    assertThatCode(() -> twoPhaseCommitTransactionSqlSession.beginTransaction())
        .doesNotThrowAnyException();
    verify(manager).start();
  }

  @Test
  public void beginTransaction_CalledTwice_ShouldThrowIllegalStateException()
      throws TransactionException {
    // Arrange
    when(manager.start()).thenReturn(transaction);

    // Act Assert
    twoPhaseCommitTransactionSqlSession.beginTransaction();
    assertThatThrownBy(() -> twoPhaseCommitTransactionSqlSession.beginTransaction())
        .isInstanceOf(IllegalStateException.class);
    verify(manager).start();
  }

  @Test
  public void beginTransaction_WhenManagerThrowsTransactionException_ShouldThrowSqlException()
      throws TransactionException {
    // Arrange
    when(manager.start()).thenThrow(TransactionException.class);

    // Act Assert
    assertThatThrownBy(() -> twoPhaseCommitTransactionSqlSession.beginTransaction())
        .isInstanceOf(SqlException.class);
    verify(manager).start();
  }

  @Test
  public void joinTransaction_CalledOnce_ShouldNotThrowAnyException() throws TransactionException {
    // Arrange
    when(manager.join("txId")).thenReturn(transaction);

    // Act Assert
    assertThatCode(() -> twoPhaseCommitTransactionSqlSession.joinTransaction("txId"))
        .doesNotThrowAnyException();
    verify(manager).join("txId");
  }

  @Test
  public void joinTransaction_CalledTwice_ShouldIllegalStateException()
      throws TransactionException {
    // Arrange
    when(manager.join("txId")).thenReturn(transaction);

    // Act Assert
    twoPhaseCommitTransactionSqlSession.joinTransaction("txId");
    assertThatThrownBy(() -> twoPhaseCommitTransactionSqlSession.joinTransaction("txId"))
        .isInstanceOf(IllegalStateException.class);
    verify(manager).join("txId");
  }

  @Test
  public void joinTransaction_WhenManagerThrowsTransactionException_ShouldThrowSqlException()
      throws TransactionException {
    // Arrange
    when(manager.join("txId")).thenThrow(TransactionException.class);

    // Act Assert
    assertThatThrownBy(() -> twoPhaseCommitTransactionSqlSession.joinTransaction("txId"))
        .isInstanceOf(SqlException.class);
    verify(manager).join("txId");
  }

  @Test
  public void execute_DdlStatementGiven_ShouldNotThrowAnyException() {
    // Arrange
    CreateTableStatement ddlStatement = mock(CreateTableStatement.class);

    // Act Assert
    assertThatCode(() -> twoPhaseCommitTransactionSqlSession.execute(ddlStatement))
        .doesNotThrowAnyException();

    verify(statementValidator).validate(ddlStatement);
    verify(ddlStatementExecutor).execute(ddlStatement);
    assertThat(twoPhaseCommitTransactionSqlSession.getResultSet())
        .isEqualTo(EmptyResultSet.INSTANCE);
  }

  @Test
  public void execute_DdlStatementGivenAfterBeginningTransaction_ShouldThrowIllegalStateException()
      throws TransactionException {
    // Arrange
    CreateTableStatement ddlStatement = mock(CreateTableStatement.class);

    when(manager.start()).thenReturn(transaction);

    // Act Assert
    twoPhaseCommitTransactionSqlSession.beginTransaction();
    assertThatThrownBy(() -> twoPhaseCommitTransactionSqlSession.execute(ddlStatement))
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
    doReturn(resultSet).when(twoPhaseCommitTransactionSqlSession).executeDmlStatement(dmlStatement);

    // Act Assert
    twoPhaseCommitTransactionSqlSession.beginTransaction();
    assertThatCode(() -> twoPhaseCommitTransactionSqlSession.execute(dmlStatement))
        .doesNotThrowAnyException();

    verify(statementValidator).validate(dmlStatement);
    verify(twoPhaseCommitTransactionSqlSession).executeDmlStatement(dmlStatement);
    assertThat(twoPhaseCommitTransactionSqlSession.getResultSet()).isEqualTo(resultSet);
  }

  @Test
  public void
      execute_DmlStatementGivenBeforeBeginningTransaction_ShouldThrowIllegalStateException() {
    // Arrange
    SelectStatement dmlStatement = mock(SelectStatement.class);

    ResultSet resultSet = mock(ResultSet.class);
    doReturn(resultSet).when(twoPhaseCommitTransactionSqlSession).executeDmlStatement(dmlStatement);

    // Act Assert
    assertThatThrownBy(() -> twoPhaseCommitTransactionSqlSession.execute(dmlStatement))
        .isInstanceOf(IllegalStateException.class);

    verify(statementValidator, never()).validate(dmlStatement);
    verify(twoPhaseCommitTransactionSqlSession, never()).executeDmlStatement(dmlStatement);
  }

  @Test
  public void getResultSet_BeforeExecutingStatement_ShouldThrowIllegalStateException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> twoPhaseCommitTransactionSqlSession.getResultSet())
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void executeQuery_SelectStatementGiven_ShouldNotThrowAnyException()
      throws TransactionException {
    // Arrange
    SelectStatement selectStatement = mock(SelectStatement.class);

    when(manager.start()).thenReturn(transaction);

    ResultSet resultSet = mock(ResultSet.class);
    doReturn(resultSet)
        .when(twoPhaseCommitTransactionSqlSession)
        .executeDmlStatement(selectStatement);

    // Act Assert
    twoPhaseCommitTransactionSqlSession.beginTransaction();
    ResultSet actual = twoPhaseCommitTransactionSqlSession.executeQuery(selectStatement);

    verify(statementValidator).validate(selectStatement);
    verify(twoPhaseCommitTransactionSqlSession).executeDmlStatement(selectStatement);
    assertThat(actual).isEqualTo(resultSet);
    assertThat(twoPhaseCommitTransactionSqlSession.getResultSet()).isEqualTo(resultSet);
  }

  @Test
  public void
      executeQuery_SelectStatementGivenBeforeBeginningTransaction_ShouldThrowIllegalStateException() {
    // Arrange
    SelectStatement selectStatement = mock(SelectStatement.class);

    ResultSet resultSet = mock(ResultSet.class);
    doReturn(resultSet)
        .when(twoPhaseCommitTransactionSqlSession)
        .executeDmlStatement(selectStatement);

    // Act Assert
    assertThatThrownBy(() -> twoPhaseCommitTransactionSqlSession.executeQuery(selectStatement))
        .isInstanceOf(IllegalStateException.class);

    verify(statementValidator, never()).validate(selectStatement);
    verify(twoPhaseCommitTransactionSqlSession, never()).executeDmlStatement(selectStatement);
  }

  @Test
  public void prepare_ShouldNotThrowAnyException() throws TransactionException {
    // Arrange
    when(manager.start()).thenReturn(transaction);

    // Act Assert
    twoPhaseCommitTransactionSqlSession.beginTransaction();
    assertThatCode(() -> twoPhaseCommitTransactionSqlSession.prepare()).doesNotThrowAnyException();

    verify(transaction).prepare();
  }

  @Test
  public void prepare_CalledBeforeBeginningTransaction_ShouldThrowIllegalStateException()
      throws TransactionException {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> twoPhaseCommitTransactionSqlSession.prepare())
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
    twoPhaseCommitTransactionSqlSession.beginTransaction();
    assertThatThrownBy(() -> twoPhaseCommitTransactionSqlSession.prepare())
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
    twoPhaseCommitTransactionSqlSession.beginTransaction();
    assertThatThrownBy(() -> twoPhaseCommitTransactionSqlSession.prepare())
        .isInstanceOf(SqlException.class);

    verify(transaction).prepare();
  }

  @Test
  public void validate_ShouldNotThrowAnyException() throws TransactionException {
    // Arrange
    when(manager.start()).thenReturn(transaction);

    // Act Assert
    twoPhaseCommitTransactionSqlSession.beginTransaction();
    assertThatCode(() -> twoPhaseCommitTransactionSqlSession.validate()).doesNotThrowAnyException();

    verify(transaction).validate();
  }

  @Test
  public void validate_CalledBeforeBeginningTransaction_ShouldThrowIllegalStateException()
      throws TransactionException {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> twoPhaseCommitTransactionSqlSession.validate())
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
    twoPhaseCommitTransactionSqlSession.beginTransaction();
    assertThatThrownBy(() -> twoPhaseCommitTransactionSqlSession.validate())
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
    twoPhaseCommitTransactionSqlSession.beginTransaction();
    assertThatThrownBy(() -> twoPhaseCommitTransactionSqlSession.validate())
        .isInstanceOf(SqlException.class);

    verify(transaction).validate();
  }

  @Test
  public void commit_ShouldNotThrowAnyException() throws TransactionException {
    // Arrange
    when(manager.start()).thenReturn(transaction);

    // Act Assert
    twoPhaseCommitTransactionSqlSession.beginTransaction();
    assertThatCode(() -> twoPhaseCommitTransactionSqlSession.commit()).doesNotThrowAnyException();

    verify(transaction).commit();
  }

  @Test
  public void commit_CalledBeforeBeginningTransaction_ShouldThrowIllegalStateException()
      throws TransactionException {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> twoPhaseCommitTransactionSqlSession.commit())
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
    twoPhaseCommitTransactionSqlSession.beginTransaction();
    assertThatThrownBy(() -> twoPhaseCommitTransactionSqlSession.commit())
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
    twoPhaseCommitTransactionSqlSession.beginTransaction();
    assertThatThrownBy(() -> twoPhaseCommitTransactionSqlSession.commit())
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
    twoPhaseCommitTransactionSqlSession.beginTransaction();
    assertThatThrownBy(() -> twoPhaseCommitTransactionSqlSession.commit())
        .isInstanceOf(UnknownTransactionStatusException.class);

    verify(transaction).commit();
  }

  @Test
  public void rollback_ShouldNotThrowAnyException() throws TransactionException {
    // Arrange
    when(manager.start()).thenReturn(transaction);

    // Act Assert
    twoPhaseCommitTransactionSqlSession.beginTransaction();
    assertThatCode(() -> twoPhaseCommitTransactionSqlSession.rollback()).doesNotThrowAnyException();

    verify(transaction).rollback();
  }

  @Test
  public void rollback_CalledBeforeBeginningTransaction_ShouldThrowIllegalStateException()
      throws TransactionException {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> twoPhaseCommitTransactionSqlSession.rollback())
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
    twoPhaseCommitTransactionSqlSession.beginTransaction();
    assertThatThrownBy(() -> twoPhaseCommitTransactionSqlSession.rollback())
        .isInstanceOf(SqlException.class);

    verify(transaction).rollback();
  }

  @Test
  public void getTransactionId_ShouldNotThrowAnyException() throws TransactionException {
    // Arrange
    when(manager.start()).thenReturn(transaction);
    when(transaction.getId()).thenReturn("txId");

    // Act
    twoPhaseCommitTransactionSqlSession.beginTransaction();
    String transactionId = twoPhaseCommitTransactionSqlSession.getTransactionId();

    // Assert
    assertThat(transactionId).isEqualTo("txId");
  }

  @Test
  public void getTransactionId_CalledBeforeBeginningTransaction_ShouldThrowIllegalStateException() {
    // Arrange

    // Act Assert
    assertThatThrownBy(() -> twoPhaseCommitTransactionSqlSession.getTransactionId())
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void getMetadata_ShouldNotThrowAnyException() throws TransactionException {
    // Arrange
    when(manager.start()).thenReturn(transaction);

    // Act Assert
    assertThatCode(() -> twoPhaseCommitTransactionSqlSession.getMetadata())
        .doesNotThrowAnyException();
  }

  @Test
  public void getMetadata_CalledAfterBeginningTransaction_ShouldThrowIllegalStateException()
      throws TransactionException {
    // Arrange
    when(manager.start()).thenReturn(transaction);

    // Act Assert
    twoPhaseCommitTransactionSqlSession.beginTransaction();
    assertThatThrownBy(() -> twoPhaseCommitTransactionSqlSession.getMetadata())
        .isInstanceOf(IllegalStateException.class);
  }
}
