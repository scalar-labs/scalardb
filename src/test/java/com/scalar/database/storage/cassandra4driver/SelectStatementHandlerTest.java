package com.scalar.database.storage.cassandra4driver;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.DriverException;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.google.common.base.Joiner;
import com.scalar.database.api.Consistency;
import com.scalar.database.api.Get;
import com.scalar.database.api.Operation;
import com.scalar.database.api.Put;
import com.scalar.database.api.Scan;
import com.scalar.database.exception.storage.ExecutionException;
import com.scalar.database.io.Key;
import com.scalar.database.io.TextValue;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

/** */
public class SelectStatementHandlerTest {
  private static final String ANY_KEYSPACE_NAME = "ks";
  private static final String ANY_TABLE_NAME = "tbl";
  private static final String ANY_NAME_1 = "name1";
  private static final String ANY_NAME_2 = "name2";
  private static final String ANY_NAME_3 = "name3";
  private static final String ANY_TEXT_1 = "text1";
  private static final String ANY_TEXT_2 = "text2";
  private static final String ANY_TEXT_3 = "text3";
  private static final String ANY_TEXT_4 = "text4";
  private static final Scan.Ordering.Order ASC_ORDER = Scan.Ordering.Order.ASC;
  private static final Scan.Ordering.Order DESC_ORDER = Scan.Ordering.Order.DESC;
  private static final int ANY_LIMIT = 100;
  private SelectStatementHandler handler;
  private Get get;
  private Scan scan;
  @Mock private CqlSession session;
  @Mock private PreparedStatement prepared;
  @Mock private BoundStatementBuilder builder;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);

    handler = new SelectStatementHandler(session);
  }

  private Get prepareGet() {
    Key partitionKey = new Key(new TextValue(ANY_NAME_1, ANY_TEXT_1));
    return new Get(partitionKey).forNamespace(ANY_KEYSPACE_NAME).forTable(ANY_TABLE_NAME);
  }

  private Get prepareGetWithClusteringKey() {
    Key partitionKey = new Key(new TextValue(ANY_NAME_1, ANY_TEXT_1));
    Key clusteringKey = new Key(new TextValue(ANY_NAME_2, ANY_TEXT_2));
    return new Get(partitionKey, clusteringKey)
        .forNamespace(ANY_KEYSPACE_NAME)
        .forTable(ANY_TABLE_NAME);
  }

  private Scan prepareScan() {
    Key partitionKey = new Key(new TextValue(ANY_NAME_1, ANY_TEXT_1));
    return new Scan(partitionKey).forNamespace(ANY_KEYSPACE_NAME).forTable(ANY_TABLE_NAME);
  }

  private void configureBehavior(String expected) {
    when(session.prepare(expected == null ? anyString() : expected)).thenReturn(prepared);

    when(prepared.boundStatementBuilder()).thenReturn(builder);
    when(builder.setString(anyInt(), anyString())).thenReturn(builder);
  }

  @Test
  public void prepare_GetOperationWithoutClusteringKeyGiven_ShouldPrepareProperQuery() {
    // Arrange
    String expected =
        Joiner.on(" ")
            .skipNulls()
            .join(
                new String[] {
                  "SELECT * FROM",
                  ANY_KEYSPACE_NAME + "." + ANY_TABLE_NAME,
                  "WHERE",
                  ANY_NAME_1 + "=?",
                });
    configureBehavior(expected);
    get = prepareGet();

    // Act
    handler.prepare(get);

    // Assert
    verify(session).prepare(expected);
  }

  @Test
  public void prepare_GetOperationWithAllProjectionGiven_ShouldPrepareProperQuery() {
    // Arrange
    String expected =
        Joiner.on(" ")
            .skipNulls()
            .join(
                new String[] {
                  "SELECT * FROM",
                  ANY_KEYSPACE_NAME + "." + ANY_TABLE_NAME,
                  "WHERE",
                  ANY_NAME_1 + "=?",
                  "AND",
                  ANY_NAME_2 + "=?",
                });
    configureBehavior(expected);
    get = prepareGetWithClusteringKey();

    // Act
    handler.prepare(get);

    // Assert
    verify(session).prepare(expected);
  }

  @Test
  public void prepare_GetOperationWithSomeProjectionGiven_ShouldPrepareProperQuery() {
    // Arrange
    String expected =
        Joiner.on(" ")
            .skipNulls()
            .join(
                new String[] {
                  "SELECT",
                  ANY_NAME_1,
                  "FROM",
                  ANY_KEYSPACE_NAME + "." + ANY_TABLE_NAME,
                  "WHERE",
                  ANY_NAME_1 + "=?",
                  "AND",
                  ANY_NAME_2 + "=?",
                });
    configureBehavior(expected);
    get = prepareGetWithClusteringKey();
    get.withProjection(ANY_NAME_1);

    // Act
    handler.prepare(get);

    // Assert
    verify(session).prepare(expected);
  }

  @Test
  public void prepare_ScanOperationWithSingleClusteringKey_ShouldPrepareProperQuery() {
    // Arrange
    String expected =
        Joiner.on(" ")
            .skipNulls()
            .join(
                new String[] {
                  "SELECT * FROM",
                  ANY_KEYSPACE_NAME + "." + ANY_TABLE_NAME,
                  "WHERE",
                  ANY_NAME_1 + "=?",
                  "AND",
                  ANY_NAME_2 + ">=?",
                  "AND",
                  ANY_NAME_2 + "<=?",
                });
    configureBehavior(expected);
    scan = prepareScan();
    scan.withStart(new Key(new TextValue(ANY_NAME_2, ANY_TEXT_2)))
        .withEnd(new Key(new TextValue(ANY_NAME_2, ANY_TEXT_3)));

    // Act
    handler.prepare(scan);

    // Assert
    verify(session).prepare(expected);
  }

  @Test
  public void prepare_ScanOperationWithMultipleClusteringKeys_ShouldPrepareProperQuery() {
    // Arrange
    String expected =
        Joiner.on(" ")
            .skipNulls()
            .join(
                new String[] {
                  "SELECT * FROM",
                  ANY_KEYSPACE_NAME + "." + ANY_TABLE_NAME,
                  "WHERE",
                  ANY_NAME_1 + "=?",
                  "AND",
                  ANY_NAME_2 + "=?",
                  "AND",
                  ANY_NAME_3 + ">=?",
                  "AND",
                  ANY_NAME_2 + "=?",
                  "AND",
                  ANY_NAME_3 + "<=?",
                });
    configureBehavior(expected);
    scan = prepareScan();
    scan.withStart(
            new Key(new TextValue(ANY_NAME_2, ANY_TEXT_2), new TextValue(ANY_NAME_3, ANY_TEXT_3)))
        .withEnd(
            new Key(new TextValue(ANY_NAME_2, ANY_TEXT_2), new TextValue(ANY_NAME_3, ANY_TEXT_4)));

    // Act
    handler.prepare(scan);

    // Assert
    verify(session).prepare(expected);
  }

  @Test
  public void prepare_ScanOperationWithNeitherInclusive_ShouldPrepareProperQuery() {
    // Arrange
    String expected =
        Joiner.on(" ")
            .skipNulls()
            .join(
                new String[] {
                  "SELECT * FROM",
                  ANY_KEYSPACE_NAME + "." + ANY_TABLE_NAME,
                  "WHERE",
                  ANY_NAME_1 + "=?",
                  "AND",
                  ANY_NAME_2 + ">?",
                  "AND",
                  ANY_NAME_2 + "<?",
                });
    configureBehavior(expected);
    scan = prepareScan();
    scan.withStart(new Key(new TextValue(ANY_NAME_2, ANY_TEXT_2)), false)
        .withEnd(new Key(new TextValue(ANY_NAME_2, ANY_TEXT_3)), false);

    // Act
    handler.prepare(scan);

    // Assert
    verify(session).prepare(expected);
  }

  @Test
  public void prepare_ScanOperationWithOrderingAndLimit_ShouldPrepareProperQuery() {
    // Arrange
    String expected =
        Joiner.on(" ")
            .skipNulls()
            .join(
                new String[] {
                  "SELECT * FROM",
                  ANY_KEYSPACE_NAME + "." + ANY_TABLE_NAME,
                  "WHERE",
                  ANY_NAME_1 + "=?",
                  "AND",
                  ANY_NAME_2 + ">=?",
                  "ORDER BY",
                  ANY_NAME_2,
                  ASC_ORDER.toString(),
                  "LIMIT",
                  Integer.toString(ANY_LIMIT),
                });
    configureBehavior(expected);
    scan = prepareScan();
    scan.withStart(new Key(new TextValue(ANY_NAME_2, ANY_TEXT_2)))
        .withOrdering(new Scan.Ordering(ANY_NAME_2, ASC_ORDER))
        .withLimit(ANY_LIMIT);

    // Act
    handler.prepare(scan);

    // Assert
    verify(session).prepare(expected);
  }

  @Test
  public void prepare_ScanOperationWithMultipleOrdering_ShouldPrepareProperQuery() {
    // Arrange
    String expected =
        Joiner.on(" ")
            .skipNulls()
            .join(
                new String[] {
                  "SELECT * FROM",
                  ANY_KEYSPACE_NAME + "." + ANY_TABLE_NAME,
                  "WHERE",
                  ANY_NAME_1 + "=?",
                  "AND",
                  ANY_NAME_2 + ">=?",
                  "ORDER BY",
                  ANY_NAME_2,
                  ASC_ORDER.toString() + "," + ANY_NAME_3,
                  DESC_ORDER.toString(),
                  "LIMIT",
                  Integer.toString(ANY_LIMIT),
                });
    configureBehavior(expected);
    scan = prepareScan();
    scan.withStart(new Key(new TextValue(ANY_NAME_2, ANY_TEXT_2)))
        .withOrdering(new Scan.Ordering(ANY_NAME_2, ASC_ORDER))
        .withOrdering(new Scan.Ordering(ANY_NAME_3, DESC_ORDER))
        .withLimit(ANY_LIMIT);

    // Act
    handler.prepare(scan);

    // Assert
    verify(session).prepare(expected);
  }

  @Test
  public void bind_GetOperationGiven_ShouldBindProperly() {
    // Arrange
    configureBehavior(null);
    get = prepareGetWithClusteringKey();

    // Act
    handler.bind(prepared, get);

    // Assert
    verify(builder).setString(0, ANY_TEXT_1);
    verify(builder).setString(1, ANY_TEXT_2);
  }

  @Test
  public void bind_ScanOperationWithMultipleClusteringKeysGiven_ShouldBindProperly() {
    // Arrange
    configureBehavior(null);
    scan = prepareScan();
    scan.withStart(
            new Key(new TextValue(ANY_NAME_2, ANY_TEXT_2), new TextValue(ANY_NAME_3, ANY_TEXT_3)))
        .withEnd(
            new Key(new TextValue(ANY_NAME_2, ANY_TEXT_2), new TextValue(ANY_NAME_3, ANY_TEXT_4)));

    // Act
    handler.bind(prepared, scan);

    // Assert
    verify(builder).setString(0, ANY_TEXT_1);
    verify(builder).setString(1, ANY_TEXT_2);
    verify(builder).setString(2, ANY_TEXT_3);
    verify(builder).setString(3, ANY_TEXT_2);
    verify(builder).setString(4, ANY_TEXT_4);
  }

  @Test
  public void setConsistency_GetOperationWithStrongConsistencyGiven_ShouldPrepareWithQuorum() {
    // Arrange
    configureBehavior(null);
    get = prepareGetWithClusteringKey();
    get.withConsistency(Consistency.SEQUENTIAL);

    // Act
    handler.setConsistency(builder, get);

    // Assert
    verify(builder).setConsistencyLevel(ConsistencyLevel.QUORUM);
  }

  @Test
  public void setConsistency_GetOperationWithEventualConsistencyGiven_ShouldPrepareWithOne() {
    // Arrange
    configureBehavior(null);
    get = prepareGetWithClusteringKey();
    get.withConsistency(Consistency.EVENTUAL);

    // Act
    handler.setConsistency(builder, get);

    // Assert
    verify(builder).setConsistencyLevel(ConsistencyLevel.ONE);
  }

  @Test
  public void
      setConsistency_GetOperationWithLinearizableConsistencyGiven_ShouldPrepareWithSerial() {
    // Arrange
    configureBehavior(null);
    get = prepareGetWithClusteringKey();
    get.withConsistency(Consistency.LINEARIZABLE);

    // Act
    handler.setConsistency(builder, get);

    // Assert
    verify(builder).setConsistencyLevel(ConsistencyLevel.SERIAL);
  }

  @Test
  public void setConsistency_ScanOperationWithStrongConsistencyGiven_ShouldPrepareWithQuorum() {
    // Arrange
    configureBehavior(null);
    scan = prepareScan();
    scan.withConsistency(Consistency.SEQUENTIAL);

    // Act
    handler.setConsistency(builder, scan);

    // Assert
    verify(builder).setConsistencyLevel(ConsistencyLevel.QUORUM);
  }

  @Test
  public void setConsistency_ScanOperationWithEventualConsistencyGiven_ShouldPrepareWithOne() {
    // Arrange
    configureBehavior(null);
    scan = prepareScan();
    scan.withConsistency(Consistency.EVENTUAL);

    // Act
    handler.setConsistency(builder, scan);

    // Assert
    verify(builder).setConsistencyLevel(ConsistencyLevel.ONE);
  }

  @Test
  public void
      setConsistency_ScanOperationWithLinearizableConsistencyGiven_ShouldPrepareWithSerial() {
    // Arrange
    configureBehavior(null);
    scan = prepareScan();
    scan.withConsistency(Consistency.LINEARIZABLE);

    // Act
    handler.setConsistency(builder, scan);

    // Assert
    verify(builder).setConsistencyLevel(ConsistencyLevel.SERIAL);
  }

  @Test
  public void handle_DriverExceptionThrown_ShouldThrowProperExecutionException()
      throws ExecutionException {
    // Arrange
    get = prepareGetWithClusteringKey();
    SelectStatementHandler spy = Mockito.spy(new SelectStatementHandler(session));
    doReturn(prepared).when(spy).prepare(get);
    doReturn(builder).when(spy).bind(prepared, get);

    DriverException toThrow = mock(DriverException.class);
    doThrow(toThrow).when(spy).handleInternal(get);

    // Act
    assertThatThrownBy(
            () -> {
              spy.handle(get);
            })
        .isInstanceOf(ExecutionException.class)
        .hasCause(toThrow);

    // Assert
  }

  @Test
  public void checkArgument_WrongOperationGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    Operation operation = mock(Put.class);

    // Act Assert
    assertThatThrownBy(
            () -> {
              StatementHandler.checkArgument(operation, Get.class, Scan.class);
            })
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void constructor_NullGiven_ShouldThrowNullPointerException() {
    // Act Assert
    assertThatThrownBy(
            () -> {
              new SelectStatementHandler(null);
            })
        .isInstanceOf(NullPointerException.class);
  }
}
