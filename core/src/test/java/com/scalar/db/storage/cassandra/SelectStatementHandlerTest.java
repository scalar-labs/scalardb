package com.scalar.db.storage.cassandra;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.DriverException;
import com.google.common.base.Joiner;
import com.scalar.db.api.Consistency;
import com.scalar.db.api.Get;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Scan;
import com.scalar.db.api.ScanAll;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.Key;
import java.util.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class SelectStatementHandlerTest {
  private static final int FETCH_SIZE = 10;
  private static final String ANY_NAMESPACE_NAME = "namespace";
  private static final String ANY_TABLE_NAME = "table_name";
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
  @Mock private Session session;
  @Mock private PreparedStatement prepared;
  @Mock private BoundStatement bound;

  @BeforeEach
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this).close();

    handler = new SelectStatementHandler(session, FETCH_SIZE);
  }

  private Get prepareGet() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    return new Get(partitionKey).forNamespace(ANY_NAMESPACE_NAME).forTable(ANY_TABLE_NAME);
  }

  private Get prepareGetWithClusteringKey() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    Key clusteringKey = new Key(ANY_NAME_2, ANY_TEXT_2);
    return new Get(partitionKey, clusteringKey)
        .forNamespace(ANY_NAMESPACE_NAME)
        .forTable(ANY_TABLE_NAME);
  }

  private Get prepareGetWithReservedKeywords() {
    Key partitionKey = new Key("from", ANY_TEXT_1);
    Key clusteringKey = new Key("to", ANY_TEXT_2);
    return new Get(partitionKey, clusteringKey).forNamespace("keyspace").forTable("table");
  }

  private Scan prepareScan() {
    Key partitionKey = new Key(ANY_NAME_1, ANY_TEXT_1);
    return new Scan(partitionKey).forNamespace(ANY_NAMESPACE_NAME).forTable(ANY_TABLE_NAME);
  }

  private ScanAll prepareScanAll() {
    return new ScanAll().forNamespace(ANY_NAMESPACE_NAME).forTable(ANY_TABLE_NAME);
  }

  private void configureBehavior(String expected) {
    when(session.prepare(expected == null ? anyString() : expected)).thenReturn(prepared);
    when(prepared.setConsistencyLevel(any(ConsistencyLevel.class))).thenReturn(prepared);

    when(prepared.bind()).thenReturn(bound);
    when(bound.setString(anyInt(), anyString())).thenReturn(bound);
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
                  ANY_NAMESPACE_NAME + "." + ANY_TABLE_NAME,
                  "WHERE",
                  ANY_NAME_1 + "=?;",
                });
    configureBehavior(expected);
    get = prepareGet();

    // Act
    handler.prepare(get);

    // Assert
    verify(session).prepare(expected);
  }

  @Test
  public void prepare_SameQueryGivenTwice_SecondTimeShouldUseStatementCache() {
    // Arrange
    String expected =
        Joiner.on(" ")
            .skipNulls()
            .join(
                new String[] {
                  "SELECT * FROM",
                  ANY_NAMESPACE_NAME + "." + ANY_TABLE_NAME,
                  "WHERE",
                  ANY_NAME_1 + "=?;",
                });
    configureBehavior(expected);
    get = prepareGet();

    // Act
    handler.prepare(get);
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
                  ANY_NAMESPACE_NAME + "." + ANY_TABLE_NAME,
                  "WHERE",
                  ANY_NAME_1 + "=?",
                  "AND",
                  ANY_NAME_2 + "=?;",
                });
    configureBehavior(expected);
    get = prepareGetWithClusteringKey();

    // Act
    handler.prepare(get);

    // Assert
    verify(session).prepare(expected);
  }

  @Test
  public void prepare_GetOperationWithReservedKeywordsGiven_ShouldPrepareProperQuery() {
    // Arrange
    String expected =
        Joiner.on(" ")
            .skipNulls()
            .join(
                new String[] {
                  "SELECT * FROM",
                  "\"keyspace\"" + "." + "\"table\"",
                  "WHERE",
                  "\"from\"" + "=?",
                  "AND",
                  "\"to\"" + "=?;",
                });
    configureBehavior(expected);
    get = prepareGetWithReservedKeywords();

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
                  ANY_NAMESPACE_NAME + "." + ANY_TABLE_NAME,
                  "WHERE",
                  ANY_NAME_1 + "=?",
                  "AND",
                  ANY_NAME_2 + "=?;",
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
                  ANY_NAMESPACE_NAME + "." + ANY_TABLE_NAME,
                  "WHERE",
                  ANY_NAME_1 + "=?",
                  "AND",
                  ANY_NAME_2 + ">=?",
                  "AND",
                  ANY_NAME_2 + "<=?;",
                });
    configureBehavior(expected);
    scan = prepareScan();
    scan.withStart(new Key(ANY_NAME_2, ANY_TEXT_2)).withEnd(new Key(ANY_NAME_2, ANY_TEXT_3));

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
                  ANY_NAMESPACE_NAME + "." + ANY_TABLE_NAME,
                  "WHERE",
                  ANY_NAME_1 + "=?",
                  "AND",
                  ANY_NAME_2 + "=?",
                  "AND",
                  ANY_NAME_3 + ">=?",
                  "AND",
                  ANY_NAME_3 + "<=?;",
                });
    configureBehavior(expected);
    scan = prepareScan();
    scan.withStart(new Key(ANY_NAME_2, ANY_TEXT_2, ANY_NAME_3, ANY_TEXT_3))
        .withEnd(new Key(ANY_NAME_2, ANY_TEXT_2, ANY_NAME_3, ANY_TEXT_4));

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
                  ANY_NAMESPACE_NAME + "." + ANY_TABLE_NAME,
                  "WHERE",
                  ANY_NAME_1 + "=?",
                  "AND",
                  ANY_NAME_2 + ">?",
                  "AND",
                  ANY_NAME_2 + "<?;",
                });
    configureBehavior(expected);
    scan = prepareScan();
    scan.withStart(new Key(ANY_NAME_2, ANY_TEXT_2), false)
        .withEnd(new Key(ANY_NAME_2, ANY_TEXT_3), false);

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
                  ANY_NAMESPACE_NAME + "." + ANY_TABLE_NAME,
                  "WHERE",
                  ANY_NAME_1 + "=?",
                  "AND",
                  ANY_NAME_2 + ">=?",
                  "ORDER BY",
                  ANY_NAME_2,
                  ASC_ORDER.toString(),
                  "LIMIT",
                  ANY_LIMIT + ";",
                });
    configureBehavior(expected);
    scan = prepareScan();
    scan.withStart(new Key(ANY_NAME_2, ANY_TEXT_2))
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
                  ANY_NAMESPACE_NAME + "." + ANY_TABLE_NAME,
                  "WHERE",
                  ANY_NAME_1 + "=?",
                  "AND",
                  ANY_NAME_2 + ">=?",
                  "ORDER BY",
                  ANY_NAME_2,
                  ASC_ORDER + "," + ANY_NAME_3,
                  DESC_ORDER.toString(),
                  "LIMIT",
                  ANY_LIMIT + ";",
                });
    configureBehavior(expected);
    scan = prepareScan();
    scan.withStart(new Key(ANY_NAME_2, ANY_TEXT_2))
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
    verify(bound).setString(0, ANY_TEXT_1);
    verify(bound).setString(1, ANY_TEXT_2);
  }

  @Test
  public void bind_ScanOperationWithMultipleClusteringKeysGiven_ShouldBindProperly() {
    // Arrange
    configureBehavior(null);
    scan = prepareScan();
    scan.withStart(new Key(ANY_NAME_2, ANY_TEXT_2, ANY_NAME_3, ANY_TEXT_3))
        .withEnd(new Key(ANY_NAME_2, ANY_TEXT_2, ANY_NAME_3, ANY_TEXT_4));

    // Act
    handler.bind(prepared, scan);

    // Assert
    verify(bound).setString(0, ANY_TEXT_1);
    verify(bound).setString(1, ANY_TEXT_2);
    verify(bound).setString(2, ANY_TEXT_3);
    verify(bound).setString(3, ANY_TEXT_4);
  }

  @Test
  public void setConsistency_GetOperationWithStrongConsistencyGiven_ShouldPrepareWithQuorum() {
    // Arrange
    configureBehavior(null);
    get = prepareGetWithClusteringKey();
    get.withConsistency(Consistency.SEQUENTIAL);

    // Act
    handler.setConsistency(bound, get);

    // Assert
    verify(bound).setConsistencyLevel(ConsistencyLevel.QUORUM);
  }

  @Test
  public void setConsistency_GetOperationWithEventualConsistencyGiven_ShouldPrepareWithOne() {
    // Arrange
    configureBehavior(null);
    get = prepareGetWithClusteringKey();
    get.withConsistency(Consistency.EVENTUAL);

    // Act
    handler.setConsistency(bound, get);

    // Assert
    verify(bound).setConsistencyLevel(ConsistencyLevel.ONE);
  }

  @Test
  public void
      setConsistency_GetOperationWithLinearizableConsistencyGiven_ShouldPrepareWithSerial() {
    // Arrange
    configureBehavior(null);
    get = prepareGetWithClusteringKey();
    get.withConsistency(Consistency.LINEARIZABLE);

    // Act
    handler.setConsistency(bound, get);

    // Assert
    verify(bound).setConsistencyLevel(ConsistencyLevel.SERIAL);
  }

  @Test
  public void setConsistency_ScanOperationWithStrongConsistencyGiven_ShouldPrepareWithQuorum() {
    // Arrange
    configureBehavior(null);
    scan = prepareScan();
    scan.withConsistency(Consistency.SEQUENTIAL);

    // Act
    handler.setConsistency(bound, scan);

    // Assert
    verify(bound).setConsistencyLevel(ConsistencyLevel.QUORUM);
  }

  @Test
  public void setConsistency_ScanOperationWithEventualConsistencyGiven_ShouldPrepareWithOne() {
    // Arrange
    configureBehavior(null);
    scan = prepareScan();
    scan.withConsistency(Consistency.EVENTUAL);

    // Act
    handler.setConsistency(bound, scan);

    // Assert
    verify(bound).setConsistencyLevel(ConsistencyLevel.ONE);
  }

  @Test
  public void
      setConsistency_ScanOperationWithLinearizableConsistencyGiven_ShouldPrepareWithSerial() {
    // Arrange
    configureBehavior(null);
    scan = prepareScan();
    scan.withConsistency(Consistency.LINEARIZABLE);

    // Act
    handler.setConsistency(bound, scan);

    // Assert
    verify(bound).setConsistencyLevel(ConsistencyLevel.SERIAL);
  }

  @Test
  public void handle_DriverExceptionThrown_ShouldThrowProperExecutionException() {
    // Arrange
    get = prepareGetWithClusteringKey();
    SelectStatementHandler spy = Mockito.spy(new SelectStatementHandler(session, FETCH_SIZE));
    doReturn(prepared).when(spy).prepare(get);
    doReturn(bound).when(spy).bind(prepared, get);

    DriverException toThrow = mock(DriverException.class);
    doThrow(toThrow).when(spy).handleInternal(get);

    // Act
    assertThatThrownBy(() -> spy.handle(get))
        .isInstanceOf(ExecutionException.class)
        .hasCause(toThrow);

    // Assert
  }

  @Test
  public void checkArgument_WrongOperationGiven_ShouldThrowIllegalArgumentException() {
    // Arrange
    Operation operation = mock(Put.class);

    // Act Assert
    assertThatThrownBy(() -> StatementHandler.checkArgument(operation, Get.class, Scan.class))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void constructor_NullGiven_ShouldThrowNullPointerException() {
    // Act Assert
    assertThatThrownBy(() -> new SelectStatementHandler(null, FETCH_SIZE))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  public void prepare_ScanAllOperationWithLimit_ShouldPrepareProperQuery() {
    // Arrange
    String expected =
        Joiner.on(" ")
            .skipNulls()
            .join(
                new String[] {
                  "SELECT * FROM",
                  ANY_NAMESPACE_NAME + "." + ANY_TABLE_NAME,
                  "LIMIT",
                  ANY_LIMIT + ";",
                });
    configureBehavior(expected);
    ScanAll scanAll = prepareScanAll();
    scanAll.withLimit(ANY_LIMIT);

    // Act
    handler.prepare(scanAll);

    // Assert
    verify(session).prepare(expected);
  }

  @Test
  public void prepare_ScanAllOperationWithoutLimit_ShouldPrepareProperQuery() {
    // Arrange
    String expected =
        Joiner.on(" ")
            .skipNulls()
            .join(
                new String[] {
                  "SELECT * FROM", ANY_NAMESPACE_NAME + "." + ANY_TABLE_NAME + ";",
                });
    configureBehavior(expected);
    ScanAll scanAll = prepareScanAll();

    // Act
    handler.prepare(scanAll);

    // Assert
    verify(session).prepare(expected);
  }

  @Test
  public void prepare_ScanAllOperationWithProjectedColumns_ShouldPrepareProperQuery() {
    // Arrange
    String expected =
        Joiner.on(" ")
            .skipNulls()
            .join(
                new String[] {
                  "SELECT",
                  ANY_NAME_1 + "," + ANY_NAME_2,
                  "FROM",
                  ANY_NAMESPACE_NAME + "." + ANY_TABLE_NAME + ";",
                });
    configureBehavior(expected);
    ScanAll scanAll = prepareScanAll().withProjections(Arrays.asList(ANY_NAME_1, ANY_NAME_2));

    // Act
    handler.prepare(scanAll);

    // Assert
    verify(session).prepare(expected);
  }

  @Test
  public void execute_ShouldSetFetchSizeAndExecute() {
    // Arrange
    Get get = prepareGet();

    // Act
    handler.execute(bound, get);

    // Assert
    verify(bound).setFetchSize(FETCH_SIZE);
    verify(session).execute(bound);
  }
}
