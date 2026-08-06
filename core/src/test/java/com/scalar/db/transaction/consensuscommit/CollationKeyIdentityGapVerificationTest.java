package com.scalar.db.transaction.consensuscommit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Put;
import com.scalar.db.api.Scan;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.ResultImpl;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.io.CollationComparator;
import com.scalar.db.io.Column;
import com.scalar.db.io.DataType;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Acceptance tests of increment B (collation-canonical snapshot key identity).
 *
 * <p>Asserts the CORRECT collation-aware key-identity behavior (origin plan R4/R6), i.e. the
 * behavior MySQL itself exhibits with a case-insensitive collation (verified empirically: write
 * 'apple' / read 'Apple' sees the row; two case-variant writes converge on one row). Each test
 * covers a Consensus Commit defect that existed while key identity was byte-exact under a
 * case-insensitive ICU collation against a CI-collated backend; the canonical snapshot key turned
 * them green.
 */
public class CollationKeyIdentityGapVerificationTest {

  private static final String NS = "ns";
  private static final String TBL = "tbl";
  private static final String TX_ID = "tx";
  private static final String PK_COL = "pk";
  private static final String CK_COL = "ck";
  private static final String VAL_COL = "val";

  private static final TableMetadata TABLE_METADATA =
      ConsensusCommitUtils.buildTransactionTableMetadata(
          TableMetadata.newBuilder()
              .addColumn(PK_COL, DataType.TEXT)
              .addColumn(CK_COL, DataType.TEXT)
              .addColumn(VAL_COL, DataType.TEXT)
              .addPartitionKey(PK_COL)
              .addClusteringKey(CK_COL)
              .build());

  private Snapshot snapshot;
  private Map<Snapshot.Key, Put> writeSet;

  private TransactionTableMetadataManager tableMetadataManager;

  @BeforeEach
  public void setUp() throws Exception {
    tableMetadataManager = mock(TransactionTableMetadataManager.class);
    when(tableMetadataManager.getTransactionTableMetadata(any()))
        .thenReturn(new TransactionTableMetadata(TABLE_METADATA));
    when(tableMetadataManager.getTransactionTableMetadata(any(), any()))
        .thenReturn(new TransactionTableMetadata(TABLE_METADATA));
    ConsensusCommitConfig config = mock(ConsensusCommitConfig.class);

    writeSet = new HashMap<>();
    snapshot =
        new Snapshot(
            TX_ID,
            tableMetadataManager,
            new ParallelExecutor(config),
            caseInsensitiveIcuCollation(),
            new ConcurrentHashMap<>(),
            new ConcurrentHashMap<>(),
            new HashMap<>(),
            writeSet,
            new HashMap<>(),
            new ArrayList<>());
  }

  private static CollationComparator caseInsensitiveIcuCollation() {
    Properties props = new Properties();
    props.setProperty(DatabaseConfig.CONTACT_POINTS, "localhost");
    props.setProperty(DatabaseConfig.COLLATION, "ICU");
    props.setProperty(DatabaseConfig.COLLATION_ICU_STRENGTH, "PRIMARY");
    return CollationComparator.from(new DatabaseConfig(props));
  }

  /** A storage-returned row, as a CI backend would echo it: clustering key stored as 'Apple'. */
  private TransactionResult storedRow(String clusteringValue, String val) {
    ImmutableMap<String, Column<?>> columns =
        ImmutableMap.<String, Column<?>>builder()
            .put(PK_COL, TextColumn.of(PK_COL, "p1"))
            .put(CK_COL, TextColumn.of(CK_COL, clusteringValue))
            .put(VAL_COL, TextColumn.of(VAL_COL, val))
            .put(Attribute.ID, TextColumn.of(Attribute.ID, "other-tx"))
            .build();
    return new TransactionResult(new ResultImpl(columns, TABLE_METADATA));
  }

  private Put put(String clusteringValue, String val) {
    return Put.newBuilder()
        .namespace(NS)
        .table(TBL)
        .partitionKey(Key.ofText(PK_COL, "p1"))
        .clusteringKey(Key.ofText(CK_COL, clusteringValue))
        .textValue(VAL_COL, val)
        .build();
  }

  private Delete delete(String clusteringValue) {
    return Delete.newBuilder()
        .namespace(NS)
        .table(TBL)
        .partitionKey(Key.ofText(PK_COL, "p1"))
        .clusteringKey(Key.ofText(CK_COL, clusteringValue))
        .build();
  }

  // ---------------------------------------------------------------------------------------------
  // Gap 1 — SILENT STALE READ (read-your-own-writes miss).
  // MySQL (verified): INSERT 'banana' then SELECT WHERE ck='Banana' inside one transaction
  // returns the own write. ScalarDB today: the write is buffered under the request bytes
  // ('apple'); a read keyed by the storage-returned bytes ('Apple') misses the writeSet in
  // mergeResult (Snapshot.java:319) and returns the STALE storage row with no error.
  // ---------------------------------------------------------------------------------------------
  @Test
  public void readYourOwnWrite_CollateEqualKeyFromStorage_ShouldSeeOwnBufferedWrite()
      throws Exception {
    // Arrange: the storage row is keyed 'Apple' (storage-returned provenance, CrudHandler:441);
    // the transaction wrote via the spelling it typed: 'apple'.
    Snapshot.Key storageKey =
        new Snapshot.Key(put("Apple", "ignored"), caseInsensitiveIcuCollation());
    snapshot.putIntoReadSet(storageKey, Optional.of(storedRow("Apple", "stale")));
    Put ownWrite = put("apple", "updated");
    snapshot.putIntoWriteSet(new Snapshot.Key(ownWrite, caseInsensitiveIcuCollation()), ownWrite);

    // Act: read the row via its storage-returned key (what a scan hands back).
    Optional<TransactionResult> result = snapshot.getResult(storageKey);

    // Assert (CORRECT behavior, R4): the merged result reflects the transaction's own write.
    assertThat(result).isPresent();
    assertThat(result.get().getText(VAL_COL))
        .as("read-your-own-writes must reflect the buffered write, as the CI backend would")
        .isEqualTo("updated");
  }

  // ---------------------------------------------------------------------------------------------
  // Gap 2 — SPURIOUS ABORT AT PREPARE (before-image join miss).
  // At prepare time, Snapshot.to() joins each writeSet key against the readSet to fetch the
  // before image (Snapshot.java:366). A byte-exact miss hands the composer a null result,
  // flipping PrepareMutationComposer into the putIfNotExists insert branch — which the CI
  // backend's uniqueness then rejects: a valid read-modify-write aborts every time.
  // ---------------------------------------------------------------------------------------------
  @Test
  public void prepare_CollateEqualReadAndWriteKeys_ComposerShouldReceiveBeforeImage()
      throws Exception {
    // Arrange: read populated the readSet under the storage bytes; the app updated via its own
    // spelling.
    TransactionResult beforeImage = storedRow("Apple", "old");
    snapshot.putIntoReadSet(
        new Snapshot.Key(put("Apple", "ignored"), caseInsensitiveIcuCollation()),
        Optional.of(beforeImage));
    Put ownWrite = put("apple", "new");
    snapshot.putIntoWriteSet(new Snapshot.Key(ownWrite, caseInsensitiveIcuCollation()), ownWrite);
    PrepareMutationComposer composer = mock(PrepareMutationComposer.class);

    // Act
    snapshot.to(composer);

    // Assert (CORRECT behavior, R4): the composer receives the before image (update branch),
    // not null (insert branch -> putIfNotExists -> spurious PreparationConflictException).
    verify(composer).add(ownWrite, beforeImage);
  }

  // ---------------------------------------------------------------------------------------------
  // Gap 3 — WRITE-WRITE SPLIT (one physical row, two mutations).
  // MySQL (verified): INSERT 'banana' then UPDATE WHERE ck='BANANA' converge on ONE row.
  // ScalarDB today: two collate-equal puts produce TWO writeSet entries (Snapshot.java:192
  // containsKey miss skips the merge), i.e. two prepared mutations against one physical row —
  // the second one conflicts with the first at prepare.
  // ---------------------------------------------------------------------------------------------
  @Test
  public void writeSet_TwoCollateEqualPuts_ShouldMergeIntoOneLogicalEntry() throws Exception {
    // Arrange + Act
    Put first = put("banana", "v1");
    Put second = put("BANANA", "v2");
    snapshot.putIntoWriteSet(new Snapshot.Key(first, caseInsensitiveIcuCollation()), first);
    snapshot.putIntoWriteSet(new Snapshot.Key(second, caseInsensitiveIcuCollation()), second);

    // Assert (CORRECT behavior, R4): one logical key -> one merged writeSet entry, exactly as
    // the CI backend holds one row.
    assertThat(writeSet)
        .as("collate-equal puts must merge into one logical write, as the CI backend holds one row")
        .hasSize(1);
  }

  // ---------------------------------------------------------------------------------------------
  // Gap 4 — MISSED SCAN-AFTER-WRITE GUARD (silent inconsistent scan).
  // verifyNoOverlap's deleteSet branch relies solely on results.containsKey
  // (Snapshot.java:430). The scan results are keyed by storage-returned bytes; the deleteSet by
  // request bytes. The byte-exact miss skips the SCANNING_ALREADY_WRITTEN protection, so the
  // scan silently returns a view contradicting the transaction's own pending delete.
  // ---------------------------------------------------------------------------------------------
  @Test
  public void verifyNoOverlap_ScanSeesRowDeletedUnderCollateEqualKey_ShouldThrow() {
    // Arrange: the transaction deleted the row via its own spelling ('apple'); the scan then
    // returns the same physical row under its stored bytes ('Apple').
    Delete ownDelete = delete("apple");
    snapshot.putIntoDeleteSet(
        new Snapshot.Key(ownDelete, caseInsensitiveIcuCollation()), ownDelete);
    LinkedHashMap<Snapshot.Key, TransactionResult> scanResults = new LinkedHashMap<>();
    scanResults.put(
        new Snapshot.Key(put("Apple", "ignored"), caseInsensitiveIcuCollation()),
        storedRow("Apple", "v"));
    Scan scan =
        Scan.newBuilder().namespace(NS).table(TBL).partitionKey(Key.ofText(PK_COL, "p1")).build();

    // Act
    Throwable thrown = catchThrowable(() -> snapshot.verifyNoOverlap(scan, scanResults));

    // Assert (CORRECT behavior, R4/R6): scanning data the transaction already deleted must be
    // detected as an overlap, not silently returned.
    assertThat(thrown)
        .as("scan-after-delete on a collate-equal key must be detected as an overlap")
        .isInstanceOf(IllegalArgumentException.class);
  }
}
