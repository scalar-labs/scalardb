package com.scalar.db.storage.jdbc;

import com.scalar.db.api.ScanAll;
import com.scalar.db.api.Selection;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.StorageInfoProvider;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.common.checker.OperationChecker;
import com.scalar.db.config.DatabaseConfig;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class JdbcOperationChecker extends OperationChecker {
  private final RdbEngineStrategy rdbEngine;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public JdbcOperationChecker(
      DatabaseConfig config,
      TableMetadataManager tableMetadataManager,
      StorageInfoProvider storageInfoProvider,
      RdbEngineStrategy rdbEngine) {
    super(config, tableMetadataManager, storageInfoProvider);
    this.rdbEngine = rdbEngine;
  }

  @Override
  protected void checkOrderingsForScanAll(ScanAll scanAll, TableMetadata metadata) {
    super.checkOrderingsForScanAll(scanAll, metadata);
    rdbEngine.throwIfCrossPartitionScanOrderingOnBlobColumnNotSupported(scanAll, metadata);
  }

  @Override
  protected void checkConjunctions(Selection selection, TableMetadata metadata) {
    super.checkConjunctions(selection, metadata);
    rdbEngine.throwIfConjunctionsColumnNotSupported(selection.getConjunctions(), metadata);
  }
}
