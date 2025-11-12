package com.scalar.db.storage.objectstorage;

import static com.google.common.base.Preconditions.checkNotNull;

import com.scalar.db.api.Operation;
import com.scalar.db.common.TableMetadataManager;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import javax.annotation.Nonnull;

public class StatementHandler {
  protected final ObjectStorageWrapper wrapper;
  protected final TableMetadataManager metadataManager;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public StatementHandler(ObjectStorageWrapper wrapper, TableMetadataManager metadataManager) {
    this.wrapper = checkNotNull(wrapper);
    this.metadataManager = checkNotNull(metadataManager);
  }

  @Nonnull
  protected String getNamespace(Operation operation) {
    assert operation.forNamespace().isPresent();
    return operation.forNamespace().get();
  }

  @Nonnull
  protected String getTable(Operation operation) {
    assert operation.forTable().isPresent();
    return operation.forTable().get();
  }
}
