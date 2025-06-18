package com.scalar.db.transaction.consensuscommit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.PutBuilder;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.DataType;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public abstract class AbstractMutationComposer implements MutationComposer {
  protected final String id;
  protected final List<Mutation> mutations;
  protected final long current;
  protected final TransactionTableMetadataManager tableMetadataManager;

  public AbstractMutationComposer(String id, TransactionTableMetadataManager tableMetadataManager) {
    this.id = id;
    this.mutations = new ArrayList<>();
    this.current = System.currentTimeMillis();
    this.tableMetadataManager = tableMetadataManager;
  }

  @VisibleForTesting
  AbstractMutationComposer(
      String id, long current, TransactionTableMetadataManager tableMetadataManager) {
    this.id = id;
    this.mutations = new ArrayList<>();
    this.current = current;
    this.tableMetadataManager = tableMetadataManager;
  }

  @Override
  public List<Mutation> get() {
    return ImmutableList.copyOf(mutations);
  }

  static void setBeforeImageColumnsToNull(
      PutBuilder.Buildable putBuilder,
      Set<String> beforeImageColumnNames,
      TableMetadata tableMetadata) {
    for (String beforeImageColumnName : beforeImageColumnNames) {
      DataType columnDataType = tableMetadata.getColumnDataType(beforeImageColumnName);
      switch (columnDataType) {
        case BOOLEAN:
          putBuilder.booleanValue(beforeImageColumnName, null);
          break;
        case INT:
          putBuilder.intValue(beforeImageColumnName, null);
          break;
        case BIGINT:
          putBuilder.bigIntValue(beforeImageColumnName, null);
          break;
        case FLOAT:
          putBuilder.floatValue(beforeImageColumnName, null);
          break;
        case DOUBLE:
          putBuilder.doubleValue(beforeImageColumnName, null);
          break;
        case TEXT:
          putBuilder.textValue(beforeImageColumnName, null);
          break;
        case BLOB:
          putBuilder.blobValue(beforeImageColumnName, (byte[]) null);
          break;
        case DATE:
          putBuilder.dateValue(beforeImageColumnName, null);
          break;
        case TIME:
          putBuilder.timeValue(beforeImageColumnName, null);
          break;
        case TIMESTAMP:
          putBuilder.timestampValue(beforeImageColumnName, null);
          break;
        case TIMESTAMPTZ:
          putBuilder.timestampTZValue(beforeImageColumnName, null);
          break;
        default:
          throw new AssertionError("Unknown data type: " + columnDataType);
      }
    }
  }
}
