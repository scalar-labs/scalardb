package com.scalar.db.storage.cosmos;

import com.azure.cosmos.models.PartitionKeyDefinitionVersion;
import com.scalar.db.api.ConditionalExpression;
import com.scalar.db.api.Delete;
import com.scalar.db.api.Get;
import com.scalar.db.api.Mutation;
import com.scalar.db.api.Operation;
import com.scalar.db.api.Put;
import com.scalar.db.api.Scan;
import com.scalar.db.api.Selection;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.CoreError;
import com.scalar.db.common.StorageInfoProvider;
import com.scalar.db.common.TableMetadataManager;
import com.scalar.db.common.checker.OperationChecker;
import com.scalar.db.config.DatabaseConfig;
import com.scalar.db.exception.storage.ExecutionException;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.BlobColumn;
import com.scalar.db.io.BooleanColumn;
import com.scalar.db.io.Column;
import com.scalar.db.io.ColumnVisitor;
import com.scalar.db.io.DataType;
import com.scalar.db.io.DateColumn;
import com.scalar.db.io.DoubleColumn;
import com.scalar.db.io.FloatColumn;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.TextColumn;
import com.scalar.db.io.TimeColumn;
import com.scalar.db.io.TimestampColumn;
import com.scalar.db.io.TimestampTZColumn;
import com.scalar.db.util.ScalarDbUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

public class CosmosOperationChecker extends OperationChecker {

  private static final long BIGINT_MAX_VALUE = 9007199254740992L;
  private static final long BIGINT_MIN_VALUE = -9007199254740992L;
  private static final int V1_PARTITION_KEY_MAX_UTF8_BYTES = 101;
  private static final int V2_PARTITION_KEY_MAX_UTF8_BYTES = 2048;
  private static final int DOCUMENT_ID_MAX_LENGTH = 255;

  private static final char[] ILLEGAL_CHARACTERS_IN_PRIMARY_KEY = {
    // Colons are not allowed in primary-key columns due to the `ConcatenationVisitor` limitation.
    ':',

    // The following characters are not allowed in primary-key columns because they are restricted
    // and cannot be used in the `Id` property of a Cosmos DB document. For more information, see:
    // https://learn.microsoft.com/en-us/dotnet/api/microsoft.azure.cosmos.databaseproperties.id?view=azure-dotnet#remarks
    '/',
    '\\',
    '#',
    '?'
  };

  private static final ColumnVisitor PRIMARY_KEY_COLUMN_CHECKER =
      new ColumnVisitor() {
        @Override
        public void visit(BooleanColumn column) {}

        @Override
        public void visit(IntColumn column) {}

        @Override
        public void visit(BigIntColumn column) {
          checkBigIntValueRange(column);
        }

        @Override
        public void visit(FloatColumn column) {}

        @Override
        public void visit(DoubleColumn column) {}

        @Override
        public void visit(TextColumn column) {
          String value = column.getTextValue();
          assert value != null;

          for (char illegalCharacter : ILLEGAL_CHARACTERS_IN_PRIMARY_KEY) {
            if (value.indexOf(illegalCharacter) != -1) {
              throw new IllegalArgumentException(
                  CoreError.COSMOS_PRIMARY_KEY_CONTAINS_ILLEGAL_CHARACTER.buildMessage(
                      column.getName(), value));
            }
          }
        }

        @Override
        public void visit(BlobColumn column) {}

        @Override
        public void visit(DateColumn column) {}

        @Override
        public void visit(TimeColumn column) {}

        @Override
        public void visit(TimestampColumn column) {}

        @Override
        public void visit(TimestampTZColumn column) {}
      };

  private final CosmosAdmin cosmosAdmin;

  @SuppressFBWarnings("EI_EXPOSE_REP2")
  public CosmosOperationChecker(
      DatabaseConfig databaseConfig,
      TableMetadataManager metadataManager,
      StorageInfoProvider storageInfoProvider,
      CosmosAdmin cosmosAdmin) {
    super(databaseConfig, metadataManager, storageInfoProvider);
    this.cosmosAdmin = cosmosAdmin;
  }

  @Override
  public void check(Get get) throws ExecutionException {
    super.check(get);
    checkPrimaryKey(get);
    checkPartitionKeyAndDocumentIdLengths(get);
  }

  @Override
  public void check(Scan scan) throws ExecutionException {
    super.check(scan);
    checkPrimaryKey(scan);
    scan.getStartClusteringKey()
        .ifPresent(
            c -> c.getColumns().forEach(column -> column.accept(PRIMARY_KEY_COLUMN_CHECKER)));
    scan.getEndClusteringKey()
        .ifPresent(
            c -> c.getColumns().forEach(column -> column.accept(PRIMARY_KEY_COLUMN_CHECKER)));
    checkPartitionKeyAndDocumentIdLengths(scan);
  }

  @Override
  public void check(Put put) throws ExecutionException {
    super.check(put);
    checkPrimaryKey(put);
    checkPartitionKeyAndDocumentIdLengths(put);
    checkBigIntColumnsInValues(put);

    TableMetadata metadata = getTableMetadata(put);
    checkCondition(put, metadata);
  }

  @Override
  public void check(Delete delete) throws ExecutionException {
    super.check(delete);
    checkPrimaryKey(delete);
    checkPartitionKeyAndDocumentIdLengths(delete);

    TableMetadata metadata = getTableMetadata(delete);
    checkCondition(delete, metadata);
  }

  private void checkPrimaryKey(Operation operation) {
    operation
        .getPartitionKey()
        .getColumns()
        .forEach(column -> column.accept(PRIMARY_KEY_COLUMN_CHECKER));
    operation
        .getClusteringKey()
        .ifPresent(
            c -> c.getColumns().forEach(column -> column.accept(PRIMARY_KEY_COLUMN_CHECKER)));
  }

  private void checkPartitionKeyAndDocumentIdLengths(Operation operation)
      throws ExecutionException {
    if (operation.getPartitionKey().getColumns().isEmpty()) {
      return;
    }

    TableMetadata metadata = getTableMetadata(operation);
    // Index Gets/Scans put the index column in getPartitionKey(). That is not the Cosmos
    // concatenated partition key, so concatenation and document-id length checks do not apply.
    if (operation instanceof Selection
        && ScalarDbUtils.isSecondaryIndexSpecified((Selection) operation, metadata)) {
      return;
    }

    CosmosOperation cosmosOperation = new CosmosOperation(operation, metadata);
    String concatenatedPartitionKey = cosmosOperation.getConcatenatedPartitionKey();
    int partitionKeyByteLength = concatenatedPartitionKey.getBytes(StandardCharsets.UTF_8).length;

    Optional<PartitionKeyDefinitionVersion> version =
        cosmosAdmin.getPartitionKeyDefinitionVersion(
            operation.forNamespace().orElseThrow(IllegalArgumentException::new),
            operation.forTable().orElseThrow(IllegalArgumentException::new));
    int maxPartitionKeyBytes =
        isV2PartitionKey(version)
            ? V2_PARTITION_KEY_MAX_UTF8_BYTES
            : V1_PARTITION_KEY_MAX_UTF8_BYTES;

    if (partitionKeyByteLength > maxPartitionKeyBytes) {
      throw new IllegalArgumentException(
          CoreError.COSMOS_CONCATENATED_PARTITION_KEY_TOO_LONG.buildMessage(
              operation.forNamespace().orElseThrow(IllegalArgumentException::new),
              operation.forTable().orElseThrow(IllegalArgumentException::new),
              formatPartitionKeyVersion(version),
              partitionKeyByteLength,
              maxPartitionKeyBytes));
    }

    if (cosmosOperation.isPrimaryKeySpecified()) {
      String documentId = cosmosOperation.getId();
      if (documentId.length() > DOCUMENT_ID_MAX_LENGTH) {
        throw new IllegalArgumentException(
            CoreError.COSMOS_DOCUMENT_ID_TOO_LONG.buildMessage(
                operation.forNamespace().orElseThrow(IllegalArgumentException::new),
                operation.forTable().orElseThrow(IllegalArgumentException::new),
                DOCUMENT_ID_MAX_LENGTH,
                documentId.length()));
      }
    }
  }

  private static boolean isV2PartitionKey(Optional<PartitionKeyDefinitionVersion> version) {
    return version.isPresent() && version.get() == PartitionKeyDefinitionVersion.V2;
  }

  private static String formatPartitionKeyVersion(Optional<PartitionKeyDefinitionVersion> version) {
    if (isV2PartitionKey(version)) {
      return "V2";
    }
    return "V1";
  }

  private void checkBigIntColumnsInValues(Put put) {
    for (Column<?> column : put.getColumns().values()) {
      if (column instanceof BigIntColumn) {
        checkBigIntValueRange((BigIntColumn) column);
      }
    }
  }

  private static void checkBigIntValueRange(BigIntColumn column) {
    if (column.hasNullValue()) {
      return;
    }
    long value = column.getBigIntValue();
    if (value < BIGINT_MIN_VALUE || value > BIGINT_MAX_VALUE) {
      throw new IllegalArgumentException(
          CoreError.COSMOS_OUT_OF_RANGE_COLUMN_VALUE_FOR_BIGINT.buildMessage(value));
    }
  }

  private void checkCondition(Mutation mutation, TableMetadata metadata) {
    if (!mutation.getCondition().isPresent()) {
      return;
    }
    for (ConditionalExpression expression : mutation.getCondition().get().getExpressions()) {
      if (metadata.getColumnDataType(expression.getColumn().getName()) == DataType.BLOB) {
        if (expression.getOperator() != ConditionalExpression.Operator.EQ
            && expression.getOperator() != ConditionalExpression.Operator.NE
            && expression.getOperator() != ConditionalExpression.Operator.IS_NULL
            && expression.getOperator() != ConditionalExpression.Operator.IS_NOT_NULL) {
          throw new IllegalArgumentException(
              CoreError.COSMOS_CONDITION_OPERATION_NOT_SUPPORTED_FOR_BLOB_TYPE.buildMessage(
                  mutation));
        }
      }
    }
  }
}
