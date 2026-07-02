package com.scalar.db.transaction.consensuscommit;

import com.scalar.db.api.Consistency;
import com.scalar.db.api.Get;
import com.scalar.db.api.GetBuilder;
import com.scalar.db.io.BigIntColumn;
import com.scalar.db.io.BlobColumn;
import com.scalar.db.io.BooleanColumn;
import com.scalar.db.io.DateColumn;
import com.scalar.db.io.DoubleColumn;
import com.scalar.db.io.FloatColumn;
import com.scalar.db.io.IntColumn;
import com.scalar.db.io.Key;
import com.scalar.db.io.TextColumn;
import com.scalar.db.io.TimeColumn;
import com.scalar.db.io.TimestampColumn;
import com.scalar.db.io.TimestampTZColumn;
import com.scalar.db.transaction.consensuscommit.proto.v1.Column;
import com.scalar.db.transaction.consensuscommit.proto.v1.Entry;
import com.scalar.db.util.TimeRelatedColumnEncodingUtils;

/**
 * Inverse of {@link WriteSetEncoder} — decodes a proto {@link Entry} into a {@link Get} for the
 * same record, so the recovery loop can read each user-table record back. The only
 * production-facing entry point is {@link #toGet(Entry)}.
 *
 * <p>The encoder side ({@link WriteSetEncoder}) uses {@link com.scalar.db.io.ColumnVisitor} against
 * {@link com.scalar.db.io.Column}; this decoder uses a {@code Column.getValueCase()} switch against
 * the proto {@link Column} because the proto API does not expose a visitor. The asymmetry is
 * intrinsic, not a pattern violation.
 *
 * <p>Currently used only for primary-key columns (partition and clustering keys), which are
 * non-nullable by ScalarDB's API contract. The decoder throws {@link AssertionError} on a missing
 * value to surface that invariant violation. If a future change uses this decoder for non-key
 * columns (e.g., a backup-window use case with {@code includeColumns=true}), those branches must
 * become recoverable or the call site must filter null-valued Columns first.
 */
final class WriteSetDecoder {
  private WriteSetDecoder() {}

  /**
   * Decodes a proto {@link Entry} into a {@link Get} for the same record.
   *
   * @param entry the proto Entry
   * @return a Get with namespace, table, partition key (and clustering key when present) populated,
   *     and {@link Consistency#LINEARIZABLE} set
   */
  static Get toGet(Entry entry) {
    GetBuilder.BuildableGetWithPartitionKey builder =
        Get.newBuilder()
            .namespace(entry.getNamespaceName())
            .table(entry.getTableName())
            .partitionKey(decodeKey(entry.getPartitionKey()))
            .consistency(Consistency.LINEARIZABLE);
    if (entry.hasClusteringKey()) {
      builder.clusteringKey(decodeKey(entry.getClusteringKey()));
    }
    return builder.build();
  }

  private static Key decodeKey(com.scalar.db.transaction.consensuscommit.proto.v1.Key protoKey) {
    Key.Builder builder = Key.newBuilder();
    for (Column protoColumn : protoKey.getColumnsList()) {
      builder.add(decodeColumn(protoColumn));
    }
    return builder.build();
  }

  // TODO: CbrlRestore.decodeColumnFromProto duplicates this proto->io column decoding (with
  // nullable
  // handling). Once the open PRs land, expose this (or a shared ProtoUtils) and have CbrlRestore
  // call it instead of keeping a parallel copy.
  private static com.scalar.db.io.Column<?> decodeColumn(Column protoColumn) {
    String name = protoColumn.getName();
    switch (protoColumn.getValueCase()) {
      case BOOLEAN_VALUE:
        Column.BooleanValue booleanValue = protoColumn.getBooleanValue();
        if (!booleanValue.hasValue()) {
          throw nullKeyAssertion(name);
        }
        return BooleanColumn.of(name, booleanValue.getValue());
      case INT_VALUE:
        Column.IntValue intValue = protoColumn.getIntValue();
        if (!intValue.hasValue()) {
          throw nullKeyAssertion(name);
        }
        return IntColumn.of(name, intValue.getValue());
      case BIGINT_VALUE:
        Column.BigIntValue bigIntValue = protoColumn.getBigintValue();
        if (!bigIntValue.hasValue()) {
          throw nullKeyAssertion(name);
        }
        return BigIntColumn.of(name, bigIntValue.getValue());
      case FLOAT_VALUE:
        Column.FloatValue floatValue = protoColumn.getFloatValue();
        if (!floatValue.hasValue()) {
          throw nullKeyAssertion(name);
        }
        return FloatColumn.of(name, floatValue.getValue());
      case DOUBLE_VALUE:
        Column.DoubleValue doubleValue = protoColumn.getDoubleValue();
        if (!doubleValue.hasValue()) {
          throw nullKeyAssertion(name);
        }
        return DoubleColumn.of(name, doubleValue.getValue());
      case TEXT_VALUE:
        Column.TextValue textValue = protoColumn.getTextValue();
        if (!textValue.hasValue()) {
          throw nullKeyAssertion(name);
        }
        return TextColumn.of(name, textValue.getValue());
      case BLOB_VALUE:
        Column.BlobValue blobValue = protoColumn.getBlobValue();
        if (!blobValue.hasValue()) {
          throw nullKeyAssertion(name);
        }
        return BlobColumn.of(name, blobValue.getValue().toByteArray());
      case DATE_VALUE:
        Column.DateValue dateValue = protoColumn.getDateValue();
        if (!dateValue.hasValue()) {
          throw nullKeyAssertion(name);
        }
        return DateColumn.of(name, TimeRelatedColumnEncodingUtils.decodeDate(dateValue.getValue()));
      case TIME_VALUE:
        Column.TimeValue timeValue = protoColumn.getTimeValue();
        if (!timeValue.hasValue()) {
          throw nullKeyAssertion(name);
        }
        return TimeColumn.of(name, TimeRelatedColumnEncodingUtils.decodeTime(timeValue.getValue()));
      case TIMESTAMP_VALUE:
        Column.TimestampValue timestampValue = protoColumn.getTimestampValue();
        if (!timestampValue.hasValue()) {
          throw nullKeyAssertion(name);
        }
        return TimestampColumn.of(
            name, TimeRelatedColumnEncodingUtils.decodeTimestamp(timestampValue.getValue()));
      case TIMESTAMPTZ_VALUE:
        Column.TimestampTZValue timestampTZValue = protoColumn.getTimestamptzValue();
        if (!timestampTZValue.hasValue()) {
          throw nullKeyAssertion(name);
        }
        return TimestampTZColumn.of(
            name, TimeRelatedColumnEncodingUtils.decodeTimestampTZ(timestampTZValue.getValue()));
      case VALUE_NOT_SET:
      default:
        throw new AssertionError(
            "Proto Column has no value set. Primary-key columns are non-nullable and the encoder "
                + "always writes a value oneof. Column: "
                + name);
    }
  }

  private static AssertionError nullKeyAssertion(String name) {
    return new AssertionError(
        "Proto Column carries a null primary-key value, but ScalarDB primary-key columns are "
            + "non-nullable by API contract. Column: "
            + name);
  }
}
