package com.scalar.db.transaction.consensuscommit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.io.DataType;
import com.scalar.db.util.groupcommit.GroupCommitConfig;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public final class ConsensusCommitUtils {

  private static final ImmutableMap<String, DataType> BEFORE_IMAGE_META_COLUMNS =
      ImmutableMap.<String, DataType>builder()
          .put(Attribute.BEFORE_ID, DataType.TEXT)
          .put(Attribute.BEFORE_STATE, DataType.INT)
          .put(Attribute.BEFORE_VERSION, DataType.INT)
          .put(Attribute.BEFORE_PREPARED_AT, DataType.BIGINT)
          .put(Attribute.BEFORE_COMMITTED_AT, DataType.BIGINT)
          .build();

  private static final ImmutableMap<String, DataType> AFTER_IMAGE_META_COLUMNS =
      ImmutableMap.<String, DataType>builder()
          .put(Attribute.ID, DataType.TEXT)
          .put(Attribute.STATE, DataType.INT)
          .put(Attribute.VERSION, DataType.INT)
          .put(Attribute.PREPARED_AT, DataType.BIGINT)
          .put(Attribute.COMMITTED_AT, DataType.BIGINT)
          .build();

  private static final ImmutableMap<String, DataType> TRANSACTION_META_COLUMNS =
      ImmutableMap.<String, DataType>builder()
          .putAll(AFTER_IMAGE_META_COLUMNS)
          .putAll(BEFORE_IMAGE_META_COLUMNS)
          .build();

  private ConsensusCommitUtils() {}

  /**
   * Builds a transaction table metadata based on the specified table metadata.
   *
   * @param tableMetadata the base table metadata to build a transaction table metadata
   * @return a transaction table metadata based on the table metadata
   */
  public static TableMetadata buildTransactionTableMetadata(TableMetadata tableMetadata) {
    checkIsNotTransactionMetaColumn(tableMetadata.getColumnNames());
    Set<String> nonPrimaryKeyColumns = getNonPrimaryKeyColumns(tableMetadata);
    checkBeforeColumnsDoNotAlreadyExist(nonPrimaryKeyColumns, tableMetadata);

    // Build a transaction table metadata
    TableMetadata.Builder builder = TableMetadata.newBuilder(tableMetadata);
    TRANSACTION_META_COLUMNS.forEach(builder::addColumn);
    nonPrimaryKeyColumns.forEach(
        c -> builder.addColumn(Attribute.BEFORE_PREFIX + c, tableMetadata.getColumnDataType(c)));
    return builder.build();
  }

  private static void checkIsNotTransactionMetaColumn(Set<String> columnNames) {
    TRANSACTION_META_COLUMNS
        .keySet()
        .forEach(
            c -> {
              if (columnNames.contains(c)) {
                throw new IllegalArgumentException(
                    CoreError.CONSENSUS_COMMIT_COLUMN_RESERVED_AS_TRANSACTION_METADATA.buildMessage(
                        c));
              }
            });
  }

  private static void checkBeforeColumnsDoNotAlreadyExist(
      Set<String> nonPrimaryKeyColumns, TableMetadata tableMetadata) {
    nonPrimaryKeyColumns.forEach(
        c -> {
          String beforePrefixed = Attribute.BEFORE_PREFIX + c;
          if (tableMetadata.getColumnNames().contains(beforePrefixed)) {
            throw new IllegalArgumentException(
                CoreError
                    .CONSENSUS_COMMIT_BEFORE_PREFIXED_COLUMN_FOR_NON_PRIMARY_KEY_RESERVED_AS_TRANSACTION_METADATA
                    .buildMessage(beforePrefixed));
          }
        });
  }

  public static String getBeforeImageColumnName(String columnName, TableMetadata tableMetadata) {
    checkIsNotTransactionMetaColumn(Collections.singleton(columnName));
    checkBeforeColumnsDoNotAlreadyExist(Collections.singleton(columnName), tableMetadata);

    return Attribute.BEFORE_PREFIX + columnName;
  }

  /**
   * Returns whether the specified table metadata is transactional.
   *
   * <p>This method checks all the transaction meta columns including the before prefix column, and
   * if any of them is missing, it returns false.
   *
   * @param tableMetadata a table metadata
   * @return whether the table metadata is transactional
   */
  public static boolean isTransactionTableMetadata(TableMetadata tableMetadata) {
    // if the table metadata doesn't have the transaction meta columns, it's not transactional
    for (String column : TRANSACTION_META_COLUMNS.keySet()) {
      if (!tableMetadata.getColumnNames().contains(column)) {
        return false;
      }
    }

    // if the table metadata doesn't have the before prefix columns, it's not transactional
    for (String nonPrimaryKeyColumn : getNonPrimaryKeyColumns(tableMetadata)) {
      if (TRANSACTION_META_COLUMNS.containsKey(nonPrimaryKeyColumn)) {
        continue;
      }
      // check if a column that has either the following name exists or not:
      //   - "before_" + the column name
      //   - the column name without the "before_" prefix
      // if both columns don't exist, the table metadata is not transactional
      if (!tableMetadata.getColumnNames().contains(Attribute.BEFORE_PREFIX + nonPrimaryKeyColumn)
          && !(nonPrimaryKeyColumn.startsWith(Attribute.BEFORE_PREFIX)
              && tableMetadata
                  .getColumnNames()
                  .contains(nonPrimaryKeyColumn.substring(Attribute.BEFORE_PREFIX.length())))) {
        return false;
      }
    }
    return true;
  }

  /**
   * Get non-primary key columns from the specified table metadata.
   *
   * @param tableMetadata a table metadata
   * @return a set of non-primary key column names
   */
  public static Set<String> getNonPrimaryKeyColumns(TableMetadata tableMetadata) {
    return tableMetadata.getColumnNames().stream()
        .filter(c -> !tableMetadata.getPartitionKeyNames().contains(c))
        .filter(c -> !tableMetadata.getClusteringKeyNames().contains(c))
        .collect(Collectors.toSet());
  }

  /**
   * Get transaction meta columns.
   *
   * @return a map of transaction meta columns
   */
  public static Map<String, DataType> getTransactionMetaColumns() {
    return TRANSACTION_META_COLUMNS;
  }

  /**
   * Removes transaction meta columns from the specified table metadata.
   *
   * @param tableMetadata a transaction table metadata
   * @return a table metadata without transaction meta columns
   */
  public static TableMetadata removeTransactionMetaColumns(TableMetadata tableMetadata) {
    Set<String> transactionMetaColumns = new HashSet<>(TRANSACTION_META_COLUMNS.keySet());
    transactionMetaColumns.addAll(
        tableMetadata.getColumnNames().stream()
            .filter(c -> c.startsWith(Attribute.BEFORE_PREFIX))
            .filter(
                c ->
                    tableMetadata
                        .getColumnNames()
                        .contains(c.substring(Attribute.BEFORE_PREFIX.length())))
            .collect(Collectors.toSet()));

    TableMetadata.Builder builder = TableMetadata.newBuilder();
    tableMetadata.getPartitionKeyNames().forEach(builder::addPartitionKey);
    tableMetadata
        .getClusteringKeyNames()
        .forEach(c -> builder.addClusteringKey(c, tableMetadata.getClusteringOrder(c)));
    tableMetadata.getColumnNames().stream()
        .filter(c -> !transactionMetaColumns.contains(c))
        .forEach(c -> builder.addColumn(c, tableMetadata.getColumnDataType(c)));
    tableMetadata.getSecondaryIndexNames().forEach(builder::addSecondaryIndex);
    return builder.build();
  }

  /**
   * Returns whether the specified column is a transaction meta column or not.
   *
   * @param columnName a column name
   * @param tableMetadata a transaction table metadata
   * @return whether the specified column is a transaction meta column or not
   */
  public static boolean isTransactionMetaColumn(String columnName, TableMetadata tableMetadata) {
    return AFTER_IMAGE_META_COLUMNS.containsKey(columnName)
        || isBeforeImageColumn(columnName, tableMetadata);
  }

  /**
   * Returns whether the specified column is a part of the before image columns or not.
   *
   * @param columnName a column name
   * @param tableMetadata a transaction table metadata
   * @return whether the specified column is a part of the before image columns or not
   */
  public static boolean isBeforeImageColumn(String columnName, TableMetadata tableMetadata) {
    if (!tableMetadata.getColumnNames().contains(columnName)
        || tableMetadata.getPartitionKeyNames().contains(columnName)
        || tableMetadata.getClusteringKeyNames().contains(columnName)) {
      return false;
    }

    if (BEFORE_IMAGE_META_COLUMNS.containsKey(columnName)) {
      return true;
    }
    if (columnName.startsWith(Attribute.BEFORE_PREFIX)) {
      // if the column name without the "before_" prefix exists, it's a part of the before image
      // columns
      return tableMetadata
          .getColumnNames()
          .contains(columnName.substring(Attribute.BEFORE_PREFIX.length()));
    }
    return false;
  }

  /**
   * Returns whether the specified column is a part of the after image columns or not.
   *
   * @param columnName a column name
   * @param tableMetadata a transaction table metadata
   * @return whether the specified column is a part of the after image columns or not
   */
  public static boolean isAfterImageColumn(String columnName, TableMetadata tableMetadata) {
    if (!tableMetadata.getColumnNames().contains(columnName)) {
      return false;
    }
    return !isBeforeImageColumn(columnName, tableMetadata);
  }

  public static Optional<CoordinatorGroupCommitter> createGroupCommitter(
      ConsensusCommitConfig config) {
    if (config.isCoordinatorGroupCommitEnabled()) {
      return Optional.of(new CoordinatorGroupCommitter(config));
    } else {
      return Optional.empty();
    }
  }

  ////////////// For group commit >>>>>>>>>>>>>>>>>
  // TODO: Remove these ones
  private static final String ENV_VAR_COORDINATOR_GROUP_COMMIT_ENABLED =
      "LOG_RECORDER_COORDINATOR_GROUP_COMMIT_ENABLED";
  private static final String ENV_VAR_COORDINATOR_GROUP_COMMIT_SLOT_CAPACITY =
      "LOG_RECORDER_COORDINATOR_GROUP_COMMIT_NUM_OF_RETENTION_VALUES";
  private static final String ENV_VAR_COORDINATOR_GROUP_COMMIT_GROUP_CLOSE_TIMEOUT_MILLIS =
      "LOG_RECORDER_COORDINATOR_GROUP_COMMIT_SIZEFIX_EXPIRATION_IN_MILLIS";
  private static final String ENV_VAR_COORDINATOR_GROUP_COMMIT_DELAYED_SLOT_MOVE_TIMEOUT_MILLIS =
      "LOG_RECORDER_COORDINATOR_GROUP_COMMIT_TIMEOUT_EXPIRATION_IN_MILLIS";
  private static final String ENV_VAR_COORDINATOR_GROUP_COMMIT_TIMEOUT_CHECK_INTERVAL_MILLIS =
      "LOG_RECORDER_COORDINATOR_GROUP_COMMIT_EXPIRATION_CHECK_INTERVAL_IN_MILLIS";

  @VisibleForTesting
  public static Optional<CoordinatorGroupCommitter> prepareGroupCommitterFromEnvVar() {
    // TODO: Make this configurable
    // TODO: Take care of lazy recovery
    if (!"true".equalsIgnoreCase(System.getenv(ENV_VAR_COORDINATOR_GROUP_COMMIT_ENABLED))) {
      return Optional.empty();
    }

    int groupCommitSlotCapacity = 32;
    if (System.getenv(ENV_VAR_COORDINATOR_GROUP_COMMIT_SLOT_CAPACITY) != null) {
      groupCommitSlotCapacity =
          Integer.parseInt(System.getenv(ENV_VAR_COORDINATOR_GROUP_COMMIT_SLOT_CAPACITY));
    }

    int groupCommitGroupCloseTimeoutMillis = 200;
    if (System.getenv(ENV_VAR_COORDINATOR_GROUP_COMMIT_GROUP_CLOSE_TIMEOUT_MILLIS) != null) {
      groupCommitGroupCloseTimeoutMillis =
          Integer.parseInt(
              System.getenv(ENV_VAR_COORDINATOR_GROUP_COMMIT_GROUP_CLOSE_TIMEOUT_MILLIS));
    }

    int groupCommitDelayedSlotMoveTimeoutMillis = 2000;
    if (System.getenv(ENV_VAR_COORDINATOR_GROUP_COMMIT_DELAYED_SLOT_MOVE_TIMEOUT_MILLIS) != null) {
      groupCommitDelayedSlotMoveTimeoutMillis =
          Integer.parseInt(
              System.getenv(ENV_VAR_COORDINATOR_GROUP_COMMIT_DELAYED_SLOT_MOVE_TIMEOUT_MILLIS));
    }

    int groupCommitTimeoutCheckIntervalMillis = 20;
    if (System.getenv(ENV_VAR_COORDINATOR_GROUP_COMMIT_TIMEOUT_CHECK_INTERVAL_MILLIS) != null) {
      groupCommitTimeoutCheckIntervalMillis =
          Integer.parseInt(
              System.getenv(ENV_VAR_COORDINATOR_GROUP_COMMIT_TIMEOUT_CHECK_INTERVAL_MILLIS));
    }

    return Optional.of(
        new CoordinatorGroupCommitter(
            new GroupCommitConfig(
                groupCommitGroupCloseTimeoutMillis,
                groupCommitDelayedSlotMoveTimeoutMillis,
                groupCommitSlotCapacity,
                groupCommitTimeoutCheckIntervalMillis)));
  }
}
