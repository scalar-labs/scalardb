package com.scalar.db.dataloader.core.dataimport.controlfile;

import static com.scalar.db.dataloader.core.ErrorMessage.CONTROL_FILE_MISSING_DATA_MAPPINGS;
import static com.scalar.db.dataloader.core.ErrorMessage.DUPLICATE_DATA_MAPPINGS;
import static com.scalar.db.dataloader.core.ErrorMessage.ERROR_METHOD_NULL_ARGUMENT;
import static com.scalar.db.dataloader.core.ErrorMessage.MISSING_CLUSTERING_KEY;
import static com.scalar.db.dataloader.core.ErrorMessage.MISSING_COLUMN_MAPPING;
import static com.scalar.db.dataloader.core.ErrorMessage.MISSING_NAMESPACE_OR_TABLE;
import static com.scalar.db.dataloader.core.ErrorMessage.MISSING_PARTITION_KEY;
import static com.scalar.db.dataloader.core.ErrorMessage.MULTIPLE_MAPPINGS_FOR_COLUMN_FOUND;
import static com.scalar.db.dataloader.core.ErrorMessage.TARGET_COLUMN_NOT_FOUND;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.scalar.db.api.TableMetadata;
import com.scalar.db.dataloader.core.util.TableMetadataUtil;
import com.scalar.db.io.DataType;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ControlFileValidatorTest {

  private static final String TABLE_NAME = "table";

  private static final String TABLE_NAME_TWO = "table_two";
  private static final String NAMESPACE = "ns";
  private static final String COLUMN_PARTITION_KEY = "pk";
  private static final String COLUMN_CLUSTERING_KEY = "ck";
  private static final String COLUMN_ONE = "c1";

  @Test
  void validate_nullValuesGiven_shouldThrowNullPointerException() {
    assertThatThrownBy(() -> ControlFileValidator.validate(null, null, null))
        .isExactlyInstanceOf(NullPointerException.class)
        .hasMessage(ERROR_METHOD_NULL_ARGUMENT);
  }

  @Test
  void validate_noTableMappingsGiven_shouldThrowControlFileValidationException() {
    ControlFile controlFile = new ControlFile();
    Map<String, TableMetadata> tableMetadataMap = new HashMap<>();
    assertThatThrownBy(
            () ->
                ControlFileValidator.validate(
                    controlFile, ControlFileValidationLevel.FULL, tableMetadataMap))
        .isExactlyInstanceOf(ControlFileValidationException.class)
        .hasMessage(CONTROL_FILE_MISSING_DATA_MAPPINGS);
  }

  @Test
  void validate_duplicateTableMappingsGiven_shouldThrowControlFileValidationException() {
    ControlFile controlFile = new ControlFile();
    ControlFileTable controlFileTable = new ControlFileTable(NAMESPACE, TABLE_NAME);
    controlFile.getTables().add(controlFileTable);
    controlFile.getTables().add(controlFileTable);

    String lookupKey = TableMetadataUtil.getTableLookupKey(controlFileTable);

    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn(COLUMN_PARTITION_KEY, DataType.TEXT)
            .addPartitionKey(COLUMN_PARTITION_KEY)
            .build();

    Map<String, TableMetadata> tableMetadataMap = new HashMap<>();
    tableMetadataMap.put(lookupKey, tableMetadata);

    assertThatThrownBy(
            () ->
                ControlFileValidator.validate(
                    controlFile, ControlFileValidationLevel.MAPPED, tableMetadataMap))
        .isExactlyInstanceOf(ControlFileValidationException.class)
        .hasMessage(String.format(DUPLICATE_DATA_MAPPINGS, lookupKey));
  }

  @Test
  void validate_duplicateTableColumnMappingsGiven_shouldThrowControlFileValidationException() {
    ControlFile controlFile = new ControlFile();
    ControlFileTable controlFileTable = new ControlFileTable(NAMESPACE, TABLE_NAME);
    controlFileTable.getMappings().add(new ControlFileTableFieldMapping(COLUMN_ONE, COLUMN_ONE));
    controlFileTable.getMappings().add(new ControlFileTableFieldMapping(COLUMN_ONE, COLUMN_ONE));
    controlFile.getTables().add(controlFileTable);

    String lookupKey = TableMetadataUtil.getTableLookupKey(controlFileTable);

    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn(COLUMN_PARTITION_KEY, DataType.TEXT)
            .addPartitionKey(COLUMN_PARTITION_KEY)
            .build();

    Map<String, TableMetadata> tableMetadataMap = new HashMap<>();
    tableMetadataMap.put(lookupKey, tableMetadata);

    assertThatThrownBy(
            () ->
                ControlFileValidator.validate(
                    controlFile, ControlFileValidationLevel.MAPPED, tableMetadataMap))
        .isExactlyInstanceOf(ControlFileValidationException.class)
        .hasMessage(String.format(MULTIPLE_MAPPINGS_FOR_COLUMN_FOUND, COLUMN_ONE, lookupKey));
  }

  @Test
  void validate_missingTableMetadataGiven_shouldThrowControlFileValidationException() {
    ControlFile controlFile = new ControlFile();
    ControlFileTable controlFileTable = new ControlFileTable(NAMESPACE, TABLE_NAME);
    controlFileTable.getMappings().add(new ControlFileTableFieldMapping(COLUMN_ONE, COLUMN_ONE));
    controlFile.getTables().add(controlFileTable);

    Map<String, TableMetadata> tableMetadataMap = new HashMap<>();

    assertThatThrownBy(
            () ->
                ControlFileValidator.validate(
                    controlFile, ControlFileValidationLevel.MAPPED, tableMetadataMap))
        .isExactlyInstanceOf(ControlFileValidationException.class)
        .hasMessage(
            String.format(
                MISSING_NAMESPACE_OR_TABLE,
                controlFileTable.getNamespace(),
                controlFileTable.getTable()));
  }

  @Test
  void validate_nonExistingTargetColumnGiven_shouldThrowControlFileValidationException() {
    ControlFile controlFile = new ControlFile();
    ControlFileTable controlFileTable = new ControlFileTable(NAMESPACE, TABLE_NAME);
    controlFileTable.getMappings().add(new ControlFileTableFieldMapping(COLUMN_ONE, COLUMN_ONE));
    controlFile.getTables().add(controlFileTable);

    String lookupKey = TableMetadataUtil.getTableLookupKey(controlFileTable);

    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn(COLUMN_PARTITION_KEY, DataType.TEXT)
            .addPartitionKey(COLUMN_PARTITION_KEY)
            .build();
    Map<String, TableMetadata> tableMetadataMap = new HashMap<>();
    tableMetadataMap.put(lookupKey, tableMetadata);

    assertThatThrownBy(
            () ->
                ControlFileValidator.validate(
                    controlFile, ControlFileValidationLevel.MAPPED, tableMetadataMap))
        .isExactlyInstanceOf(ControlFileValidationException.class)
        .hasMessage(String.format(TARGET_COLUMN_NOT_FOUND, COLUMN_ONE, COLUMN_ONE, lookupKey));
  }

  @Test
  void
      validate_fullValidationAndHasMissingMappedColumnsGiven_shouldThrowControlFileValidationException() {
    ControlFile controlFile = new ControlFile();
    ControlFileTable controlFileTable = new ControlFileTable(NAMESPACE, TABLE_NAME);
    controlFileTable.getMappings().add(new ControlFileTableFieldMapping(COLUMN_ONE, COLUMN_ONE));
    controlFile.getTables().add(controlFileTable);

    String lookupKey = TableMetadataUtil.getTableLookupKey(controlFileTable);

    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn(COLUMN_PARTITION_KEY, DataType.TEXT)
            .addColumn(COLUMN_ONE, DataType.TEXT)
            .addPartitionKey(COLUMN_PARTITION_KEY)
            .build();
    Map<String, TableMetadata> tableMetadataMap = new HashMap<>();
    tableMetadataMap.put(lookupKey, tableMetadata);

    assertThatThrownBy(
            () ->
                ControlFileValidator.validate(
                    controlFile, ControlFileValidationLevel.FULL, tableMetadataMap))
        .isExactlyInstanceOf(ControlFileValidationException.class)
        .hasMessage(String.format(MISSING_COLUMN_MAPPING, COLUMN_PARTITION_KEY, lookupKey));
  }

  @Test
  void
      validate_keysValidationAndHasMissingMappedPartitionKeysGiven_shouldThrowControlFileValidationException() {
    ControlFile controlFile = new ControlFile();
    ControlFileTable controlFileTable = new ControlFileTable(NAMESPACE, TABLE_NAME);
    controlFileTable.getMappings().add(new ControlFileTableFieldMapping(COLUMN_ONE, COLUMN_ONE));
    controlFile.getTables().add(controlFileTable);

    String lookupKey = TableMetadataUtil.getTableLookupKey(controlFileTable);

    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn(COLUMN_PARTITION_KEY, DataType.TEXT)
            .addColumn(COLUMN_ONE, DataType.TEXT)
            .addPartitionKey(COLUMN_PARTITION_KEY)
            .build();
    Map<String, TableMetadata> tableMetadataMap = new HashMap<>();
    tableMetadataMap.put(lookupKey, tableMetadata);

    assertThatThrownBy(
            () ->
                ControlFileValidator.validate(
                    controlFile, ControlFileValidationLevel.KEYS, tableMetadataMap))
        .isExactlyInstanceOf(ControlFileValidationException.class)
        .hasMessage(String.format(MISSING_PARTITION_KEY, COLUMN_PARTITION_KEY, lookupKey));
  }

  @Test
  void
      validate_keysValidationAndHasMissingMappedClusteringKeysGiven_shouldThrowControlFileValidationException() {
    ControlFile controlFile = new ControlFile();
    ControlFileTable controlFileTable = new ControlFileTable(NAMESPACE, TABLE_NAME);
    controlFileTable
        .getMappings()
        .add(new ControlFileTableFieldMapping(COLUMN_PARTITION_KEY, COLUMN_PARTITION_KEY));
    controlFileTable.getMappings().add(new ControlFileTableFieldMapping(COLUMN_ONE, COLUMN_ONE));
    controlFile.getTables().add(controlFileTable);

    String lookupKey = TableMetadataUtil.getTableLookupKey(controlFileTable);

    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn(COLUMN_PARTITION_KEY, DataType.TEXT)
            .addColumn(COLUMN_CLUSTERING_KEY, DataType.TEXT)
            .addColumn(COLUMN_ONE, DataType.TEXT)
            .addPartitionKey(COLUMN_PARTITION_KEY)
            .addClusteringKey(COLUMN_CLUSTERING_KEY)
            .build();
    Map<String, TableMetadata> tableMetadataMap = new HashMap<>();
    tableMetadataMap.put(lookupKey, tableMetadata);

    assertThatThrownBy(
            () ->
                ControlFileValidator.validate(
                    controlFile, ControlFileValidationLevel.KEYS, tableMetadataMap))
        .isExactlyInstanceOf(ControlFileValidationException.class)
        .hasMessage(String.format(MISSING_CLUSTERING_KEY, COLUMN_CLUSTERING_KEY, lookupKey));
  }

  @Test
  void validate_mappedValidationAndValidArgumentsGiven_shouldNotThrowException()
      throws ControlFileValidationException {
    ControlFile controlFile = new ControlFile();
    ControlFileTable controlFileTable = new ControlFileTable(NAMESPACE, TABLE_NAME);
    controlFileTable.getMappings().add(new ControlFileTableFieldMapping(COLUMN_ONE, COLUMN_ONE));
    controlFile.getTables().add(controlFileTable);

    String lookupKey = TableMetadataUtil.getTableLookupKey(controlFileTable);

    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn(COLUMN_PARTITION_KEY, DataType.TEXT)
            .addColumn(COLUMN_CLUSTERING_KEY, DataType.TEXT)
            .addColumn(COLUMN_ONE, DataType.TEXT)
            .addPartitionKey(COLUMN_PARTITION_KEY)
            .addClusteringKey(COLUMN_CLUSTERING_KEY)
            .build();
    Map<String, TableMetadata> tableMetadataMap = new HashMap<>();
    tableMetadataMap.put(lookupKey, tableMetadata);

    ControlFileValidator.validate(controlFile, ControlFileValidationLevel.MAPPED, tableMetadataMap);
  }

  @Test
  void validate_keysValidationAndValidArgumentsGiven_shouldNotThrowException()
      throws ControlFileValidationException {
    ControlFile controlFile = new ControlFile();
    ControlFileTable controlFileTable = new ControlFileTable(NAMESPACE, TABLE_NAME);
    controlFileTable
        .getMappings()
        .add(new ControlFileTableFieldMapping(COLUMN_PARTITION_KEY, COLUMN_PARTITION_KEY));
    controlFileTable
        .getMappings()
        .add(new ControlFileTableFieldMapping(COLUMN_CLUSTERING_KEY, COLUMN_CLUSTERING_KEY));
    controlFile.getTables().add(controlFileTable);

    String lookupKey = TableMetadataUtil.getTableLookupKey(controlFileTable);

    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn(COLUMN_PARTITION_KEY, DataType.TEXT)
            .addColumn(COLUMN_CLUSTERING_KEY, DataType.TEXT)
            .addColumn(COLUMN_ONE, DataType.TEXT)
            .addPartitionKey(COLUMN_PARTITION_KEY)
            .addClusteringKey(COLUMN_CLUSTERING_KEY)
            .build();
    Map<String, TableMetadata> tableMetadataMap = new HashMap<>();
    tableMetadataMap.put(lookupKey, tableMetadata);

    ControlFileValidator.validate(controlFile, ControlFileValidationLevel.MAPPED, tableMetadataMap);
  }

  @Test
  void validate_fullValidationAndValidArgumentsGiven_shouldNotThrowException()
      throws ControlFileValidationException {
    ControlFile controlFile = new ControlFile();
    ControlFileTable controlFileTable = new ControlFileTable(NAMESPACE, TABLE_NAME);
    controlFileTable
        .getMappings()
        .add(new ControlFileTableFieldMapping(COLUMN_PARTITION_KEY, COLUMN_PARTITION_KEY));
    controlFileTable
        .getMappings()
        .add(new ControlFileTableFieldMapping(COLUMN_CLUSTERING_KEY, COLUMN_CLUSTERING_KEY));
    controlFileTable.getMappings().add(new ControlFileTableFieldMapping(COLUMN_ONE, COLUMN_ONE));
    controlFile.getTables().add(controlFileTable);

    String lookupKey = TableMetadataUtil.getTableLookupKey(controlFileTable);

    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn(COLUMN_PARTITION_KEY, DataType.TEXT)
            .addColumn(COLUMN_CLUSTERING_KEY, DataType.TEXT)
            .addColumn(COLUMN_ONE, DataType.TEXT)
            .addPartitionKey(COLUMN_PARTITION_KEY)
            .addClusteringKey(COLUMN_CLUSTERING_KEY)
            .build();
    Map<String, TableMetadata> tableMetadataMap = new HashMap<>();
    tableMetadataMap.put(lookupKey, tableMetadata);

    ControlFileValidator.validate(controlFile, ControlFileValidationLevel.FULL, tableMetadataMap);
  }

  @Test
  void
      validate_twoControlFileTablesAndFullValidationAndHasMissingMappedColumnsGiven_shouldThrowControlFileValidationException() {
    ControlFile controlFile = new ControlFile();
    ControlFileTable controlFileTable = new ControlFileTable(NAMESPACE, TABLE_NAME);
    controlFileTable
        .getMappings()
        .add(new ControlFileTableFieldMapping(COLUMN_PARTITION_KEY, COLUMN_PARTITION_KEY));
    controlFileTable
        .getMappings()
        .add(new ControlFileTableFieldMapping(COLUMN_CLUSTERING_KEY, COLUMN_CLUSTERING_KEY));
    controlFileTable.getMappings().add(new ControlFileTableFieldMapping(COLUMN_ONE, COLUMN_ONE));
    controlFile.getTables().add(controlFileTable);
    ControlFileTable controlFileTable2 = new ControlFileTable(NAMESPACE, TABLE_NAME_TWO);
    controlFileTable2
        .getMappings()
        .add(new ControlFileTableFieldMapping(COLUMN_PARTITION_KEY, COLUMN_PARTITION_KEY));
    controlFileTable2
        .getMappings()
        .add(new ControlFileTableFieldMapping(COLUMN_CLUSTERING_KEY, COLUMN_CLUSTERING_KEY));
    controlFile.getTables().add(controlFileTable2);

    String lookupKey = TableMetadataUtil.getTableLookupKey(controlFileTable);
    String lookupKeyTwo = TableMetadataUtil.getTableLookupKey(controlFileTable2);

    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn(COLUMN_PARTITION_KEY, DataType.TEXT)
            .addColumn(COLUMN_CLUSTERING_KEY, DataType.TEXT)
            .addColumn(COLUMN_ONE, DataType.TEXT)
            .addPartitionKey(COLUMN_PARTITION_KEY)
            .addClusteringKey(COLUMN_CLUSTERING_KEY)
            .build();
    Map<String, TableMetadata> tableMetadataMap = new HashMap<>();
    tableMetadataMap.put(lookupKey, tableMetadata);
    tableMetadataMap.put(lookupKeyTwo, tableMetadata);

    assertThatThrownBy(
            () ->
                ControlFileValidator.validate(
                    controlFile, ControlFileValidationLevel.FULL, tableMetadataMap))
        .isExactlyInstanceOf(ControlFileValidationException.class)
        .hasMessage(String.format(MISSING_COLUMN_MAPPING, COLUMN_ONE, lookupKeyTwo));
  }

  @Test
  void
      validate_twoControlFileTablesAndKeysValidationAndHasMissingMappedColumnsGiven_shouldThrowControlFileValidationException() {
    ControlFile controlFile = new ControlFile();
    ControlFileTable controlFileTable = new ControlFileTable(NAMESPACE, TABLE_NAME);
    controlFileTable
        .getMappings()
        .add(new ControlFileTableFieldMapping(COLUMN_PARTITION_KEY, COLUMN_PARTITION_KEY));
    controlFileTable
        .getMappings()
        .add(new ControlFileTableFieldMapping(COLUMN_CLUSTERING_KEY, COLUMN_CLUSTERING_KEY));
    controlFile.getTables().add(controlFileTable);
    ControlFileTable controlFileTable2 = new ControlFileTable(NAMESPACE, TABLE_NAME_TWO);
    controlFileTable2
        .getMappings()
        .add(new ControlFileTableFieldMapping(COLUMN_PARTITION_KEY, COLUMN_PARTITION_KEY));
    controlFile.getTables().add(controlFileTable2);

    String lookupKey = TableMetadataUtil.getTableLookupKey(controlFileTable);
    String lookupKeyTwo = TableMetadataUtil.getTableLookupKey(controlFileTable2);

    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn(COLUMN_PARTITION_KEY, DataType.TEXT)
            .addColumn(COLUMN_CLUSTERING_KEY, DataType.TEXT)
            .addColumn(COLUMN_ONE, DataType.TEXT)
            .addPartitionKey(COLUMN_PARTITION_KEY)
            .addClusteringKey(COLUMN_CLUSTERING_KEY)
            .build();
    Map<String, TableMetadata> tableMetadataMap = new HashMap<>();
    tableMetadataMap.put(lookupKey, tableMetadata);
    tableMetadataMap.put(lookupKeyTwo, tableMetadata);

    assertThatThrownBy(
            () ->
                ControlFileValidator.validate(
                    controlFile, ControlFileValidationLevel.KEYS, tableMetadataMap))
        .isExactlyInstanceOf(ControlFileValidationException.class)
        .hasMessage(String.format(MISSING_CLUSTERING_KEY, COLUMN_CLUSTERING_KEY, lookupKeyTwo));
  }

  @Test
  void
      validate_twoControlFileTablesAndMappedValidationAndHasMissingMappedColumnsInOneTableGiven_shouldThrowControlFileValidationException() {
    ControlFile controlFile = new ControlFile();
    ControlFileTable controlFileTable = new ControlFileTable(NAMESPACE, TABLE_NAME);
    controlFileTable
        .getMappings()
        .add(new ControlFileTableFieldMapping(COLUMN_PARTITION_KEY, COLUMN_PARTITION_KEY));
    controlFileTable
        .getMappings()
        .add(new ControlFileTableFieldMapping(COLUMN_CLUSTERING_KEY, COLUMN_CLUSTERING_KEY));
    controlFile.getTables().add(controlFileTable);
    ControlFileTable controlFileTable2 = new ControlFileTable(NAMESPACE, TABLE_NAME_TWO);
    controlFile.getTables().add(controlFileTable2);

    String lookupKey = TableMetadataUtil.getTableLookupKey(controlFileTable);
    String lookupKeyTwo = TableMetadataUtil.getTableLookupKey(controlFileTable2);

    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn(COLUMN_PARTITION_KEY, DataType.TEXT)
            .addColumn(COLUMN_CLUSTERING_KEY, DataType.TEXT)
            .addColumn(COLUMN_ONE, DataType.TEXT)
            .addPartitionKey(COLUMN_PARTITION_KEY)
            .addClusteringKey(COLUMN_CLUSTERING_KEY)
            .build();
    Map<String, TableMetadata> tableMetadataMap = new HashMap<>();
    tableMetadataMap.put(lookupKey, tableMetadata);
    tableMetadataMap.put(lookupKeyTwo, tableMetadata);

    assertThatThrownBy(
            () ->
                ControlFileValidator.validate(
                    controlFile, ControlFileValidationLevel.KEYS, tableMetadataMap))
        .isExactlyInstanceOf(ControlFileValidationException.class)
        .hasMessage(String.format(MISSING_PARTITION_KEY, COLUMN_PARTITION_KEY, lookupKeyTwo));
  }

  @Test
  void
      validate_twoControlFileTablesAndFullValidationAndHasValidArgumentsGiven_shouldNotThrowException()
          throws ControlFileValidationException {
    ControlFile controlFile = new ControlFile();
    ControlFileTable controlFileTable = new ControlFileTable(NAMESPACE, TABLE_NAME);
    controlFileTable
        .getMappings()
        .add(new ControlFileTableFieldMapping(COLUMN_PARTITION_KEY, COLUMN_PARTITION_KEY));
    controlFileTable
        .getMappings()
        .add(new ControlFileTableFieldMapping(COLUMN_CLUSTERING_KEY, COLUMN_CLUSTERING_KEY));
    controlFileTable.getMappings().add(new ControlFileTableFieldMapping(COLUMN_ONE, COLUMN_ONE));
    controlFile.getTables().add(controlFileTable);
    ControlFileTable controlFileTable2 = new ControlFileTable(NAMESPACE, TABLE_NAME_TWO);
    controlFileTable2
        .getMappings()
        .add(new ControlFileTableFieldMapping(COLUMN_PARTITION_KEY, COLUMN_PARTITION_KEY));
    controlFileTable2
        .getMappings()
        .add(new ControlFileTableFieldMapping(COLUMN_CLUSTERING_KEY, COLUMN_CLUSTERING_KEY));
    controlFileTable2.getMappings().add(new ControlFileTableFieldMapping(COLUMN_ONE, COLUMN_ONE));
    controlFile.getTables().add(controlFileTable2);

    String lookupKey = TableMetadataUtil.getTableLookupKey(controlFileTable);
    String lookupKeyTwo = TableMetadataUtil.getTableLookupKey(controlFileTable2);

    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn(COLUMN_PARTITION_KEY, DataType.TEXT)
            .addColumn(COLUMN_CLUSTERING_KEY, DataType.TEXT)
            .addColumn(COLUMN_ONE, DataType.TEXT)
            .addPartitionKey(COLUMN_PARTITION_KEY)
            .addClusteringKey(COLUMN_CLUSTERING_KEY)
            .build();
    Map<String, TableMetadata> tableMetadataMap = new HashMap<>();
    tableMetadataMap.put(lookupKey, tableMetadata);
    tableMetadataMap.put(lookupKeyTwo, tableMetadata);

    ControlFileValidator.validate(controlFile, ControlFileValidationLevel.FULL, tableMetadataMap);
  }

  @Test
  void
      validate_twoControlFileTablesAndKeysValidationAndHasValidArgumentsGiven_shouldNotThrowException()
          throws ControlFileValidationException {
    ControlFile controlFile = new ControlFile();
    ControlFileTable controlFileTable = new ControlFileTable(NAMESPACE, TABLE_NAME);
    controlFileTable
        .getMappings()
        .add(new ControlFileTableFieldMapping(COLUMN_PARTITION_KEY, COLUMN_PARTITION_KEY));
    controlFileTable
        .getMappings()
        .add(new ControlFileTableFieldMapping(COLUMN_CLUSTERING_KEY, COLUMN_CLUSTERING_KEY));
    controlFile.getTables().add(controlFileTable);
    ControlFileTable controlFileTable2 = new ControlFileTable(NAMESPACE, TABLE_NAME_TWO);
    controlFileTable2
        .getMappings()
        .add(new ControlFileTableFieldMapping(COLUMN_PARTITION_KEY, COLUMN_PARTITION_KEY));
    controlFile.getTables().add(controlFileTable2);
    controlFileTable2
        .getMappings()
        .add(new ControlFileTableFieldMapping(COLUMN_CLUSTERING_KEY, COLUMN_CLUSTERING_KEY));

    String lookupKey = TableMetadataUtil.getTableLookupKey(controlFileTable);
    String lookupKeyTwo = TableMetadataUtil.getTableLookupKey(controlFileTable2);

    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn(COLUMN_PARTITION_KEY, DataType.TEXT)
            .addColumn(COLUMN_CLUSTERING_KEY, DataType.TEXT)
            .addColumn(COLUMN_ONE, DataType.TEXT)
            .addPartitionKey(COLUMN_PARTITION_KEY)
            .addClusteringKey(COLUMN_CLUSTERING_KEY)
            .build();
    Map<String, TableMetadata> tableMetadataMap = new HashMap<>();
    tableMetadataMap.put(lookupKey, tableMetadata);
    tableMetadataMap.put(lookupKeyTwo, tableMetadata);

    ControlFileValidator.validate(controlFile, ControlFileValidationLevel.KEYS, tableMetadataMap);
  }

  @Test
  void
      validate_twoControlFileTablesAndMappedValidationAndHasValidArgumentsGiven_shouldNotThrowException()
          throws ControlFileValidationException {
    ControlFile controlFile = new ControlFile();
    ControlFileTable controlFileTable = new ControlFileTable(NAMESPACE, TABLE_NAME);
    controlFileTable
        .getMappings()
        .add(new ControlFileTableFieldMapping(COLUMN_PARTITION_KEY, COLUMN_PARTITION_KEY));
    controlFileTable.getMappings().add(new ControlFileTableFieldMapping(COLUMN_ONE, COLUMN_ONE));
    controlFile.getTables().add(controlFileTable);
    ControlFileTable controlFileTable2 = new ControlFileTable(NAMESPACE, TABLE_NAME_TWO);
    controlFileTable2
        .getMappings()
        .add(new ControlFileTableFieldMapping(COLUMN_PARTITION_KEY, COLUMN_PARTITION_KEY));
    controlFileTable2.getMappings().add(new ControlFileTableFieldMapping(COLUMN_ONE, COLUMN_ONE));
    controlFile.getTables().add(controlFileTable2);

    String lookupKey = TableMetadataUtil.getTableLookupKey(controlFileTable);
    String lookupKeyTwo = TableMetadataUtil.getTableLookupKey(controlFileTable2);

    TableMetadata tableMetadata =
        TableMetadata.newBuilder()
            .addColumn(COLUMN_PARTITION_KEY, DataType.TEXT)
            .addColumn(COLUMN_CLUSTERING_KEY, DataType.TEXT)
            .addColumn(COLUMN_ONE, DataType.TEXT)
            .addPartitionKey(COLUMN_PARTITION_KEY)
            .addClusteringKey(COLUMN_CLUSTERING_KEY)
            .build();
    Map<String, TableMetadata> tableMetadataMap = new HashMap<>();
    tableMetadataMap.put(lookupKey, tableMetadata);
    tableMetadataMap.put(lookupKeyTwo, tableMetadata);

    ControlFileValidator.validate(controlFile, ControlFileValidationLevel.MAPPED, tableMetadataMap);
  }
}
