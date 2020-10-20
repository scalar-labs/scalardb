package com.scalar.dataloader.model;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.google.common.collect.Streams;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public class TableMetadataDeserializer extends StdDeserializer<TableMetadata> {

  public TableMetadataDeserializer() {
    this(null);
  }

  public TableMetadataDeserializer(Class<?> vc) {
    super(vc);
  }

  @Override
  public TableMetadata deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
    TableMetadata.TableMetadataBuilder tableMetadata = TableMetadata.builder();
    JsonNode node = jp.getCodec().readTree(jp);
    Entry<String, JsonNode> tableEntry =
        Streams.stream(node.fields())
            .filter(entry -> entry.getValue().isObject())
            .findFirst()
            .get();
    // Keyspace and name are separated by a '.'
    String[] keySpaceAndTableName = tableEntry.getKey().split("\\.");
    tableMetadata.keyspace(keySpaceAndTableName[0]);
    tableMetadata.tableName(keySpaceAndTableName[1]);
    JsonNode tableNode = tableEntry.getValue();
    JsonNode columns = tableNode.get("columns");
    // There may be no columns pattern
    JsonNode columnsPattern =
        tableNode.has("column_patterns") ? tableNode.get("column_patterns") : null;
    List<String> partitionKeyNames =
        Streams.stream(tableNode.get("partition-key").elements())
            .map(JsonNode::asText)
            .collect(Collectors.toList());
    // There may be no clustering keys
    List<String> clusteringKeyNames;
    if (tableNode.has("clustering-key")) {
      clusteringKeyNames =
          Streams.stream(tableNode.get("clustering-key").elements())
              .map(JsonNode::asText)
              .collect(Collectors.toList());
      tableMetadata.clusteringKeys(parseKeys(columns, columnsPattern, clusteringKeyNames));
    } else {
      clusteringKeyNames = Collections.emptyList();
      tableMetadata.clusteringKeys(Collections.emptyList());
    }
    tableMetadata.partitionsKeys(parseKeys(columns, columnsPattern, partitionKeyNames));

    tableMetadata.columns(
        parseColumns(columns, columnsPattern, partitionKeyNames, clusteringKeyNames));
    if (tableNode.has("transaction") && tableNode.get("transaction").asBoolean()) {
      tableMetadata.tableType(TableType.TRANSACTION);
    } else {
      tableMetadata.tableType(TableType.STORAGE);
    }

    return tableMetadata.build();
  }

  /**
   * Parse the partition key or clustering key
   *
   * @param columns
   * @param columnsPattern
   * @param keyNames
   * @return a list of paritition keys or clustering keys
   */
  private List<DataValue> parseKeys(
      JsonNode columns, @Nullable JsonNode columnsPattern, List<String> keyNames) {
    return keyNames.stream()
        .map(keyName -> buildDataValue(keyName, columns.get(keyName).asText(), columnsPattern))
        .collect(Collectors.toList());
  }

  /**
   * Parse the table columns that are not part of the primary key
   *
   * @param columns
   * @param columnsPattern
   * @param partitionKeyNames
   * @param clusteringKeyNames
   * @return
   */
  private List<DataValue> parseColumns(
      JsonNode columns,
      @Nullable JsonNode columnsPattern,
      List<String> partitionKeyNames,
      List<String> clusteringKeyNames) {
    return Streams.stream(columns.fields())
        .filter(
            c ->
                !partitionKeyNames.contains(c.getKey()) && !clusteringKeyNames.contains(c.getKey()))
        .map(c -> buildDataValue(c.getKey(), c.getValue().asText(), columnsPattern))
        .collect(Collectors.toList());
  }

  private DataValue buildDataValue(String name, String type, @Nullable JsonNode columnsPattern) {
    DataValue.DataValueBuilder dataValue =
        DataValue.builder().name(name).type(ValueType.valueOf(type));
    if (columnsPattern != null && columnsPattern.has(name)) {
      dataValue.pattern(columnsPattern.get(name).asText());
    }
    return dataValue.build();
  }
}
