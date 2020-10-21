package com.scalar.dataloader.model;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.List;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
@JsonDeserialize(using = TableMetadataDeserializer.class)
public class TableMetadata {
  TableType tableType;
  String tableName;
  String keyspace;
  List<DataValue> partitionsKeys;
  List<DataValue> clusteringKeys;
  List<DataValue> columns;
}
