package com.scalar.dataloader;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.scalar.dataloader.model.TableMetadata;
import com.scalar.dataloader.model.TableType;
import lombok.SneakyThrows;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class DeserializationTest {

  @Test
  @SneakyThrows
  public void deserialize_shouldWorkAsExpected() {
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.enable(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS);
    TableMetadata metadata =
        objectMapper.readValue(
            getClass().getClassLoader().getResource("table.json"), TableMetadata.class);
    Assertions.assertThat(metadata).isNotNull();
    Assertions.assertThat(metadata.getKeyspace()).isEqualTo("test");
    Assertions.assertThat(metadata.getTableName()).isEqualTo("sample");
    Assertions.assertThat(metadata.getTableType()).isEqualTo(TableType.TRANSACTION);
    Assertions.assertThat(metadata.getPartitionsKeys().size()).isEqualTo(1);
    Assertions.assertThat(metadata.getClusteringKeys().size()).isEqualTo(2);
    Assertions.assertThat(metadata.getColumns().size()).isEqualTo(1);
  }
}
