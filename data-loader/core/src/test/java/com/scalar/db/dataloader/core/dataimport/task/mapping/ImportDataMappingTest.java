package com.scalar.db.dataloader.core.dataimport.task.mapping;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.dataloader.core.UnitTestUtils;
import com.scalar.db.dataloader.core.dataimport.controlfile.ControlFileTable;
import com.scalar.db.dataloader.core.dataimport.controlfile.ControlFileTableFieldMapping;
import java.util.ArrayList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ImportDataMappingTest {

  TableMetadata mockMetadata;
  ControlFileTable controlFilTable;

  @BeforeEach
  void setup() {
    mockMetadata = UnitTestUtils.createTestTableMetadata();
    controlFilTable = new ControlFileTable("namespace", "table");
    ControlFileTableFieldMapping m1 = new ControlFileTableFieldMapping("source_id", "target_id");
    ControlFileTableFieldMapping m2 =
        new ControlFileTableFieldMapping("source_name", "target_name");
    ControlFileTableFieldMapping m3 =
        new ControlFileTableFieldMapping("source_email", "target_email");
    ArrayList<ControlFileTableFieldMapping> mappingArrayList = new ArrayList<>();
    mappingArrayList.add(m1);
    mappingArrayList.add(m2);
    mappingArrayList.add(m3);
    controlFilTable.getMappings().addAll(mappingArrayList);
  }

  @Test
  void apply_withValidData_shouldUpdateSourceData() throws JsonProcessingException {
    ObjectMapper objectMapper = new ObjectMapper();
    ObjectNode source = objectMapper.createObjectNode();
    source.put("source_id", "111");
    source.put("source_name", "abc");
    source.put("source_email", "sam@dsd.com");
    ImportDataMapping.apply(source, controlFilTable);
    // Assert changes
    Assertions.assertEquals("111", source.get("target_id").asText());
    Assertions.assertEquals("abc", source.get("target_name").asText());
    Assertions.assertEquals("sam@dsd.com", source.get("target_email").asText());
  }
}
