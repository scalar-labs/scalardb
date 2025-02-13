package com.scalar.db.dataloader.core.dataimport.task.mapping;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.scalar.db.dataloader.core.dataimport.controlfile.ControlFileTable;
import com.scalar.db.dataloader.core.dataimport.controlfile.ControlFileTableFieldMapping;

public class ImportDataMapping {

  /**
   * * Update the source data replace the source column name with the target column name according
   * to control file table data
   *
   * @param source source data
   * @param controlFileTable control file table to map source data
   */
  public static void apply(ObjectNode source, ControlFileTable controlFileTable) {
    // Copy the source field data to the target column if missing
    for (ControlFileTableFieldMapping mapping : controlFileTable.getMappings()) {
      String sourceField = mapping.getSourceField();
      String targetColumn = mapping.getTargetColumn();

      if (source.has(sourceField) && !source.has(targetColumn)) {
        source.set(targetColumn, source.get(sourceField));
        source.remove(sourceField);
      }
    }
  }
}
