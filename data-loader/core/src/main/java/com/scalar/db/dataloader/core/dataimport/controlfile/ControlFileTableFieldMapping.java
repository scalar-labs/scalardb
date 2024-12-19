package com.scalar.db.dataloader.core.dataimport.controlfile;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

/** Represents the one field mapping for a table mapping in the control file */
@Getter
@Setter
public class ControlFileTableFieldMapping {

  @JsonProperty("source_field")
  private String sourceField;

  @JsonProperty("target_column")
  private String targetColumn;

  /**
   * Class constructor
   *
   * @param sourceField The data field in the provided json field
   * @param targetColumn The column in the ScalarDB table
   */
  @JsonCreator
  public ControlFileTableFieldMapping(
      @JsonProperty("source_field") String sourceField,
      @JsonProperty("target_column") String targetColumn) {
    this.sourceField = sourceField;
    this.targetColumn = targetColumn;
  }
}
