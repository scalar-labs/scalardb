package com.scalar.db.dataloader.core.dataimport.controlfile;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

/**
 * Represents the mapping of a single field in the control file to a column in a ScalarDB table.
 * This class defines how data from a specific field in the input source should be mapped to the
 * corresponding column in the database.
 */
@Getter
@Setter
public class ControlFileTableFieldMapping {

  /** The name of the field in the input source (e.g., JSON or CSV). */
  @JsonProperty("source_field")
  private String sourceField;

  /** The name of the column in the ScalarDB table that the field maps to. */
  @JsonProperty("target_column")
  private String targetColumn;

  /**
   * Constructs a {@code ControlFileTableFieldMapping} instance using data from a serialized JSON
   * object. This constructor is primarily used for deserialization of control file mappings.
   *
   * @param sourceField The name of the field in the input source (e.g., JSON or CSV).
   * @param targetColumn The name of the corresponding column in the ScalarDB table.
   */
  @JsonCreator
  public ControlFileTableFieldMapping(
      @JsonProperty("source_field") String sourceField,
      @JsonProperty("target_column") String targetColumn) {
    this.sourceField = sourceField;
    this.targetColumn = targetColumn;
  }
}
