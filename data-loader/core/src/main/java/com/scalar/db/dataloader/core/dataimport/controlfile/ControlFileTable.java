package com.scalar.db.dataloader.core.dataimport.controlfile;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;

/** Represents the mapping for one table in the control file */
@Getter
@Setter
public class ControlFileTable {

  @JsonProperty("namespace")
  private String namespace;

  @JsonProperty("table_name")
  private String tableName;

  @JsonProperty("mappings")
  private final List<ControlFileTableFieldMapping> mappings;

  /** Class constructor */
  public ControlFileTable(String namespace, String tableName) {
    this.tableName = tableName;
    this.namespace = namespace;
    this.mappings = new ArrayList<>();
  }

  /**
   * Added for mapping data to control file table object from API request
   * @param namespace namespace
   * @param tableName table name
   * @param mappings column name mapping from control file
   */
  @JsonCreator
  public ControlFileTable(
      @JsonProperty("namespace") String namespace,
      @JsonProperty("table_name") String tableName,
      @JsonProperty("mappings") List<ControlFileTableFieldMapping> mappings) {
    this.namespace = namespace;
    this.tableName = tableName;
    this.mappings = mappings;
  }
}
