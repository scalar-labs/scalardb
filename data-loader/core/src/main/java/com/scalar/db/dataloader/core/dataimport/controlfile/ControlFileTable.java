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
  public String namespace;

  @JsonProperty("table_name")
  public String tableName;

  @JsonProperty("mappings")
  public List<ControlFileTableFieldMapping> mappings;

  /** Class constructor */
  public ControlFileTable(String namespace, String tableName) {
    this.tableName = tableName;
    this.namespace = namespace;
    this.mappings = new ArrayList<>();
  }

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
