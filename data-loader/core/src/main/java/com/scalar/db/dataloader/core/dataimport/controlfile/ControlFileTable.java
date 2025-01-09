package com.scalar.db.dataloader.core.dataimport.controlfile;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;

/**
 * Represents the configuration for a single table in the control file, including its namespace,
 * table name, and field mappings. This class is used to define how data from a control file maps to
 * a specific table in ScalarDB.
 */
@Getter
@Setter
public class ControlFileTable {

  /** The namespace of the table in ScalarDB. */
  @JsonProperty("namespace")
  private String namespace;

  /** The name of the table in ScalarDB. */
  @JsonProperty("table_name")
  private String table;

  /**
   * A list of mappings defining the correspondence between control file fields and table columns.
   */
  @JsonProperty("mappings")
  private final List<ControlFileTableFieldMapping> mappings;

  /**
   * Creates a new {@code ControlFileTable} instance with the specified namespace and table name.
   * The mappings list is initialized as an empty list.
   *
   * @param namespace The namespace of the table in ScalarDB.
   * @param table The name of the table in ScalarDB.
   */
  public ControlFileTable(String namespace, String table) {
    this.namespace = namespace;
    this.table = table;
    this.mappings = new ArrayList<>();
  }

  /**
   * Constructs a {@code ControlFileTable} instance using data from a serialized JSON object. This
   * constructor is used for deserialization of API requests or control files.
   *
   * @param namespace The namespace of the table in ScalarDB.
   * @param table The name of the table in ScalarDB.
   * @param mappings A list of mappings that define the relationship between control file fields and
   *     table columns.
   */
  @JsonCreator
  public ControlFileTable(
      @JsonProperty("namespace") String namespace,
      @JsonProperty("table_name") String table,
      @JsonProperty("mappings") List<ControlFileTableFieldMapping> mappings) {
    this.namespace = namespace;
    this.table = table;
    this.mappings = mappings;
  }
}
