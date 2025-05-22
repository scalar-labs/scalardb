package com.scalar.db.dataloader.core.dataimport.controlfile;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;

/**
 * Represents a control file that holds control file tables which contains the column mappings that
 * maps a source file column to the actual database table column.
 */
@SuppressWarnings("SameNameButDifferent")
@Getter
@Setter
public class ControlFile {

  /**
   * A list of {@link ControlFileTable} objects representing the tables defined in the control file.
   */
  @JsonProperty("tables")
  private final List<ControlFileTable> tables;

  /** Default constructor that initializes an empty list of tables. */
  public ControlFile() {
    this.tables = new ArrayList<>();
  }

  /**
   * Constructs a {@code ControlFile} with the specified list of tables.
   *
   * @param tables the list of {@link ControlFileTable} objects to initialize the control file with
   */
  @JsonCreator
  public ControlFile(@JsonProperty("tables") List<ControlFileTable> tables) {
    this.tables = tables;
  }
}
