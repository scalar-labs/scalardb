package com.scalar.db.dataloader.core.dataimport.controlfile;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;

/** Represents the control file */
@Getter
@Setter
public class ControlFile {

  @JsonProperty("tables")
  private final List<ControlFileTable> tables;

  /** Class constructor */
  public ControlFile() {
    this.tables = new ArrayList<>();
  }

  @JsonCreator
  public ControlFile(@JsonProperty("tables") List<ControlFileTable> tables) {
    this.tables = tables;
  }
}
