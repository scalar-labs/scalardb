package com.scalar.db.transaction.consensuscommit.replication.semisync.columns;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public class Key {
  @JsonProperty private final List<Column<?>> columns;

  public Key(List<Column<?>> columns) {
    this.columns = columns;
  }

  public static Key of(@Nullable com.scalar.db.io.Key src) {
    if (src == null) {
      return new Key(new ArrayList<>());
    }
    return new Key(src.getColumns().stream().map(Column::of).collect(Collectors.toList()));
  }
}
