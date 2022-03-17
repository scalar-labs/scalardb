package com.scalar.db.sql;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Optional;

public interface ResultSet extends Iterable<Record> {

  Optional<Record> one();

  default List<Record> all() {
    ImmutableList.Builder<Record> builder = ImmutableList.builder();
    while (true) {
      Optional<Record> one = one();
      if (!one.isPresent()) {
        return builder.build();
      }
      builder.add(one.get());
    }
  }
}
