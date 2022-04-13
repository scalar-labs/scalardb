package com.scalar.db.sql;

import com.google.common.base.MoreObjects;
import java.util.Objects;
import javax.annotation.concurrent.Immutable;

@Immutable
public class NamedBindMarker implements BindMarker {

  public final String name;

  NamedBindMarker(String name) {
    this.name = name;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("name", name).toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof NamedBindMarker)) {
      return false;
    }
    NamedBindMarker that = (NamedBindMarker) o;
    return Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }
}
