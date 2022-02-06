package com.scalar.db.util;

import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.base.Suppliers;
import com.scalar.db.api.Result;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

public abstract class AbstractResult implements Result {

  private final Supplier<Integer> hashCode;

  public AbstractResult() {
    // lazy loading
    hashCode =
        Suppliers.memoize(
            () -> {
              List<String> containedColumnNames = new ArrayList<>(getContainedColumnNames());
              Collections.sort(containedColumnNames);
              Object[] values = new Object[containedColumnNames.size()];
              for (int i = 0; i < containedColumnNames.size(); i++) {
                values[i] = getObject(containedColumnNames.get(i));
              }
              return Objects.hash(values);
            });
  }

  protected void checkIfExists(String name) {
    if (!contains(name)) {
      throw new IllegalArgumentException(name + " doesn't exist");
    }
  }

  @Override
  public int hashCode() {
    return hashCode.get();
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }
    if (!(o instanceof Result)) {
      return false;
    }

    Result other = (Result) o;
    if (!getContainedColumnNames().equals(other.getContainedColumnNames())) {
      return false;
    }
    for (String containedColumnName : getContainedColumnNames()) {
      Object value = getObject(containedColumnName);
      Object otherValue = other.getObject(containedColumnName);
      if (value == null && otherValue == null) {
        continue;
      }
      if (value == null || otherValue == null) {
        return false;
      }
      if (!value.equals(otherValue)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public String toString() {
    ToStringHelper toStringHelper = MoreObjects.toStringHelper(this);
    getContainedColumnNames().forEach(c -> toStringHelper.add(c, getObject(c)));
    return toStringHelper.toString();
  }
}
