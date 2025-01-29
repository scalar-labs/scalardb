package com.scalar.db.common;

import com.google.common.base.MoreObjects;
import com.google.common.base.MoreObjects.ToStringHelper;
import com.google.common.base.Suppliers;
import com.scalar.db.api.Result;
import com.scalar.db.common.error.CoreError;
import com.scalar.db.io.Column;
import com.scalar.db.io.Value;
import com.scalar.db.util.ScalarDbUtils;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public abstract class AbstractResult implements Result {

  private final Supplier<Integer> hashCode;

  public AbstractResult() {
    hashCode =
        Suppliers.memoize(
            () -> {
              List<String> containedColumnNames = new ArrayList<>(getContainedColumnNames());
              Collections.sort(containedColumnNames);
              Object[] values = new Object[containedColumnNames.size()];
              for (int i = 0; i < containedColumnNames.size(); i++) {
                values[i] = getAsObject(containedColumnNames.get(i));
              }
              return Objects.hash(values);
            });
  }

  protected void checkIfExists(String name) {
    if (!contains(name)) {
      throw new IllegalArgumentException(CoreError.COLUMN_NOT_FOUND.buildMessage(name));
    }
  }

  /** @deprecated As of release 3.6.0. Will be removed in release 5.0.0 */
  @Deprecated
  @Override
  public Optional<Value<?>> getValue(String columnName) {
    Column<?> column = getColumns().get(columnName);
    if (column == null) {
      return Optional.empty();
    } else {
      return Optional.of(ScalarDbUtils.toValue(column));
    }
  }

  /** @deprecated As of release 3.6.0. Will be removed in release 5.0.0 */
  @Deprecated
  @Override
  public Map<String, Value<?>> getValues() {
    return getColumns().entrySet().stream()
        .collect(Collectors.toMap(Entry::getKey, e -> ScalarDbUtils.toValue(e.getValue())));
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
      Object value = getAsObject(containedColumnName);
      Object otherValue = other.getAsObject(containedColumnName);
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
    getContainedColumnNames().forEach(c -> toStringHelper.add(c, getAsObject(c)));
    return toStringHelper.toString();
  }
}
