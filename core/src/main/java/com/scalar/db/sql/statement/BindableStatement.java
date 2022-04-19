package com.scalar.db.sql.statement;

import com.scalar.db.sql.Value;
import java.util.List;
import java.util.Map;

public interface BindableStatement<T extends Statement> extends Statement {
  T bind(List<Value> positionalValues);

  T bind(Map<String, Value> namedValues);
}
