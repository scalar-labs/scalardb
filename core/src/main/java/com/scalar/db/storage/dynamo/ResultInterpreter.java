package com.scalar.db.storage.dynamo;

import com.scalar.db.api.Result;
import com.scalar.db.api.TableMetadata;
import com.scalar.db.io.BigIntValue;
import com.scalar.db.io.BlobValue;
import com.scalar.db.io.BooleanValue;
import com.scalar.db.io.DataType;
import com.scalar.db.io.DoubleValue;
import com.scalar.db.io.FloatValue;
import com.scalar.db.io.IntValue;
import com.scalar.db.io.TextValue;
import com.scalar.db.io.Value;
import com.scalar.db.util.ResultImpl;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.concurrent.ThreadSafe;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

@ThreadSafe
public class ResultInterpreter {

  private final List<String> projections;
  private final TableMetadata metadata;

  public ResultInterpreter(List<String> projections, TableMetadata metadata) {
    this.projections = Objects.requireNonNull(projections);
    this.metadata = Objects.requireNonNull(metadata);
  }

  public Result interpret(Map<String, AttributeValue> item) {
    Map<String, Value<?>> ret = new HashMap<>();
    if (projections.isEmpty()) {
      metadata.getColumnNames().forEach(name -> add(ret, name, item.get(name)));
    } else {
      projections.forEach(name -> add(ret, name, item.get(name)));
    }
    return new ResultImpl(ret, metadata);
  }

  private void add(Map<String, Value<?>> values, String name, AttributeValue itemValue) {
    values.put(name, convert(itemValue, name, metadata.getColumnDataType(name)));
  }

  private Value<?> convert(AttributeValue itemValue, String name, DataType dataType) {
    // When itemValue is NULL, the value will be the default value.
    // It is the same behavior as the datastax C* driver
    boolean isNull = itemValue == null || (itemValue.nul() != null && itemValue.nul());
    switch (dataType) {
      case BOOLEAN:
        return new BooleanValue(name, !isNull && itemValue.bool());
      case INT:
        return new IntValue(name, isNull ? 0 : Integer.parseInt(itemValue.n()));
      case BIGINT:
        return new BigIntValue(name, isNull ? 0L : Long.parseLong(itemValue.n()));
      case FLOAT:
        return new FloatValue(name, isNull ? 0.0f : Float.parseFloat(itemValue.n()));
      case DOUBLE:
        return new DoubleValue(name, isNull ? 0.0 : Double.parseDouble(itemValue.n()));
      case TEXT:
        return new TextValue(name, isNull ? null : itemValue.s());
      case BLOB:
        return new BlobValue(name, isNull ? null : itemValue.b().asByteArray());
      default:
        throw new AssertionError();
    }
  }
}
