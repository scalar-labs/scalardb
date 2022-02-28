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
import java.util.Optional;
import javax.annotation.Nullable;
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
    Map<String, Optional<Value<?>>> ret = new HashMap<>();
    if (projections.isEmpty()) {
      metadata.getColumnNames().forEach(name -> add(ret, name, item.get(name)));
    } else {
      projections.forEach(name -> add(ret, name, item.get(name)));
    }
    return new ResultImpl(ret, metadata);
  }

  private void add(Map<String, Optional<Value<?>>> values, String name, AttributeValue itemValue) {
    values.put(
        name, Optional.ofNullable(convert(itemValue, name, metadata.getColumnDataType(name))));
  }

  @Nullable
  private Value<?> convert(@Nullable AttributeValue itemValue, String name, DataType dataType) {
    if (itemValue == null || (itemValue.nul() != null && itemValue.nul())) {
      return null;
    }
    switch (dataType) {
      case BOOLEAN:
        return new BooleanValue(name, itemValue.bool());
      case INT:
        return new IntValue(name, Integer.parseInt(itemValue.n()));
      case BIGINT:
        return new BigIntValue(name, Long.parseLong(itemValue.n()));
      case FLOAT:
        return new FloatValue(name, Float.parseFloat(itemValue.n()));
      case DOUBLE:
        return new DoubleValue(name, Double.parseDouble(itemValue.n()));
      case TEXT:
        return new TextValue(name, itemValue.s());
      case BLOB:
        return new BlobValue(name, itemValue.b().asByteArray());
      default:
        throw new AssertionError();
    }
  }
}
