package com.scalar.dataloader.model;

import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class DataValue {
  String name;
  ValueType type;
  String pattern;
}
