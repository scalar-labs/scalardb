package com.scalar.db.schemaloader.alteration;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.scalar.db.io.DataType;
import com.scalar.db.util.ImmutableLinkedHashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public class TableMetadataAlteration {

  private final ImmutableLinkedHashSet<String> addedColumnNames;
  private final ImmutableMap<String, DataType> addedColumnDataTypes;
  private final ImmutableSet<String> addedSecondaryIndexNames;
  private final ImmutableSet<String> deletedSecondaryIndexNames;

  public TableMetadataAlteration(
      LinkedHashSet<String> addedColumnNames,
      Map<String, DataType> addedColumnDataTypes,
      Set<String> addedSecondaryIndexNames,
      Set<String> deletedSecondaryIndexNames) {
    this.addedColumnNames = new ImmutableLinkedHashSet<>(addedColumnNames);
    this.addedColumnDataTypes = ImmutableMap.copyOf(addedColumnDataTypes);
    this.addedSecondaryIndexNames = ImmutableSet.copyOf(addedSecondaryIndexNames);
    this.deletedSecondaryIndexNames = ImmutableSet.copyOf(deletedSecondaryIndexNames);
  }

  public LinkedHashSet<String> getAddedColumnNames() {
    return addedColumnNames;
  }

  public Map<String, DataType> getAddedColumnDataTypes() {
    return addedColumnDataTypes;
  }

  public Set<String> getAddedSecondaryIndexNames() {
    return addedSecondaryIndexNames;
  }

  public Set<String> getDeletedSecondaryIndexNames() {
    return deletedSecondaryIndexNames;
  }

  public boolean hasAlterations() {
    return !addedColumnNames.isEmpty()
        || !addedSecondaryIndexNames.isEmpty()
        || !deletedSecondaryIndexNames.isEmpty();
  }
}
