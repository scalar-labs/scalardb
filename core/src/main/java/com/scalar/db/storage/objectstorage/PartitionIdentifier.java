package com.scalar.db.storage.objectstorage;

public class PartitionIdentifier {
  private final String namespaceName;
  private final String tableName;
  private final String partitionName;

  public PartitionIdentifier(String namespaceName, String tableName, String partitionName) {
    this.namespaceName = namespaceName;
    this.tableName = tableName;
    this.partitionName = partitionName;
  }

  public static PartitionIdentifier of(
      String namespaceName, String tableName, String partitionName) {
    return new PartitionIdentifier(namespaceName, tableName, partitionName);
  }

  public String getNamespaceName() {
    return namespaceName;
  }

  public String getTableName() {
    return tableName;
  }

  public String getPartitionName() {
    return partitionName;
  }

  @Override
  public int hashCode() {
    return java.util.Objects.hash(namespaceName, tableName, partitionName);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!(obj instanceof PartitionIdentifier)) return false;
    PartitionIdentifier other = (PartitionIdentifier) obj;
    return namespaceName.equals(other.namespaceName)
        && tableName.equals(other.tableName)
        && partitionName.equals(other.partitionName);
  }
}
