package com.scalar.db.io;

import com.scalar.db.exception.storage.UnsupportedTypeException;

public enum DataType {
  BOOLEAN,
  INT,
  BIGINT,
  FLOAT,
  DOUBLE,
  TEXT,
  BLOB;

  /**
   * Returns the equivalent {@link com.datastax.driver.core.DataType}
   *
   * @return the equivalent {@link com.datastax.driver.core.DataType} to this enum value
   */
  public com.datastax.driver.core.DataType toCassandraDataType() {
    switch (this) {
      case BOOLEAN:
        return com.datastax.driver.core.DataType.cboolean();
      case INT:
        return com.datastax.driver.core.DataType.cint();
      case BIGINT:
        return com.datastax.driver.core.DataType.bigint();
      case FLOAT:
        return com.datastax.driver.core.DataType.cfloat();
      case DOUBLE:
        return com.datastax.driver.core.DataType.cdouble();
      case TEXT:
        return com.datastax.driver.core.DataType.text();
      case BLOB:
        return com.datastax.driver.core.DataType.blob();
      default:
        throw new UnsupportedOperationException(String.format("%s is not yet implemented", this));
    }
  }

  public static DataType fromCassandraDataType(
      com.datastax.driver.core.DataType.Name cassandraDataTypeName) {
    switch (cassandraDataTypeName) {
      case INT:
        return DataType.INT;
      case BIGINT:
        return DataType.BIGINT;
      case FLOAT:
        return DataType.FLOAT;
      case DOUBLE:
        return DataType.DOUBLE;
      case TEXT:
        return DataType.TEXT;
      case BOOLEAN:
        return DataType.BOOLEAN;
      case BLOB:
        return DataType.BLOB;
      default:
        throw new UnsupportedTypeException(cassandraDataTypeName.toString());
    }
  }
}
