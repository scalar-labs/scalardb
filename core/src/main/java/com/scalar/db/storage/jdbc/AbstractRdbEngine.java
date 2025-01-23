package com.scalar.db.storage.jdbc;

import com.scalar.db.common.error.CoreError;
import com.scalar.db.io.DataType;
import java.sql.JDBCType;
import javax.annotation.Nullable;

public abstract class AbstractRdbEngine implements RdbEngineStrategy {

  @Override
  public final DataType getDataTypeForScalarDb(
      JDBCType type,
      String typeName,
      int columnSize,
      int digits,
      String columnDescription,
      @Nullable DataType overrideDataType) {
    DataType dataType =
        getDataTypeForScalarDbInternal(
            type, typeName, columnSize, digits, columnDescription, overrideDataType);

    if (overrideDataType != null && overrideDataType != dataType) {
      throw new IllegalArgumentException(
          CoreError.JDBC_IMPORT_DATA_TYPE_OVERRIDE_NOT_SUPPORTED.buildMessage(
              typeName, overrideDataType, columnDescription));
    }

    return dataType;
  }

  abstract DataType getDataTypeForScalarDbInternal(
      JDBCType type,
      String typeName,
      int columnSize,
      int digits,
      String columnDescription,
      @Nullable DataType overrideDataType);
}
