package com.scalar.db.storage.jdbc;

import com.scalar.db.io.DataType;

public interface RdbEngineStrategy {
   String getDataTypeForEngine(DataType dataType);
}