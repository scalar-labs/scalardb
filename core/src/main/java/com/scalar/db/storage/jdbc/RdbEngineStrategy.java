package com.scalar.db.storage.jdbc;

import com.scalar.db.io.DataType;

public abstract class RdbEngineStrategy {
   protected abstract String getDataTypeForEngine(DataType dataType);
}