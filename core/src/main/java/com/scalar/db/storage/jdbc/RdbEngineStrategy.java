package com.scalar.db.storage.jdbc;

import com.scalar.db.io.DataType;

abstract class RdbEngineStrategy {
   abstract String getDataTypeForEngine(DataType dataType);
}