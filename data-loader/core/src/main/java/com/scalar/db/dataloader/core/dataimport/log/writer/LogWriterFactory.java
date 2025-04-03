package com.scalar.db.dataloader.core.dataimport.log.writer;

import java.io.IOException;

public interface LogWriterFactory {

  LogWriter createLogWriter(String logFilePath) throws IOException;
}
