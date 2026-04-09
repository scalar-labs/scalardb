package com.scalar.db.storage.objectstorage;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

public class InMemoryOutputFile implements OutputFile {
  private final ByteArrayOutputStream baos = new ByteArrayOutputStream();

  @Override
  public PositionOutputStream create(long blockSizeHint) {
    return new InMemoryPositionOutputStream(baos);
  }

  @Override
  public PositionOutputStream createOrOverwrite(long blockSizeHint) {
    baos.reset();
    return new InMemoryPositionOutputStream(baos);
  }

  @Override
  public boolean supportsBlockSize() {
    return false;
  }

  @Override
  public long defaultBlockSize() {
    return 0;
  }

  public byte[] toByteArray() {
    return baos.toByteArray();
  }

  private static class InMemoryPositionOutputStream extends PositionOutputStream {
    private final ByteArrayOutputStream baos;
    private long pos;

    InMemoryPositionOutputStream(ByteArrayOutputStream baos) {
      this.baos = baos;
      this.pos = 0;
    }

    @Override
    public long getPos() {
      return pos;
    }

    @Override
    public void write(int b) throws IOException {
      baos.write(b);
      pos++;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      baos.write(b, off, len);
      pos += len;
    }
  }
}
