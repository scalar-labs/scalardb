package com.scalar.db.storage.objectstorage;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.EOFException;
import java.io.IOException;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

@SuppressFBWarnings({"EI_EXPOSE_REP", "EI_EXPOSE_REP2"})
public class InMemoryInputFile implements InputFile {
  private final byte[] data;

  public InMemoryInputFile(byte[] data) {
    this.data = data;
  }

  @Override
  public long getLength() {
    return data.length;
  }

  @Override
  public SeekableInputStream newStream() {
    return new DelegatingSeekableInputStream(new InMemorySeekableStream(data)) {
      @Override
      public long getPos() throws IOException {
        return ((InMemorySeekableStream) getStream()).getPos();
      }

      @Override
      public void seek(long newPos) throws IOException {
        ((InMemorySeekableStream) getStream()).seek(newPos);
      }
    };
  }

  private static class InMemorySeekableStream extends java.io.InputStream {
    private final byte[] data;
    private int pos;

    InMemorySeekableStream(byte[] data) {
      this.data = data;
      this.pos = 0;
    }

    public long getPos() {
      return pos;
    }

    public void seek(long newPos) throws IOException {
      if (newPos < 0 || newPos > data.length) {
        throw new EOFException("Invalid seek position: " + newPos);
      }
      this.pos = (int) newPos;
    }

    @Override
    public int read() {
      if (pos >= data.length) {
        return -1;
      }
      return data[pos++] & 0xFF;
    }

    @Override
    public int read(byte[] b, int off, int len) {
      if (pos >= data.length) {
        return -1;
      }
      int bytesToRead = Math.min(len, data.length - pos);
      System.arraycopy(data, pos, b, off, bytesToRead);
      pos += bytesToRead;
      return bytesToRead;
    }
  }
}
