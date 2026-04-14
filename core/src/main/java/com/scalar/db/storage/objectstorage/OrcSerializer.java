package com.scalar.db.storage.objectstorage;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.util.Progressable;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

@ThreadSafe
public class OrcSerializer {
  private static final OrcSchemaGenerator schemaGenerator = new OrcSchemaGenerator();
  private static final Configuration HADOOP_CONF = new Configuration();

  private static final int CATEGORY_PARTITION_KEY = 0;
  private static final int CATEGORY_CLUSTERING_KEY = 1;
  private static final int CATEGORY_VALUE = 2;

  private static final int BATCH_SIZE = 1024;

  public static byte[] serialize(
      ObjectStoragePartition partition, ObjectStorageTableMetadata metadata) {
    return serialize(partition, metadata, CompressionKind.ZSTD);
  }

  static byte[] serialize(
      ObjectStoragePartition partition,
      ObjectStorageTableMetadata metadata,
      CompressionKind compressionKind) {
    try {
      TypeDescription schema = schemaGenerator.getRecordSchema(metadata);

      // Precompute column info
      Map<String, String> columns = metadata.getColumns();
      int columnCount = columns.size();
      String[] columnNames = new String[columnCount];
      String[] columnTypes = new String[columnCount];
      int[] fieldIndices = new int[columnCount];
      int[] categories = new int[columnCount];

      LinkedHashSet<String> pkNames = metadata.getPartitionKeyNames();
      LinkedHashSet<String> ckNames = metadata.getClusteringKeyNames();
      int idx = 0;
      for (Map.Entry<String, String> entry : columns.entrySet()) {
        String name = entry.getKey();
        columnNames[idx] = name;
        columnTypes[idx] = entry.getValue();
        fieldIndices[idx] =
            schema.getFieldNames().indexOf(OrcSchemaGenerator.sanitizeFieldName(name));
        if (pkNames.contains(name)) {
          categories[idx] = CATEGORY_PARTITION_KEY;
        } else if (ckNames.contains(name)) {
          categories[idx] = CATEGORY_CLUSTERING_KEY;
        } else {
          categories[idx] = CATEGORY_VALUE;
        }
        idx++;
      }
      int idFieldIndex = schema.getFieldNames().indexOf("id");

      int estimatedSize = estimateOutputSize(partition, metadata);
      InMemoryFileSystem memFs = new InMemoryFileSystem(estimatedSize);
      org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path("mem:///data.orc");

      try (Writer writer =
          OrcFile.createWriter(
              path,
              OrcFile.writerOptions(HADOOP_CONF)
                  .setSchema(schema)
                  .compress(compressionKind)
                  .fileSystem(memFs)
                  .overwrite(true))) {

        VectorizedRowBatch batch = schema.createRowBatch(BATCH_SIZE);

        for (ObjectStorageRecord record : partition.getRecords().values()) {
          int row = batch.size++;

          // Set id
          BytesColumnVector idCol = (BytesColumnVector) batch.cols[idFieldIndex];
          byte[] idBytes = record.getId().getBytes(StandardCharsets.UTF_8);
          idCol.setVal(row, idBytes);

          Map<String, Object> pkMap = record.getPartitionKey();
          Map<String, Object> ckMap = record.getClusteringKey();
          Map<String, Object> valMap = record.getValues();

          for (int i = 0; i < columnCount; i++) {
            Object value;
            switch (categories[i]) {
              case CATEGORY_PARTITION_KEY:
                value = pkMap.get(columnNames[i]);
                break;
              case CATEGORY_CLUSTERING_KEY:
                value = ckMap.get(columnNames[i]);
                break;
              default:
                value = valMap.get(columnNames[i]);
                break;
            }

            ColumnVector col = batch.cols[fieldIndices[i]];
            setColumnValue(col, row, value, columnTypes[i]);
          }

          if (batch.size == batch.getMaxSize()) {
            writer.addRowBatch(batch);
            batch.reset();
          }
        }

        if (batch.size > 0) {
          writer.addRowBatch(batch);
        }
      }

      return memFs.getOutputData();
    } catch (IOException e) {
      throw new RuntimeException("Failed to serialize partition to ORC", e);
    }
  }

  public static ObjectStoragePartition deserialize(
      byte[] data, ObjectStorageTableMetadata metadata) {
    try {
      TypeDescription schema = schemaGenerator.getRecordSchema(metadata);

      // Precompute column info
      Map<String, String> columns = metadata.getColumns();
      int columnCount = columns.size();
      String[] columnNames = new String[columnCount];
      String[] columnTypes = new String[columnCount];
      int[] fieldIndices = new int[columnCount];
      int[] categories = new int[columnCount];

      LinkedHashSet<String> pkNames = metadata.getPartitionKeyNames();
      LinkedHashSet<String> ckNames = metadata.getClusteringKeyNames();
      int pkCount = pkNames.size();
      int ckCount = ckNames.size();
      int valueCount = columnCount - pkCount - ckCount;

      int idx = 0;
      for (Map.Entry<String, String> entry : columns.entrySet()) {
        String name = entry.getKey();
        columnNames[idx] = name;
        columnTypes[idx] = entry.getValue();
        fieldIndices[idx] =
            schema.getFieldNames().indexOf(OrcSchemaGenerator.sanitizeFieldName(name));
        if (pkNames.contains(name)) {
          categories[idx] = CATEGORY_PARTITION_KEY;
        } else if (ckNames.contains(name)) {
          categories[idx] = CATEGORY_CLUSTERING_KEY;
        } else {
          categories[idx] = CATEGORY_VALUE;
        }
        idx++;
      }
      int idFieldIndex = schema.getFieldNames().indexOf("id");

      InMemoryFileSystem memFs = new InMemoryFileSystem(data);
      org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path("mem:///data.orc");

      try (Reader reader =
          OrcFile.createReader(path, OrcFile.readerOptions(HADOOP_CONF).filesystem(memFs))) {
        Map<String, ObjectStorageRecord> records = new HashMap<>();
        try (RecordReader rows = reader.rows()) {
          VectorizedRowBatch batch = reader.getSchema().createRowBatch(BATCH_SIZE);
          while (rows.nextBatch(batch)) {
            for (int row = 0; row < batch.size; row++) {
              // Read id
              BytesColumnVector idCol = (BytesColumnVector) batch.cols[idFieldIndex];
              int idIdx = idCol.isRepeating ? 0 : row;
              String id =
                  new String(
                      idCol.vector[idIdx],
                      idCol.start[idIdx],
                      idCol.length[idIdx],
                      StandardCharsets.UTF_8);

              Map<String, Object> partitionKey = new HashMap<>(pkCount);
              Map<String, Object> clusteringKey = new HashMap<>(ckCount);
              Map<String, Object> values = new HashMap<>(valueCount);

              for (int i = 0; i < columnCount; i++) {
                ColumnVector col = batch.cols[fieldIndices[i]];
                Object value = getColumnValue(col, row, columnTypes[i]);
                switch (categories[i]) {
                  case CATEGORY_PARTITION_KEY:
                    partitionKey.put(columnNames[i], value);
                    break;
                  case CATEGORY_CLUSTERING_KEY:
                    clusteringKey.put(columnNames[i], value);
                    break;
                  default:
                    values.put(columnNames[i], value);
                    break;
                }
              }

              records.put(
                  id,
                  ObjectStorageRecord.newBuilder()
                      .id(id)
                      .partitionKey(partitionKey)
                      .clusteringKey(clusteringKey)
                      .values(values)
                      .build());
            }
          }
        }
        return new ObjectStoragePartition(records);
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to deserialize partition from ORC", e);
    }
  }

  private static void setColumnValue(ColumnVector col, int row, Object value, String columnType) {
    if (value == null) {
      col.noNulls = false;
      col.isNull[row] = true;
      return;
    }

    switch (columnType) {
      case "boolean":
        ((LongColumnVector) col).vector[row] = ((Boolean) value) ? 1 : 0;
        break;
      case "int":
      case "date":
        ((LongColumnVector) col).vector[row] = ((Number) value).intValue();
        break;
      case "bigint":
      case "time":
      case "timestamp":
      case "timestamptz":
        ((LongColumnVector) col).vector[row] = ((Number) value).longValue();
        break;
      case "float":
        ((DoubleColumnVector) col).vector[row] = ((Number) value).floatValue();
        break;
      case "double":
        ((DoubleColumnVector) col).vector[row] = ((Number) value).doubleValue();
        break;
      case "text":
        {
          byte[] bytes = ((String) value).getBytes(StandardCharsets.UTF_8);
          ((BytesColumnVector) col).setVal(row, bytes);
          break;
        }
      case "blob":
        {
          byte[] bytes;
          if (value instanceof byte[]) {
            bytes = (byte[]) value;
          } else {
            ByteBuffer buffer = ((ByteBuffer) value).duplicate();
            bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
          }
          ((BytesColumnVector) col).setVal(row, bytes);
          break;
        }
      default:
        throw new AssertionError("Unknown column type: " + columnType);
    }
  }

  private static Object getColumnValue(ColumnVector col, int row, String columnType) {
    int idx = col.isRepeating ? 0 : row;
    if (!col.noNulls && col.isNull[idx]) {
      return null;
    }

    switch (columnType) {
      case "boolean":
        return ((LongColumnVector) col).vector[idx] != 0;
      case "int":
      case "date":
        return (int) ((LongColumnVector) col).vector[idx];
      case "bigint":
      case "time":
      case "timestamp":
      case "timestamptz":
        return ((LongColumnVector) col).vector[idx];
      case "float":
        return (float) ((DoubleColumnVector) col).vector[idx];
      case "double":
        return ((DoubleColumnVector) col).vector[idx];
      case "text":
        {
          BytesColumnVector bcv = (BytesColumnVector) col;
          return new String(
              bcv.vector[idx], bcv.start[idx], bcv.length[idx], StandardCharsets.UTF_8);
        }
      case "blob":
        {
          BytesColumnVector bcv = (BytesColumnVector) col;
          byte[] bytes = new byte[bcv.length[idx]];
          System.arraycopy(bcv.vector[idx], bcv.start[idx], bytes, 0, bcv.length[idx]);
          return bytes;
        }
      default:
        throw new AssertionError("Unknown column type: " + columnType);
    }
  }

  /** In-memory Hadoop FileSystem to avoid temp file I/O for ORC read/write. */
  private static int estimateOutputSize(
      ObjectStoragePartition partition, ObjectStorageTableMetadata metadata) {
    int recordCount = partition.getRecords().size();
    if (recordCount == 0) {
      return 1024;
    }

    long bytesPerRecord = 0;
    for (String columnType : metadata.getColumns().values()) {
      switch (columnType) {
        case "boolean":
          bytesPerRecord += 1;
          break;
        case "int":
        case "float":
        case "date":
          bytesPerRecord += 4;
          break;
        case "bigint":
        case "double":
        case "time":
        case "timestamp":
        case "timestamptz":
          bytesPerRecord += 8;
          break;
        case "text":
          bytesPerRecord += 64;
          break;
        case "blob":
          bytesPerRecord += 1024;
          break;
        default:
          bytesPerRecord += 32;
          break;
      }
    }

    long estimated = (long) recordCount * bytesPerRecord;
    return (int) Math.min(estimated, Integer.MAX_VALUE - 8);
  }

  private static final class InMemoryFileSystem extends FileSystem {
    private final byte[] inputData;
    private final int initialOutputCapacity;
    private GrowableByteArrayOutputStream outputBuffer;

    InMemoryFileSystem(int initialOutputCapacity) {
      this.inputData = null;
      this.initialOutputCapacity = initialOutputCapacity;
      setConf(HADOOP_CONF);
    }

    InMemoryFileSystem(byte[] data) {
      this.inputData = data;
      this.initialOutputCapacity = 0;
      setConf(HADOOP_CONF);
    }

    byte[] getOutputData() {
      return outputBuffer.toByteArray();
    }

    @Override
    public URI getUri() {
      return URI.create("mem:///");
    }

    @Override
    public FSDataInputStream open(org.apache.hadoop.fs.Path f, int bufferSize) throws IOException {
      if (inputData == null) {
        throw new FileNotFoundException(f.toString());
      }
      return new FSDataInputStream(new SeekableByteArrayInputStream(inputData));
    }

    @Override
    public FSDataOutputStream create(
        org.apache.hadoop.fs.Path f,
        FsPermission permission,
        boolean overwrite,
        int bufferSize,
        short replication,
        long blockSize,
        Progressable progress)
        throws IOException {
      outputBuffer = new GrowableByteArrayOutputStream(initialOutputCapacity);
      return new FSDataOutputStream(outputBuffer, null);
    }

    @Override
    public FileStatus getFileStatus(org.apache.hadoop.fs.Path f) throws IOException {
      long len;
      if (inputData != null) {
        len = inputData.length;
      } else if (outputBuffer != null) {
        len = outputBuffer.size();
      } else {
        throw new FileNotFoundException(f.toString());
      }
      return new FileStatus(len, false, 1, len, 0, f);
    }

    @Override
    public boolean delete(org.apache.hadoop.fs.Path f, boolean recursive) {
      return true;
    }

    @Override
    public FSDataOutputStream append(
        org.apache.hadoop.fs.Path f, int bufferSize, Progressable progress) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean rename(org.apache.hadoop.fs.Path src, org.apache.hadoop.fs.Path dst) {
      return false;
    }

    @Override
    public boolean mkdirs(org.apache.hadoop.fs.Path f, FsPermission permission) {
      return true;
    }

    @Override
    public FileStatus[] listStatus(org.apache.hadoop.fs.Path f) {
      return new FileStatus[0];
    }

    @Override
    public void setWorkingDirectory(org.apache.hadoop.fs.Path newDir) {}

    @Override
    public org.apache.hadoop.fs.Path getWorkingDirectory() {
      return new org.apache.hadoop.fs.Path("/");
    }
  }

  /** ByteArrayInputStream that implements Seekable and PositionedReadable for ORC reader. */
  private static final class SeekableByteArrayInputStream extends ByteArrayInputStream
      implements Seekable, PositionedReadable {

    SeekableByteArrayInputStream(byte[] buf) {
      super(buf);
    }

    @Override
    public void seek(long pos) throws IOException {
      if (pos < 0 || pos > count) {
        throw new IOException("Invalid seek position: " + pos);
      }
      this.pos = (int) pos;
    }

    @Override
    public long getPos() {
      return pos;
    }

    @Override
    public boolean seekToNewSource(long targetPos) {
      return false;
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length) {
      if (position >= count) {
        return -1;
      }
      int available = count - (int) position;
      int readLen = Math.min(length, available);
      System.arraycopy(buf, (int) position, buffer, offset, readLen);
      return readLen;
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
      if (position + length > count) {
        throw new IOException("Read past end of data");
      }
      System.arraycopy(buf, (int) position, buffer, offset, length);
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException {
      readFully(position, buffer, 0, buffer.length);
    }
  }

  /**
   * OutputStream backed by a growable byte array. Uses 1.5x growth factor and avoids copying on
   * {@link #toByteArray()} when the buffer is exactly filled.
   */
  private static final class GrowableByteArrayOutputStream extends OutputStream {
    private byte[] buf;
    private int count;

    GrowableByteArrayOutputStream(int initialCapacity) {
      buf = new byte[Math.max(initialCapacity, 256)];
    }

    @Override
    public void write(int b) {
      ensureCapacity(count + 1);
      buf[count++] = (byte) b;
    }

    @Override
    public void write(byte[] b, int off, int len) {
      ensureCapacity(count + len);
      System.arraycopy(b, off, buf, count, len);
      count += len;
    }

    private void ensureCapacity(int minCapacity) {
      if (minCapacity > buf.length) {
        buf = Arrays.copyOf(buf, Math.max(buf.length + (buf.length >> 1), minCapacity));
      }
    }

    byte[] toByteArray() {
      return count == buf.length ? buf : Arrays.copyOf(buf, count);
    }

    int size() {
      return count;
    }
  }
}
