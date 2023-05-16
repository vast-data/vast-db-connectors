/* Copyright (C) Vast Data Ltd. */

// automatically generated by the FlatBuffers compiler, do not modify

package vast_flatbuf.tabular;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class S3File extends Table {
  public static void ValidateVersion() { Constants.FLATBUFFERS_2_0_0(); }
  public static S3File getRootAsS3File(ByteBuffer _bb) { return getRootAsS3File(_bb, new S3File()); }
  public static S3File getRootAsS3File(ByteBuffer _bb, S3File obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public void __init(int _i, ByteBuffer _bb) { __reset(_i, _bb); }
  public S3File __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  /**
   * currently supported only parquet
   */
  public String format() { int o = __offset(4); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer formatAsByteBuffer() { return __vector_as_bytebuffer(4, 1); }
  public ByteBuffer formatInByteBuffer(ByteBuffer _bb) { return __vector_in_bytebuffer(_bb, 4, 1); }
  public String bucketName() { int o = __offset(6); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer bucketNameAsByteBuffer() { return __vector_as_bytebuffer(6, 1); }
  public ByteBuffer bucketNameInByteBuffer(ByteBuffer _bb) { return __vector_in_bytebuffer(_bb, 6, 1); }
  /**
   * name includes path
   */
  public String fileName() { int o = __offset(8); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer fileNameAsByteBuffer() { return __vector_as_bytebuffer(8, 1); }
  public ByteBuffer fileNameInByteBuffer(ByteBuffer _bb) { return __vector_in_bytebuffer(_bb, 8, 1); }
  /**
   * https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format
   * serialized as a Arrow schema + single-row Arrow page, if exist
   * A page of partitions including one or more column:type:value per file.
   * We should validate that column exists in table layout and type is valid
   * when importing we should use insert this column value for
   * all the rows in the table
   */
  public int partitions(int j) { int o = __offset(10); return o != 0 ? bb.get(__vector(o) + j * 1) & 0xFF : 0; }
  public int partitionsLength() { int o = __offset(10); return o != 0 ? __vector_len(o) : 0; }
  public ByteVector partitionsVector() { return partitionsVector(new ByteVector()); }
  public ByteVector partitionsVector(ByteVector obj) { int o = __offset(10); return o != 0 ? obj.__assign(__vector(o), bb) : null; }
  public ByteBuffer partitionsAsByteBuffer() { return __vector_as_bytebuffer(10, 1); }
  public ByteBuffer partitionsInByteBuffer(ByteBuffer _bb) { return __vector_in_bytebuffer(_bb, 10, 1); }

  public static int createS3File(FlatBufferBuilder builder,
      int formatOffset,
      int bucket_nameOffset,
      int file_nameOffset,
      int partitionsOffset) {
    builder.startTable(4);
    S3File.addPartitions(builder, partitionsOffset);
    S3File.addFileName(builder, file_nameOffset);
    S3File.addBucketName(builder, bucket_nameOffset);
    S3File.addFormat(builder, formatOffset);
    return S3File.endS3File(builder);
  }

  public static void startS3File(FlatBufferBuilder builder) { builder.startTable(4); }
  public static void addFormat(FlatBufferBuilder builder, int formatOffset) { builder.addOffset(0, formatOffset, 0); }
  public static void addBucketName(FlatBufferBuilder builder, int bucketNameOffset) { builder.addOffset(1, bucketNameOffset, 0); }
  public static void addFileName(FlatBufferBuilder builder, int fileNameOffset) { builder.addOffset(2, fileNameOffset, 0); }
  public static void addPartitions(FlatBufferBuilder builder, int partitionsOffset) { builder.addOffset(3, partitionsOffset, 0); }
  public static int createPartitionsVector(FlatBufferBuilder builder, byte[] data) { return builder.createByteVector(data); }
  public static int createPartitionsVector(FlatBufferBuilder builder, ByteBuffer data) { return builder.createByteVector(data); }
  public static void startPartitionsVector(FlatBufferBuilder builder, int numElems) { builder.startVector(1, numElems, 1); }
  public static int endS3File(FlatBufferBuilder builder) {
    int o = builder.endTable();
    return o;
  }

  public static final class Vector extends BaseVector {
    public Vector __assign(int _vector, int _element_size, ByteBuffer _bb) { __reset(_vector, _element_size, _bb); return this; }

    public S3File get(int j) { return get(new S3File(), j); }
    public S3File get(S3File obj, int j) {  return obj.__assign(__indirect(__element(j), bb), bb); }
  }
}

