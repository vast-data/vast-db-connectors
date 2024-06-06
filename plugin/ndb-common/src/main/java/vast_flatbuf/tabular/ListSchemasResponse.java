// automatically generated by the FlatBuffers compiler, do not modify

package vast_flatbuf.tabular;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class ListSchemasResponse extends Table {
  public static void ValidateVersion() { Constants.FLATBUFFERS_2_0_0(); }
  public static ListSchemasResponse getRootAsListSchemasResponse(ByteBuffer _bb) { return getRootAsListSchemasResponse(_bb, new ListSchemasResponse()); }
  public static ListSchemasResponse getRootAsListSchemasResponse(ByteBuffer _bb, ListSchemasResponse obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public void __init(int _i, ByteBuffer _bb) { __reset(_i, _bb); }
  public ListSchemasResponse __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  public String bucketName() { int o = __offset(4); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer bucketNameAsByteBuffer() { return __vector_as_bytebuffer(4, 1); }
  public ByteBuffer bucketNameInByteBuffer(ByteBuffer _bb) { return __vector_in_bytebuffer(_bb, 4, 1); }
  public vast_flatbuf.tabular.ObjectDetails schemas(int j) { return schemas(new vast_flatbuf.tabular.ObjectDetails(), j); }
  public vast_flatbuf.tabular.ObjectDetails schemas(vast_flatbuf.tabular.ObjectDetails obj, int j) { int o = __offset(6); return o != 0 ? obj.__assign(__indirect(__vector(o) + j * 4), bb) : null; }
  public int schemasLength() { int o = __offset(6); return o != 0 ? __vector_len(o) : 0; }
  public vast_flatbuf.tabular.ObjectDetails.Vector schemasVector() { return schemasVector(new vast_flatbuf.tabular.ObjectDetails.Vector()); }
  public vast_flatbuf.tabular.ObjectDetails.Vector schemasVector(vast_flatbuf.tabular.ObjectDetails.Vector obj) { int o = __offset(6); return o != 0 ? obj.__assign(__vector(o), 4, bb) : null; }

  public static int createListSchemasResponse(FlatBufferBuilder builder,
      int bucket_nameOffset,
      int schemasOffset) {
    builder.startTable(2);
    ListSchemasResponse.addSchemas(builder, schemasOffset);
    ListSchemasResponse.addBucketName(builder, bucket_nameOffset);
    return ListSchemasResponse.endListSchemasResponse(builder);
  }

  public static void startListSchemasResponse(FlatBufferBuilder builder) { builder.startTable(2); }
  public static void addBucketName(FlatBufferBuilder builder, int bucketNameOffset) { builder.addOffset(0, bucketNameOffset, 0); }
  public static void addSchemas(FlatBufferBuilder builder, int schemasOffset) { builder.addOffset(1, schemasOffset, 0); }
  public static int createSchemasVector(FlatBufferBuilder builder, int[] data) { builder.startVector(4, data.length, 4); for (int i = data.length - 1; i >= 0; i--) builder.addOffset(data[i]); return builder.endVector(); }
  public static void startSchemasVector(FlatBufferBuilder builder, int numElems) { builder.startVector(4, numElems, 4); }
  public static int endListSchemasResponse(FlatBufferBuilder builder) {
    int o = builder.endTable();
    return o;
  }
  public static void finishListSchemasResponseBuffer(FlatBufferBuilder builder, int offset) { builder.finish(offset); }
  public static void finishSizePrefixedListSchemasResponseBuffer(FlatBufferBuilder builder, int offset) { builder.finishSizePrefixed(offset); }

  public static final class Vector extends BaseVector {
    public Vector __assign(int _vector, int _element_size, ByteBuffer _bb) { __reset(_vector, _element_size, _bb); return this; }

    public ListSchemasResponse get(int j) { return get(new ListSchemasResponse(), j); }
    public ListSchemasResponse get(ListSchemasResponse obj, int j) {  return obj.__assign(__indirect(__element(j), bb), bb); }
  }
}

