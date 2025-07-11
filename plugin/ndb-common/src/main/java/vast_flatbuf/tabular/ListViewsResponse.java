// automatically generated by the FlatBuffers compiler, do not modify

package vast_flatbuf.tabular;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class ListViewsResponse extends Table {
  public static void ValidateVersion() { Constants.FLATBUFFERS_2_0_0(); }
  public static ListViewsResponse getRootAsListViewsResponse(ByteBuffer _bb) { return getRootAsListViewsResponse(_bb, new ListViewsResponse()); }
  public static ListViewsResponse getRootAsListViewsResponse(ByteBuffer _bb, ListViewsResponse obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public void __init(int _i, ByteBuffer _bb) { __reset(_i, _bb); }
  public ListViewsResponse __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  public String bucketName() { int o = __offset(4); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer bucketNameAsByteBuffer() { return __vector_as_bytebuffer(4, 1); }
  public ByteBuffer bucketNameInByteBuffer(ByteBuffer _bb) { return __vector_in_bytebuffer(_bb, 4, 1); }
  public String schemaName() { int o = __offset(6); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer schemaNameAsByteBuffer() { return __vector_as_bytebuffer(6, 1); }
  public ByteBuffer schemaNameInByteBuffer(ByteBuffer _bb) { return __vector_in_bytebuffer(_bb, 6, 1); }
  public vast_flatbuf.tabular.ObjectDetails views(int j) { return views(new vast_flatbuf.tabular.ObjectDetails(), j); }
  public vast_flatbuf.tabular.ObjectDetails views(vast_flatbuf.tabular.ObjectDetails obj, int j) { int o = __offset(8); return o != 0 ? obj.__assign(__indirect(__vector(o) + j * 4), bb) : null; }
  public int viewsLength() { int o = __offset(8); return o != 0 ? __vector_len(o) : 0; }
  public vast_flatbuf.tabular.ObjectDetails.Vector viewsVector() { return viewsVector(new vast_flatbuf.tabular.ObjectDetails.Vector()); }
  public vast_flatbuf.tabular.ObjectDetails.Vector viewsVector(vast_flatbuf.tabular.ObjectDetails.Vector obj) { int o = __offset(8); return o != 0 ? obj.__assign(__vector(o), 4, bb) : null; }

  public static int createListViewsResponse(FlatBufferBuilder builder,
      int bucket_nameOffset,
      int schema_nameOffset,
      int viewsOffset) {
    builder.startTable(3);
    ListViewsResponse.addViews(builder, viewsOffset);
    ListViewsResponse.addSchemaName(builder, schema_nameOffset);
    ListViewsResponse.addBucketName(builder, bucket_nameOffset);
    return ListViewsResponse.endListViewsResponse(builder);
  }

  public static void startListViewsResponse(FlatBufferBuilder builder) { builder.startTable(3); }
  public static void addBucketName(FlatBufferBuilder builder, int bucketNameOffset) { builder.addOffset(0, bucketNameOffset, 0); }
  public static void addSchemaName(FlatBufferBuilder builder, int schemaNameOffset) { builder.addOffset(1, schemaNameOffset, 0); }
  public static void addViews(FlatBufferBuilder builder, int viewsOffset) { builder.addOffset(2, viewsOffset, 0); }
  public static int createViewsVector(FlatBufferBuilder builder, int[] data) { builder.startVector(4, data.length, 4); for (int i = data.length - 1; i >= 0; i--) builder.addOffset(data[i]); return builder.endVector(); }
  public static void startViewsVector(FlatBufferBuilder builder, int numElems) { builder.startVector(4, numElems, 4); }
  public static int endListViewsResponse(FlatBufferBuilder builder) {
    int o = builder.endTable();
    return o;
  }
  public static void finishListViewsResponseBuffer(FlatBufferBuilder builder, int offset) { builder.finish(offset); }
  public static void finishSizePrefixedListViewsResponseBuffer(FlatBufferBuilder builder, int offset) { builder.finishSizePrefixed(offset); }

  public static final class Vector extends BaseVector {
    public Vector __assign(int _vector, int _element_size, ByteBuffer _bb) { __reset(_vector, _element_size, _bb); return this; }

    public ListViewsResponse get(int j) { return get(new ListViewsResponse(), j); }
    public ListViewsResponse get(ListViewsResponse obj, int j) {  return obj.__assign(__indirect(__element(j), bb), bb); }
  }
}

