// automatically generated by the FlatBuffers compiler, do not modify

package org.apache.arrow.computeir.flatbuf;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

/**
 * A function call expression
 */
@SuppressWarnings("unused")
public final class Call extends Table {
  public static void ValidateVersion() { Constants.FLATBUFFERS_2_0_0(); }
  public static Call getRootAsCall(ByteBuffer _bb) { return getRootAsCall(_bb, new Call()); }
  public static Call getRootAsCall(ByteBuffer _bb, Call obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public void __init(int _i, ByteBuffer _bb) { __reset(_i, _bb); }
  public Call __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  /**
   * The function to call
   */
  public String name() { int o = __offset(4); return o != 0 ? __string(o + bb_pos) : null; }
  public ByteBuffer nameAsByteBuffer() { return __vector_as_bytebuffer(4, 1); }
  public ByteBuffer nameInByteBuffer(ByteBuffer _bb) { return __vector_in_bytebuffer(_bb, 4, 1); }
  /**
   * The arguments passed to `name`.
   */
  public org.apache.arrow.computeir.flatbuf.Expression arguments(int j) { return arguments(new org.apache.arrow.computeir.flatbuf.Expression(), j); }
  public org.apache.arrow.computeir.flatbuf.Expression arguments(org.apache.arrow.computeir.flatbuf.Expression obj, int j) { int o = __offset(6); return o != 0 ? obj.__assign(__indirect(__vector(o) + j * 4), bb) : null; }
  public int argumentsLength() { int o = __offset(6); return o != 0 ? __vector_len(o) : 0; }
  public org.apache.arrow.computeir.flatbuf.Expression.Vector argumentsVector() { return argumentsVector(new org.apache.arrow.computeir.flatbuf.Expression.Vector()); }
  public org.apache.arrow.computeir.flatbuf.Expression.Vector argumentsVector(org.apache.arrow.computeir.flatbuf.Expression.Vector obj) { int o = __offset(6); return o != 0 ? obj.__assign(__vector(o), 4, bb) : null; }
  /**
   * Possible ordering of input. These are useful
   * in aggregates where ordering in meaningful such as
   * string concatenation
   */
  public org.apache.arrow.computeir.flatbuf.SortKey orderings(int j) { return orderings(new org.apache.arrow.computeir.flatbuf.SortKey(), j); }
  public org.apache.arrow.computeir.flatbuf.SortKey orderings(org.apache.arrow.computeir.flatbuf.SortKey obj, int j) { int o = __offset(8); return o != 0 ? obj.__assign(__indirect(__vector(o) + j * 4), bb) : null; }
  public int orderingsLength() { int o = __offset(8); return o != 0 ? __vector_len(o) : 0; }
  public org.apache.arrow.computeir.flatbuf.SortKey.Vector orderingsVector() { return orderingsVector(new org.apache.arrow.computeir.flatbuf.SortKey.Vector()); }
  public org.apache.arrow.computeir.flatbuf.SortKey.Vector orderingsVector(org.apache.arrow.computeir.flatbuf.SortKey.Vector obj) { int o = __offset(8); return o != 0 ? obj.__assign(__vector(o), 4, bb) : null; }

  public static int createCall(FlatBufferBuilder builder,
      int nameOffset,
      int argumentsOffset,
      int orderingsOffset) {
    builder.startTable(3);
    Call.addOrderings(builder, orderingsOffset);
    Call.addArguments(builder, argumentsOffset);
    Call.addName(builder, nameOffset);
    return Call.endCall(builder);
  }

  public static void startCall(FlatBufferBuilder builder) { builder.startTable(3); }
  public static void addName(FlatBufferBuilder builder, int nameOffset) { builder.addOffset(0, nameOffset, 0); }
  public static void addArguments(FlatBufferBuilder builder, int argumentsOffset) { builder.addOffset(1, argumentsOffset, 0); }
  public static int createArgumentsVector(FlatBufferBuilder builder, int[] data) { builder.startVector(4, data.length, 4); for (int i = data.length - 1; i >= 0; i--) builder.addOffset(data[i]); return builder.endVector(); }
  public static void startArgumentsVector(FlatBufferBuilder builder, int numElems) { builder.startVector(4, numElems, 4); }
  public static void addOrderings(FlatBufferBuilder builder, int orderingsOffset) { builder.addOffset(2, orderingsOffset, 0); }
  public static int createOrderingsVector(FlatBufferBuilder builder, int[] data) { builder.startVector(4, data.length, 4); for (int i = data.length - 1; i >= 0; i--) builder.addOffset(data[i]); return builder.endVector(); }
  public static void startOrderingsVector(FlatBufferBuilder builder, int numElems) { builder.startVector(4, numElems, 4); }
  public static int endCall(FlatBufferBuilder builder) {
    int o = builder.endTable();
    builder.required(o, 4);  // name
    builder.required(o, 6);  // arguments
    return o;
  }

  public static final class Vector extends BaseVector {
    public Vector __assign(int _vector, int _element_size, ByteBuffer _bb) { __reset(_vector, _element_size, _bb); return this; }

    public Call get(int j) { return get(new Call(), j); }
    public Call get(Call obj, int j) {  return obj.__assign(__indirect(__element(j), bb), bb); }
  }
}

