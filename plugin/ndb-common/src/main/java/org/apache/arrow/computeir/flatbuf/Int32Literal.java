// automatically generated by the FlatBuffers compiler, do not modify

package org.apache.arrow.computeir.flatbuf;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class Int32Literal extends Table {
  public static void ValidateVersion() { Constants.FLATBUFFERS_2_0_0(); }
  public static Int32Literal getRootAsInt32Literal(ByteBuffer _bb) { return getRootAsInt32Literal(_bb, new Int32Literal()); }
  public static Int32Literal getRootAsInt32Literal(ByteBuffer _bb, Int32Literal obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public void __init(int _i, ByteBuffer _bb) { __reset(_i, _bb); }
  public Int32Literal __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  public int value() { int o = __offset(4); return o != 0 ? bb.getInt(o + bb_pos) : 0; }

  public static int createInt32Literal(FlatBufferBuilder builder,
      int value) {
    builder.startTable(1);
    Int32Literal.addValue(builder, value);
    return Int32Literal.endInt32Literal(builder);
  }

  public static void startInt32Literal(FlatBufferBuilder builder) { builder.startTable(1); }
  public static void addValue(FlatBufferBuilder builder, int value) { builder.addInt(0, value, 0); }
  public static int endInt32Literal(FlatBufferBuilder builder) {
    int o = builder.endTable();
    return o;
  }

  public static final class Vector extends BaseVector {
    public Vector __assign(int _vector, int _element_size, ByteBuffer _bb) { __reset(_vector, _element_size, _bb); return this; }

    public Int32Literal get(int j) { return get(new Int32Literal(), j); }
    public Int32Literal get(Int32Literal obj, int j) {  return obj.__assign(__indirect(__element(j), bb), bb); }
  }
}

