// automatically generated by the FlatBuffers compiler, do not modify

package org.apache.arrow.computeir.flatbuf;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

@SuppressWarnings("unused")
public final class UInt16Literal extends Table {
  public static void ValidateVersion() { Constants.FLATBUFFERS_2_0_0(); }
  public static UInt16Literal getRootAsUInt16Literal(ByteBuffer _bb) { return getRootAsUInt16Literal(_bb, new UInt16Literal()); }
  public static UInt16Literal getRootAsUInt16Literal(ByteBuffer _bb, UInt16Literal obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public void __init(int _i, ByteBuffer _bb) { __reset(_i, _bb); }
  public UInt16Literal __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  public int value() { int o = __offset(4); return o != 0 ? bb.getShort(o + bb_pos) & 0xFFFF : 0; }

  public static int createUInt16Literal(FlatBufferBuilder builder,
      int value) {
    builder.startTable(1);
    UInt16Literal.addValue(builder, value);
    return UInt16Literal.endUInt16Literal(builder);
  }

  public static void startUInt16Literal(FlatBufferBuilder builder) { builder.startTable(1); }
  public static void addValue(FlatBufferBuilder builder, int value) { builder.addShort(0, (short)value, (short)0); }
  public static int endUInt16Literal(FlatBufferBuilder builder) {
    int o = builder.endTable();
    return o;
  }

  public static final class Vector extends BaseVector {
    public Vector __assign(int _vector, int _element_size, ByteBuffer _bb) { __reset(_vector, _element_size, _bb); return this; }

    public UInt16Literal get(int j) { return get(new UInt16Literal(), j); }
    public UInt16Literal get(UInt16Literal obj, int j) {  return obj.__assign(__indirect(__element(j), bb), bb); }
  }
}

