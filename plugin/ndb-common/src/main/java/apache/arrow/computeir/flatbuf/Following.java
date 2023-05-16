/* Copyright (C) Vast Data Ltd. */

// automatically generated by the FlatBuffers compiler, do not modify

package org.apache.arrow.computeir.flatbuf;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

/**
 * Boundary is following rows, determined by the contained expression
 */
@SuppressWarnings("unused")
public final class Following extends Table {
  public static void ValidateVersion() { Constants.FLATBUFFERS_2_0_0(); }
  public static Following getRootAsFollowing(ByteBuffer _bb) { return getRootAsFollowing(_bb, new Following()); }
  public static Following getRootAsFollowing(ByteBuffer _bb, Following obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public void __init(int _i, ByteBuffer _bb) { __reset(_i, _bb); }
  public Following __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  public byte implType() { int o = __offset(4); return o != 0 ? bb.get(o + bb_pos) : 0; }
  public Table impl(Table obj) { int o = __offset(6); return o != 0 ? __union(obj, o + bb_pos) : null; }

  public static int createFollowing(FlatBufferBuilder builder,
      byte impl_type,
      int implOffset) {
    builder.startTable(2);
    Following.addImpl(builder, implOffset);
    Following.addImplType(builder, impl_type);
    return Following.endFollowing(builder);
  }

  public static void startFollowing(FlatBufferBuilder builder) { builder.startTable(2); }
  public static void addImplType(FlatBufferBuilder builder, byte implType) { builder.addByte(0, implType, 0); }
  public static void addImpl(FlatBufferBuilder builder, int implOffset) { builder.addOffset(1, implOffset, 0); }
  public static int endFollowing(FlatBufferBuilder builder) {
    int o = builder.endTable();
    builder.required(o, 6);  // impl
    return o;
  }

  public static final class Vector extends BaseVector {
    public Vector __assign(int _vector, int _element_size, ByteBuffer _bb) { __reset(_vector, _element_size, _bb); return this; }

    public Following get(int j) { return get(new Following(), j); }
    public Following get(Following obj, int j) {  return obj.__assign(__indirect(__element(j), bb), bb); }
  }
}

