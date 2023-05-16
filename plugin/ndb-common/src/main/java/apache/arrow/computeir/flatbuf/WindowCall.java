/* Copyright (C) Vast Data Ltd. */

// automatically generated by the FlatBuffers compiler, do not modify

package org.apache.arrow.computeir.flatbuf;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

/**
 * An expression representing a window function call.
 */
@SuppressWarnings("unused")
public final class WindowCall extends Table {
  public static void ValidateVersion() { Constants.FLATBUFFERS_2_0_0(); }
  public static WindowCall getRootAsWindowCall(ByteBuffer _bb) { return getRootAsWindowCall(_bb, new WindowCall()); }
  public static WindowCall getRootAsWindowCall(ByteBuffer _bb, WindowCall obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public void __init(int _i, ByteBuffer _bb) { __reset(_i, _bb); }
  public WindowCall __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  /**
   * The expression to operate over
   */
  public org.apache.arrow.computeir.flatbuf.Expression expression() { return expression(new org.apache.arrow.computeir.flatbuf.Expression()); }
  public org.apache.arrow.computeir.flatbuf.Expression expression(org.apache.arrow.computeir.flatbuf.Expression obj) { int o = __offset(4); return o != 0 ? obj.__assign(__indirect(o + bb_pos), bb) : null; }
  /**
   * The kind of window frame
   */
  public int kind() { int o = __offset(6); return o != 0 ? bb.get(o + bb_pos) & 0xFF : 0; }
  /**
   * Partition keys
   */
  public org.apache.arrow.computeir.flatbuf.Expression partitions(int j) { return partitions(new org.apache.arrow.computeir.flatbuf.Expression(), j); }
  public org.apache.arrow.computeir.flatbuf.Expression partitions(org.apache.arrow.computeir.flatbuf.Expression obj, int j) { int o = __offset(8); return o != 0 ? obj.__assign(__indirect(__vector(o) + j * 4), bb) : null; }
  public int partitionsLength() { int o = __offset(8); return o != 0 ? __vector_len(o) : 0; }
  public org.apache.arrow.computeir.flatbuf.Expression.Vector partitionsVector() { return partitionsVector(new org.apache.arrow.computeir.flatbuf.Expression.Vector()); }
  public org.apache.arrow.computeir.flatbuf.Expression.Vector partitionsVector(org.apache.arrow.computeir.flatbuf.Expression.Vector obj) { int o = __offset(8); return o != 0 ? obj.__assign(__vector(o), 4, bb) : null; }
  /**
   * Sort keys
   */
  public org.apache.arrow.computeir.flatbuf.SortKey orderings(int j) { return orderings(new org.apache.arrow.computeir.flatbuf.SortKey(), j); }
  public org.apache.arrow.computeir.flatbuf.SortKey orderings(org.apache.arrow.computeir.flatbuf.SortKey obj, int j) { int o = __offset(10); return o != 0 ? obj.__assign(__indirect(__vector(o) + j * 4), bb) : null; }
  public int orderingsLength() { int o = __offset(10); return o != 0 ? __vector_len(o) : 0; }
  public org.apache.arrow.computeir.flatbuf.SortKey.Vector orderingsVector() { return orderingsVector(new org.apache.arrow.computeir.flatbuf.SortKey.Vector()); }
  public org.apache.arrow.computeir.flatbuf.SortKey.Vector orderingsVector(org.apache.arrow.computeir.flatbuf.SortKey.Vector obj) { int o = __offset(10); return o != 0 ? obj.__assign(__vector(o), 4, bb) : null; }
  public byte lowerBoundType() { int o = __offset(12); return o != 0 ? bb.get(o + bb_pos) : 0; }
  /**
   * Lower window bound
   */
  public Table lowerBound(Table obj) { int o = __offset(14); return o != 0 ? __union(obj, o + bb_pos) : null; }
  public byte upperBoundType() { int o = __offset(16); return o != 0 ? bb.get(o + bb_pos) : 0; }
  /**
   * Upper window bound
   */
  public Table upperBound(Table obj) { int o = __offset(18); return o != 0 ? __union(obj, o + bb_pos) : null; }

  public static int createWindowCall(FlatBufferBuilder builder,
      int expressionOffset,
      int kind,
      int partitionsOffset,
      int orderingsOffset,
      byte lower_bound_type,
      int lower_boundOffset,
      byte upper_bound_type,
      int upper_boundOffset) {
    builder.startTable(8);
    WindowCall.addUpperBound(builder, upper_boundOffset);
    WindowCall.addLowerBound(builder, lower_boundOffset);
    WindowCall.addOrderings(builder, orderingsOffset);
    WindowCall.addPartitions(builder, partitionsOffset);
    WindowCall.addExpression(builder, expressionOffset);
    WindowCall.addUpperBoundType(builder, upper_bound_type);
    WindowCall.addLowerBoundType(builder, lower_bound_type);
    WindowCall.addKind(builder, kind);
    return WindowCall.endWindowCall(builder);
  }

  public static void startWindowCall(FlatBufferBuilder builder) { builder.startTable(8); }
  public static void addExpression(FlatBufferBuilder builder, int expressionOffset) { builder.addOffset(0, expressionOffset, 0); }
  public static void addKind(FlatBufferBuilder builder, int kind) { builder.addByte(1, (byte)kind, (byte)0); }
  public static void addPartitions(FlatBufferBuilder builder, int partitionsOffset) { builder.addOffset(2, partitionsOffset, 0); }
  public static int createPartitionsVector(FlatBufferBuilder builder, int[] data) { builder.startVector(4, data.length, 4); for (int i = data.length - 1; i >= 0; i--) builder.addOffset(data[i]); return builder.endVector(); }
  public static void startPartitionsVector(FlatBufferBuilder builder, int numElems) { builder.startVector(4, numElems, 4); }
  public static void addOrderings(FlatBufferBuilder builder, int orderingsOffset) { builder.addOffset(3, orderingsOffset, 0); }
  public static int createOrderingsVector(FlatBufferBuilder builder, int[] data) { builder.startVector(4, data.length, 4); for (int i = data.length - 1; i >= 0; i--) builder.addOffset(data[i]); return builder.endVector(); }
  public static void startOrderingsVector(FlatBufferBuilder builder, int numElems) { builder.startVector(4, numElems, 4); }
  public static void addLowerBoundType(FlatBufferBuilder builder, byte lowerBoundType) { builder.addByte(4, lowerBoundType, 0); }
  public static void addLowerBound(FlatBufferBuilder builder, int lowerBoundOffset) { builder.addOffset(5, lowerBoundOffset, 0); }
  public static void addUpperBoundType(FlatBufferBuilder builder, byte upperBoundType) { builder.addByte(6, upperBoundType, 0); }
  public static void addUpperBound(FlatBufferBuilder builder, int upperBoundOffset) { builder.addOffset(7, upperBoundOffset, 0); }
  public static int endWindowCall(FlatBufferBuilder builder) {
    int o = builder.endTable();
    builder.required(o, 4);  // expression
    builder.required(o, 8);  // partitions
    builder.required(o, 10);  // orderings
    builder.required(o, 14);  // lower_bound
    builder.required(o, 18);  // upper_bound
    return o;
  }

  public static final class Vector extends BaseVector {
    public Vector __assign(int _vector, int _element_size, ByteBuffer _bb) { __reset(_vector, _element_size, _bb); return this; }

    public WindowCall get(int j) { return get(new WindowCall(), j); }
    public WindowCall get(WindowCall obj, int j) {  return obj.__assign(__indirect(__element(j), bb), bb); }
  }
}

