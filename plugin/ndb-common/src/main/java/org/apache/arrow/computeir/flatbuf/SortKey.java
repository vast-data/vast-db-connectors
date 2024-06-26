// automatically generated by the FlatBuffers compiler, do not modify

package org.apache.arrow.computeir.flatbuf;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

/**
 * An expression with an order
 */
@SuppressWarnings("unused")
public final class SortKey extends Table {
  public static void ValidateVersion() { Constants.FLATBUFFERS_2_0_0(); }
  public static SortKey getRootAsSortKey(ByteBuffer _bb) { return getRootAsSortKey(_bb, new SortKey()); }
  public static SortKey getRootAsSortKey(ByteBuffer _bb, SortKey obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public void __init(int _i, ByteBuffer _bb) { __reset(_i, _bb); }
  public SortKey __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  public org.apache.arrow.computeir.flatbuf.Expression expression() { return expression(new org.apache.arrow.computeir.flatbuf.Expression()); }
  public org.apache.arrow.computeir.flatbuf.Expression expression(org.apache.arrow.computeir.flatbuf.Expression obj) { int o = __offset(4); return o != 0 ? obj.__assign(__indirect(o + bb_pos), bb) : null; }
  public int ordering() { int o = __offset(6); return o != 0 ? bb.get(o + bb_pos) & 0xFF : 0; }

  public static int createSortKey(FlatBufferBuilder builder,
      int expressionOffset,
      int ordering) {
    builder.startTable(2);
    SortKey.addExpression(builder, expressionOffset);
    SortKey.addOrdering(builder, ordering);
    return SortKey.endSortKey(builder);
  }

  public static void startSortKey(FlatBufferBuilder builder) { builder.startTable(2); }
  public static void addExpression(FlatBufferBuilder builder, int expressionOffset) { builder.addOffset(0, expressionOffset, 0); }
  public static void addOrdering(FlatBufferBuilder builder, int ordering) { builder.addByte(1, (byte)ordering, (byte)0); }
  public static int endSortKey(FlatBufferBuilder builder) {
    int o = builder.endTable();
    builder.required(o, 4);  // expression
    return o;
  }

  public static final class Vector extends BaseVector {
    public Vector __assign(int _vector, int _element_size, ByteBuffer _bb) { __reset(_vector, _element_size, _bb); return this; }

    public SortKey get(int j) { return get(new SortKey(), j); }
    public SortKey get(SortKey obj, int j) {  return obj.__assign(__indirect(__element(j), bb), bb); }
  }
}

