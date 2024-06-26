// automatically generated by the FlatBuffers compiler, do not modify

package org.apache.arrow.computeir.flatbuf;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

/**
 * Join between two tables
 */
@SuppressWarnings("unused")
public final class Join extends Table {
  public static void ValidateVersion() { Constants.FLATBUFFERS_2_0_0(); }
  public static Join getRootAsJoin(ByteBuffer _bb) { return getRootAsJoin(_bb, new Join()); }
  public static Join getRootAsJoin(ByteBuffer _bb, Join obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public void __init(int _i, ByteBuffer _bb) { __reset(_i, _bb); }
  public Join __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  /**
   * An identifiier for the relation. The identifier should be unique over the
   * entire plan. Optional.
   */
  public org.apache.arrow.computeir.flatbuf.RelId id() { return id(new org.apache.arrow.computeir.flatbuf.RelId()); }
  public org.apache.arrow.computeir.flatbuf.RelId id(org.apache.arrow.computeir.flatbuf.RelId obj) { int o = __offset(4); return o != 0 ? obj.__assign(__indirect(o + bb_pos), bb) : null; }
  /**
   * Left relation
   */
  public org.apache.arrow.computeir.flatbuf.Relation left() { return left(new org.apache.arrow.computeir.flatbuf.Relation()); }
  public org.apache.arrow.computeir.flatbuf.Relation left(org.apache.arrow.computeir.flatbuf.Relation obj) { int o = __offset(6); return o != 0 ? obj.__assign(__indirect(o + bb_pos), bb) : null; }
  /**
   * Right relation
   */
  public org.apache.arrow.computeir.flatbuf.Relation right() { return right(new org.apache.arrow.computeir.flatbuf.Relation()); }
  public org.apache.arrow.computeir.flatbuf.Relation right(org.apache.arrow.computeir.flatbuf.Relation obj) { int o = __offset(8); return o != 0 ? obj.__assign(__indirect(o + bb_pos), bb) : null; }
  /**
   * The expression which will be evaluated against rows from each
   * input to determine whether they should be included in the
   * join relation's output.
   */
  public org.apache.arrow.computeir.flatbuf.Expression onExpression() { return onExpression(new org.apache.arrow.computeir.flatbuf.Expression()); }
  public org.apache.arrow.computeir.flatbuf.Expression onExpression(org.apache.arrow.computeir.flatbuf.Expression obj) { int o = __offset(10); return o != 0 ? obj.__assign(__indirect(o + bb_pos), bb) : null; }
  /**
   * The kind of join to use.
   */
  public int joinKind() { int o = __offset(12); return o != 0 ? bb.get(o + bb_pos) & 0xFF : 0; }

  public static int createJoin(FlatBufferBuilder builder,
      int idOffset,
      int leftOffset,
      int rightOffset,
      int on_expressionOffset,
      int join_kind) {
    builder.startTable(5);
    Join.addOnExpression(builder, on_expressionOffset);
    Join.addRight(builder, rightOffset);
    Join.addLeft(builder, leftOffset);
    Join.addId(builder, idOffset);
    Join.addJoinKind(builder, join_kind);
    return Join.endJoin(builder);
  }

  public static void startJoin(FlatBufferBuilder builder) { builder.startTable(5); }
  public static void addId(FlatBufferBuilder builder, int idOffset) { builder.addOffset(0, idOffset, 0); }
  public static void addLeft(FlatBufferBuilder builder, int leftOffset) { builder.addOffset(1, leftOffset, 0); }
  public static void addRight(FlatBufferBuilder builder, int rightOffset) { builder.addOffset(2, rightOffset, 0); }
  public static void addOnExpression(FlatBufferBuilder builder, int onExpressionOffset) { builder.addOffset(3, onExpressionOffset, 0); }
  public static void addJoinKind(FlatBufferBuilder builder, int joinKind) { builder.addByte(4, (byte)joinKind, (byte)0); }
  public static int endJoin(FlatBufferBuilder builder) {
    int o = builder.endTable();
    builder.required(o, 6);  // left
    builder.required(o, 8);  // right
    builder.required(o, 10);  // on_expression
    return o;
  }

  public static final class Vector extends BaseVector {
    public Vector __assign(int _vector, int _element_size, ByteBuffer _bb) { __reset(_vector, _element_size, _bb); return this; }

    public Join get(int j) { return get(new Join(), j); }
    public Join get(Join obj, int j) {  return obj.__assign(__indirect(__element(j), bb), bb); }
  }
}

