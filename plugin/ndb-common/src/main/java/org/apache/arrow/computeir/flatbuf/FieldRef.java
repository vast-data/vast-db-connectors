// automatically generated by the FlatBuffers compiler, do not modify

package org.apache.arrow.computeir.flatbuf;

import java.nio.*;
import java.lang.*;
import java.util.*;
import com.google.flatbuffers.*;

/**
 * Access the data of a field
 */
@SuppressWarnings("unused")
public final class FieldRef extends Table {
  public static void ValidateVersion() { Constants.FLATBUFFERS_2_0_0(); }
  public static FieldRef getRootAsFieldRef(ByteBuffer _bb) { return getRootAsFieldRef(_bb, new FieldRef()); }
  public static FieldRef getRootAsFieldRef(ByteBuffer _bb, FieldRef obj) { _bb.order(ByteOrder.LITTLE_ENDIAN); return (obj.__assign(_bb.getInt(_bb.position()) + _bb.position(), _bb)); }
  public void __init(int _i, ByteBuffer _bb) { __reset(_i, _bb); }
  public FieldRef __assign(int _i, ByteBuffer _bb) { __init(_i, _bb); return this; }

  public byte refType() { int o = __offset(4); return o != 0 ? bb.get(o + bb_pos) : 0; }
  public Table ref(Table obj) { int o = __offset(6); return o != 0 ? __union(obj, o + bb_pos) : null; }
  /**
   * For Expressions which might reference fields in multiple Relations,
   * this index may be provided to indicate which Relation's fields
   * `ref` points into. For example in the case of a join,
   * 0 refers to the left relation and 1 to the right relation.
   */
  public int relationIndex() { int o = __offset(8); return o != 0 ? bb.getInt(o + bb_pos) : 0; }

  public static int createFieldRef(FlatBufferBuilder builder,
      byte ref_type,
      int refOffset,
      int relation_index) {
    builder.startTable(3);
    FieldRef.addRelationIndex(builder, relation_index);
    FieldRef.addRef(builder, refOffset);
    FieldRef.addRefType(builder, ref_type);
    return FieldRef.endFieldRef(builder);
  }

  public static void startFieldRef(FlatBufferBuilder builder) { builder.startTable(3); }
  public static void addRefType(FlatBufferBuilder builder, byte refType) { builder.addByte(0, refType, 0); }
  public static void addRef(FlatBufferBuilder builder, int refOffset) { builder.addOffset(1, refOffset, 0); }
  public static void addRelationIndex(FlatBufferBuilder builder, int relationIndex) { builder.addInt(2, relationIndex, 0); }
  public static int endFieldRef(FlatBufferBuilder builder) {
    int o = builder.endTable();
    builder.required(o, 6);  // ref
    return o;
  }

  public static final class Vector extends BaseVector {
    public Vector __assign(int _vector, int _element_size, ByteBuffer _bb) { __reset(_vector, _element_size, _bb); return this; }

    public FieldRef get(int j) { return get(new FieldRef(), j); }
    public FieldRef get(FieldRef obj, int j) {  return obj.__assign(__indirect(__element(j), bb), bb); }
  }
}

