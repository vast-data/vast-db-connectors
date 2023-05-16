/* Copyright (C) Vast Data Ltd. */

// automatically generated by the FlatBuffers compiler, do not modify

package org.apache.arrow.computeir.flatbuf;

/**
 * A concrete bound, which can be an expression or unbounded
 */
public final class ConcreteBoundImpl {
  private ConcreteBoundImpl() { }
  public static final byte NONE = 0;
  public static final byte Expression = 1;
  public static final byte Unbounded = 2;

  public static final String[] names = { "NONE", "Expression", "Unbounded", };

  public static String name(int e) { return names[e]; }
}

