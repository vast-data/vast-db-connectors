// automatically generated by the FlatBuffers compiler, do not modify

package org.apache.arrow.computeir.flatbuf;

/**
 * Whether lesser values should precede greater or vice versa,
 * also whether nulls should preced or follow values
 */
public final class Ordering {
  private Ordering() { }
  public static final int ASCENDING_THEN_NULLS = 0;
  public static final int DESCENDING_THEN_NULLS = 1;
  public static final int NULLS_THEN_ASCENDING = 2;
  public static final int NULLS_THEN_DESCENDING = 3;

  public static final String[] names = { "ASCENDING_THEN_NULLS", "DESCENDING_THEN_NULLS", "NULLS_THEN_ASCENDING", "NULLS_THEN_DESCENDING", };

  public static String name(int e) { return names[e]; }
}

