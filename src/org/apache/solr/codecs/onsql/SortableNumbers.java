/**
 * Copyright Andrey Prokopenko
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.codecs.onsql;

public final class SortableNumbers {

  private SortableNumbers() {} // no instance!
  
  /**
   * The default precision step used by {@link NumericField}, {@link NumericTokenStream},
   * {@link NumericRangeQuery}, and {@link NumericRangeFilter} as default
   */
  public static final int PRECISION_STEP_DEFAULT = 4;
  
  /**
   * Expert: Longs are stored at lower precision by shifting off lower bits. The shift count is
   * stored as <code>SHIFT_START_LONG+shift</code> in the first character
   */
  public static final char SHIFT_START_LONG = (char)0x20;

  /**
   * Expert: The maximum term length (used for <code>char[]</code> buffer size)
   * for encoding <code>long</code> values.
   * @see #longToPrefixCoded(long,int,char[])
   */
  public static final int BUF_SIZE_LONG = 63/7 + 2;

  /**
   * Expert: Integers are stored at lower precision by shifting off lower bits. The shift count is
   * stored as <code>SHIFT_START_INT+shift</code> in the first character
   */
  public static final char SHIFT_START_INT  = (char)0x60;

  /**
   * Expert: The maximum term length (used for <code>char[]</code> buffer size)
   * for encoding <code>int</code> values.
   * @see #intToPrefixCoded(int,int,char[])
   */
  public static final int BUF_SIZE_INT = 31/7 + 2;

  /**
   * Expert: Returns prefix coded bits after reducing the precision by <code>shift</code> bits.
   * This is method is used by {@link NumericTokenStream}.
   * @param val the numeric value
   * @param shift how many bits to strip from the right
   * @param buffer that will contain the encoded chars, must be at least of {@link #BUF_SIZE_LONG}
   * length
   * @return number of chars written to buffer
   */
  public static int longToPrefixCoded(final long val, final int shift, final char[] buffer) {
    if (shift>63 || shift<0)
      throw new IllegalArgumentException("Illegal shift value, must be 0..63");
    int nChars = (63-shift)/7 + 1, len = nChars+1;
    buffer[0] = (char)(SHIFT_START_LONG + shift);
    long sortableBits = val ^ 0x8000000000000000L;
    sortableBits >>>= shift;
    while (nChars>=1) {
      // Store 7 bits per character for good efficiency when UTF-8 encoding.
      // The whole number is right-justified so that lucene can prefix-encode
      // the terms more efficiently.
      buffer[nChars--] = (char)(sortableBits & 0x7f);
      sortableBits >>>= 7;
    }
    return len;
  }

  /**
   * Expert: Returns prefix coded bits after reducing the precision by <code>shift</code> bits.
   * This is method is used by {@link LongRangeBuilder}.
   * @param val the numeric value
   * @param shift how many bits to strip from the right
   */
  public static String longToPrefixCoded(final long val, final int shift) {
    final char[] buffer = new char[BUF_SIZE_LONG];
    final int len = longToPrefixCoded(val, shift, buffer);
    return new String(buffer, 0, len);
  }

  /**
   * This is a convenience method, that returns prefix coded bits of a long without
   * reducing the precision. It can be used to store the full precision value as a
   * stored field in index.
   * <p>To decode, use {@link #prefixCodedToLong}.
   */
  public static String longToPrefixCoded(final long val) {
    return longToPrefixCoded(val, 0);
  }
  
  /**
   * Expert: Returns prefix coded bits after reducing the precision by <code>shift</code> bits.
   * This is method is used by {@link NumericTokenStream}.
   * @param val the numeric value
   * @param shift how many bits to strip from the right
   * @param buffer that will contain the encoded chars, must be at least of {@link #BUF_SIZE_INT}
   * length
   * @return number of chars written to buffer
   */
  public static int intToPrefixCoded(final int val, final int shift, final char[] buffer) {
    if (shift>31 || shift<0)
      throw new IllegalArgumentException("Illegal shift value, must be 0..31");
    int nChars = (31-shift)/7 + 1, len = nChars+1;
    buffer[0] = (char)(SHIFT_START_INT + shift);
    int sortableBits = val ^ 0x80000000;
    sortableBits >>>= shift;
    while (nChars>=1) {
      // Store 7 bits per character for good efficiency when UTF-8 encoding.
      // The whole number is right-justified so that lucene can prefix-encode
      // the terms more efficiently.
      buffer[nChars--] = (char)(sortableBits & 0x7f);
      sortableBits >>>= 7;
    }
    return len;
  }

  /**
   * Expert: Returns prefix coded bits after reducing the precision by <code>shift</code> bits.
   * This is method is used by {@link IntRangeBuilder}.
   * @param val the numeric value
   * @param shift how many bits to strip from the right
   */
  public static String intToPrefixCoded(final int val, final int shift) {
    final char[] buffer = new char[BUF_SIZE_INT];
    final int len = intToPrefixCoded(val, shift, buffer);
    return new String(buffer, 0, len);
  }

  /**
   * This is a convenience method, that returns prefix coded bits of an int without
   * reducing the precision. It can be used to store the full precision value as a
   * stored field in index.
   * <p>To decode, use {@link #prefixCodedToInt}.
   */
  public static String intToPrefixCoded(final int val) {
    return intToPrefixCoded(val, 0);
  }

  /**
   * Returns a long from prefixCoded characters.
   * Rightmost bits will be zero for lower precision codes.
   * This method can be used to decode e.g. a stored field.
   * @throws NumberFormatException if the supplied string is
   * not correctly prefix encoded.
   * @see #longToPrefixCoded(long)
   */
  public static long prefixCodedToLong(final String prefixCoded) {
    final int shift = prefixCoded.charAt(0)-SHIFT_START_LONG;
    if (shift>63 || shift<0)
      throw new NumberFormatException("Invalid shift value in prefixCoded string (is encoded value really a LONG?)");
    long sortableBits = 0L;
    for (int i=1, len=prefixCoded.length(); i<len; i++) {
      sortableBits <<= 7;
      final char ch = prefixCoded.charAt(i);
      if (ch>0x7f) {
        throw new NumberFormatException(
          "Invalid prefixCoded numerical value representation (char "+
          Integer.toHexString((int)ch)+" at position "+i+" is invalid)"
        );
      }
      sortableBits |= (long)ch;
    }
    return (sortableBits << shift) ^ 0x8000000000000000L;
  }

  /**
   * Returns an int from prefixCoded characters.
   * Rightmost bits will be zero for lower precision codes.
   * This method can be used to decode e.g. a stored field.
   * @throws NumberFormatException if the supplied string is
   * not correctly prefix encoded.
   * @see #intToPrefixCoded(int)
   */
  public static int prefixCodedToInt(final String prefixCoded) {
    final int shift = prefixCoded.charAt(0)-SHIFT_START_INT;
    if (shift>31 || shift<0)
      throw new NumberFormatException("Invalid shift value in prefixCoded string (is encoded value really an INT?)");
    int sortableBits = 0;
    for (int i=1, len=prefixCoded.length(); i<len; i++) {
      sortableBits <<= 7;
      final char ch = prefixCoded.charAt(i);
      if (ch>0x7f) {
        throw new NumberFormatException(
          "Invalid prefixCoded numerical value representation (char "+
          Integer.toHexString((int)ch)+" at position "+i+" is invalid)"
        );
      }
      sortableBits |= (int)ch;
    }
    return (sortableBits << shift) ^ 0x80000000;
  }

  /**
   * Converts a <code>double</code> value to a sortable signed <code>long</code>.
   * The value is converted by getting their IEEE 754 floating-point &quot;double format&quot;
   * bit layout and then some bits are swapped, to be able to compare the result as long.
   * By this the precision is not reduced, but the value can easily used as a long.
   * @see #sortableLongToDouble
   */
  public static long doubleToSortableLong(double val) {
    long f = Double.doubleToRawLongBits(val);
    if (f<0) f ^= 0x7fffffffffffffffL;
    return f;
  }

  /**
   * Convenience method: this just returns:
   *   longToPrefixCoded(doubleToSortableLong(val))
   */
  public static String doubleToPrefixCoded(double val) {
    return longToPrefixCoded(doubleToSortableLong(val));
  }

  /**
   * Converts a sortable <code>long</code> back to a <code>double</code>.
   * @see #doubleToSortableLong
   */
  public static double sortableLongToDouble(long val) {
    if (val<0) val ^= 0x7fffffffffffffffL;
    return Double.longBitsToDouble(val);
  }

  /**
   * Convenience method: this just returns:
   *    sortableLongToDouble(prefixCodedToLong(val))
   */
  public static double prefixCodedToDouble(String val) {
    return sortableLongToDouble(prefixCodedToLong(val));
  }

  /**
   * Converts a <code>float</code> value to a sortable signed <code>int</code>.
   * The value is converted by getting their IEEE 754 floating-point &quot;float format&quot;
   * bit layout and then some bits are swapped, to be able to compare the result as int.
   * By this the precision is not reduced, but the value can easily used as an int.
   * @see #sortableIntToFloat
   */
  public static int floatToSortableInt(float val) {
    int f = Float.floatToRawIntBits(val);
    if (f<0) f ^= 0x7fffffff;
    return f;
  }

  /**
   * Convenience method: this just returns:
   *   intToPrefixCoded(floatToSortableInt(val))
   */
  public static String floatToPrefixCoded(float val) {
    return intToPrefixCoded(floatToSortableInt(val));
  }

  /**
   * Converts a sortable <code>int</code> back to a <code>float</code>.
   * @see #floatToSortableInt
   */
  public static float sortableIntToFloat(int val) {
    if (val<0) val ^= 0x7fffffff;
    return Float.intBitsToFloat(val);
  }

  /**
   * Convenience method: this just returns:
   *    sortableIntToFloat(prefixCodedToInt(val))
   */
  public static float prefixCodedToFloat(String val) {
    return sortableIntToFloat(prefixCodedToInt(val));
  }

  
  
}
