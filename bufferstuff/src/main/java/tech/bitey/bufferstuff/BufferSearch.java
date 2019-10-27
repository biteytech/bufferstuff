package tech.bitey.bufferstuff;

import static tech.bitey.bufferstuff.BufferUtils.rangeCheck;

import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;

/**
 * The primitive-array binary search implementations from
 * {@code java.util.Arrays}, modified with minimal changes to support nio
 * buffers.
 * <p>
 * <u>Supported Buffer Types</u>
 * <ul>
 * <li>{@link IntBuffer}
 * <li>{@link LongBuffer}
 * <li>{@link FloatBuffer}
 * <li>{@link DoubleBuffer}
 * </ul>
 * 
 * @author Lior Privman
 */
public enum BufferSearch {
	; // static methods only, enum prevents instantiation

	/**
	 * Copied from {@code java.util.Arrays}
	 * <p>
	 * Searches a range of the specified {@link IntBuffer} for the specified value
	 * using the binary search algorithm. The range must be sorted in ascending
	 * order prior to making this call. If it is not sorted, the results are
	 * undefined. If the range contains multiple elements with the specified value,
	 * there is no guarantee which one will be found.
	 *
	 * @param b         the buffer to be searched
	 * @param fromIndex the index of the first element (inclusive) to be searched
	 * @param toIndex   the index of the last element (exclusive) to be searched
	 * @param key       the value to be searched for
	 * 
	 * @return index of the search key, if it is contained in the buffer within the
	 *         specified range; otherwise, <tt>(-(<i>insertion point</i>) - 1)</tt>.
	 *         The <i>insertion point</i> is defined as the point at which the key
	 *         would be inserted into the buffer: the index of the first element in
	 *         the range greater than the key, or <tt>toIndex</tt> if all elements
	 *         in the range are less than the specified key. Note that this
	 *         guarantees that the return value will be &gt;= 0 if and only if the
	 *         key is found.
	 * 
	 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
	 * @throws IndexOutOfBoundsException if
	 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
	 */
	public static int binarySearch(IntBuffer b, int fromIndex, int toIndex, int key) {
		rangeCheck(b.capacity(), fromIndex, toIndex);
		return binarySearch0(b, fromIndex, toIndex, key);
	}

	private static int binarySearch0(IntBuffer b, int fromIndex, int toIndex, int key) {
		int low = fromIndex;
		int high = toIndex - 1;

		while (low <= high) {
			int mid = (low + high) >>> 1;
			int midVal = b.get(mid);

			if (midVal < key)
				low = mid + 1;
			else if (midVal > key)
				high = mid - 1;
			else
				return mid; // key found
		}
		return -(low + 1); // key not found.
	}

	/**
	 * Copied from {@code java.util.Arrays}
	 * <p>
	 * Searches a range of the specified {@link LongBuffer} for the specified value
	 * using the binary search algorithm. The range must be sorted in ascending
	 * order prior to making this call. If it is not sorted, the results are
	 * undefined. If the range contains multiple elements with the specified value,
	 * there is no guarantee which one will be found.
	 *
	 * @param b         the buffer to be searched
	 * @param fromIndex the index of the first element (inclusive) to be searched
	 * @param toIndex   the index of the last element (exclusive) to be searched
	 * @param key       the value to be searched for
	 * 
	 * @return index of the search key, if it is contained in the buffer within the
	 *         specified range; otherwise, <tt>(-(<i>insertion point</i>) - 1)</tt>.
	 *         The <i>insertion point</i> is defined as the point at which the key
	 *         would be inserted into the buffer: the index of the first element in
	 *         the range greater than the key, or <tt>toIndex</tt> if all elements
	 *         in the range are less than the specified key. Note that this
	 *         guarantees that the return value will be &gt;= 0 if and only if the
	 *         key is found.
	 * 
	 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
	 * @throws IndexOutOfBoundsException if
	 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
	 */
	public static int binarySearch(LongBuffer b, int fromIndex, int toIndex, long key) {
		rangeCheck(b.capacity(), fromIndex, toIndex);
		return binarySearch0(b, fromIndex, toIndex, key);
	}

	private static int binarySearch0(LongBuffer b, int fromIndex, int toIndex, long key) {
		int low = fromIndex;
		int high = toIndex - 1;

		while (low <= high) {
			int mid = (low + high) >>> 1;
			long midVal = b.get(mid);

			if (midVal < key)
				low = mid + 1;
			else if (midVal > key)
				high = mid - 1;
			else
				return mid; // key found
		}
		return -(low + 1); // key not found.
	}

	/**
	 * Copied from {@code java.util.Arrays}
	 * <p>
	 * Searches a range of the specified {@link FloatBuffer} for the specified value
	 * using the binary search algorithm. The range must be sorted in ascending
	 * order prior to making this call. If it is not sorted, the results are
	 * undefined. If the range contains multiple elements with the specified value,
	 * there is no guarantee which one will be found. This method considers all NaN
	 * values to be equivalent and equal.
	 *
	 * @param b         the buffer to be searched
	 * @param fromIndex the index of the first element (inclusive) to be searched
	 * @param toIndex   the index of the last element (exclusive) to be searched
	 * @param key       the value to be searched for
	 * 
	 * @return index of the search key, if it is contained in the buffer within the
	 *         specified range; otherwise, <tt>(-(<i>insertion point</i>) - 1)</tt>.
	 *         The <i>insertion point</i> is defined as the point at which the key
	 *         would be inserted into the buffer: the index of the first element in
	 *         the range greater than the key, or <tt>toIndex</tt> if all elements
	 *         in the range are less than the specified key. Note that this
	 *         guarantees that the return value will be &gt;= 0 if and only if the
	 *         key is found.
	 * 
	 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
	 * @throws IndexOutOfBoundsException if
	 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
	 */
	public static int binarySearch(FloatBuffer b, int fromIndex, int toIndex, float key) {
		rangeCheck(b.capacity(), fromIndex, toIndex);
		return binarySearch0(b, fromIndex, toIndex, key);
	}

	private static int binarySearch0(FloatBuffer b, int fromIndex, int toIndex, float key) {
		int low = fromIndex;
		int high = toIndex - 1;

		while (low <= high) {
			int mid = (low + high) >>> 1;
			float midVal = b.get(mid);

			if (midVal < key)
				low = mid + 1; // Neither val is NaN, thisVal is smaller
			else if (midVal > key)
				high = mid - 1; // Neither val is NaN, thisVal is larger
			else {
				int midBits = Float.floatToIntBits(midVal);
				int keyBits = Float.floatToIntBits(key);
				if (midBits == keyBits) // Values are equal
					return mid; // Key found
				else if (midBits < keyBits) // (-0.0, 0.0) or (!NaN, NaN)
					low = mid + 1;
				else // (0.0, -0.0) or (NaN, !NaN)
					high = mid - 1;
			}
		}
		return -(low + 1); // key not found.
	}

	/**
	 * Copied from {@code java.util.Arrays}
	 * <p>
	 * Searches a range of the specified {@link DoubleBuffer} for the specified
	 * value using the binary search algorithm. The range must be sorted in
	 * ascending order prior to making this call. If it is not sorted, the results
	 * are undefined. If the range contains multiple elements with the specified
	 * value, there is no guarantee which one will be found. This method considers
	 * all NaN values to be equivalent and equal.
	 *
	 * @param b         the buffer to be searched
	 * @param fromIndex the index of the first element (inclusive) to be searched
	 * @param toIndex   the index of the last element (exclusive) to be searched
	 * @param key       the value to be searched for
	 * 
	 * @return index of the search key, if it is contained in the buffer within the
	 *         specified range; otherwise, <tt>(-(<i>insertion point</i>) - 1)</tt>.
	 *         The <i>insertion point</i> is defined as the point at which the key
	 *         would be inserted into the buffer: the index of the first element in
	 *         the range greater than the key, or <tt>toIndex</tt> if all elements
	 *         in the range are less than the specified key. Note that this
	 *         guarantees that the return value will be &gt;= 0 if and only if the
	 *         key is found.
	 * 
	 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
	 * @throws IndexOutOfBoundsException if
	 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
	 */
	public static int binarySearch(DoubleBuffer b, int fromIndex, int toIndex, double key) {
		rangeCheck(b.capacity(), fromIndex, toIndex);
		return binarySearch0(b, fromIndex, toIndex, key);
	}

	private static int binarySearch0(DoubleBuffer b, int fromIndex, int toIndex, double key) {
		int low = fromIndex;
		int high = toIndex - 1;

		while (low <= high) {
			int mid = (low + high) >>> 1;
			double midVal = b.get(mid);

			if (midVal < key)
				low = mid + 1; // Neither val is NaN, thisVal is smaller
			else if (midVal > key)
				high = mid - 1; // Neither val is NaN, thisVal is larger
			else {
				long midBits = Double.doubleToLongBits(midVal);
				long keyBits = Double.doubleToLongBits(key);
				if (midBits == keyBits) // Values are equal
					return mid; // Key found
				else if (midBits < keyBits) // (-0.0, 0.0) or (!NaN, NaN)
					low = mid + 1;
				else // (0.0, -0.0) or (NaN, !NaN)
					high = mid - 1;
			}
		}
		return -(low + 1); // key not found.
	}
}
