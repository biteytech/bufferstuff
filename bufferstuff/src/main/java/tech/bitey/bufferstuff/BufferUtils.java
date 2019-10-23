package tech.bitey.bufferstuff;

import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;

/**
 * Utility methods for working with nio buffers.
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
public enum BufferUtils {
	; // static methods only, enum prevents instantiation

	/**
	 * Determines if the specified buffer is sorted inside the specified range. That
	 * is: {@code buffer[i] <= buffer[i + 1]} for all elements in the range. A range
	 * of length zero or one is considered sorted.
	 * 
	 * @param b         - the buffer to be checked
	 * @param fromIndex - the index of the first element (inclusive) to be checked
	 * @param toIndex   - the index of the last element (exclusive) to be checked
	 * 
	 * @return true if the buffer is sorted inside the specified range
	 * 
	 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
	 * @throws IndexOutOfBoundsException if
	 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
	 */
	public static boolean isSorted(IntBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);

		if (toIndex - fromIndex <= 1)
			return true;

		int prev = b.get(fromIndex);
		for (int i = fromIndex + 1; i < toIndex; i++) {
			int value = b.get(i);
			if (prev > value)
				return false;
			prev = value;
		}

		return true;
	}

	/**
	 * Determines if the specified buffer is sorted and distinct inside the
	 * specified range. That is: {@code buffer[i] < buffer[i + 1]} for all elements
	 * in the range. A range of length zero or one is considered sorted and
	 * distinct.
	 * 
	 * @param b         - the buffer to be checked
	 * @param fromIndex - the index of the first element (inclusive) to be checked
	 * @param toIndex   - the index of the last element (exclusive) to be checked
	 * 
	 * @return true if the buffer is sorted and distinct inside the specified range
	 * 
	 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
	 * @throws IndexOutOfBoundsException if
	 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
	 */
	public static boolean isSortedAndDistinct(IntBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);

		if (toIndex - fromIndex <= 1)
			return true;

		int prev = b.get(fromIndex);
		for (int i = fromIndex + 1; i < toIndex; i++) {
			int value = b.get(i);
			if (prev >= value)
				return false;
			prev = value;
		}

		return true;
	}

	/**
	 * Determines if the specified buffer is sorted inside the specified range. That
	 * is: {@code buffer[i] <= buffer[i + 1]} for all elements in the range. A range
	 * of length zero or one is considered sorted.
	 * 
	 * @param b         - the buffer to be checked
	 * @param fromIndex - the index of the first element (inclusive) to be checked
	 * @param toIndex   - the index of the last element (exclusive) to be checked
	 * 
	 * @return true if the buffer is sorted inside the specified range
	 * 
	 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
	 * @throws IndexOutOfBoundsException if
	 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
	 */
	public static boolean isSorted(LongBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);

		if (toIndex - fromIndex <= 1)
			return true;

		long prev = b.get(fromIndex);
		for (int i = fromIndex + 1; i < toIndex; i++) {
			long value = b.get(i);
			if (prev > value)
				return false;
			prev = value;
		}

		return true;
	}

	/**
	 * Determines if the specified buffer is sorted and distinct inside the
	 * specified range. That is: {@code buffer[i] < buffer[i + 1]} for all elements
	 * in the range. A range of length zero or one is considered sorted and
	 * distinct.
	 * 
	 * @param b         - the buffer to be checked
	 * @param fromIndex - the index of the first element (inclusive) to be checked
	 * @param toIndex   - the index of the last element (exclusive) to be checked
	 * 
	 * @return true if the buffer is sorted and distinct inside the specified range
	 * 
	 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
	 * @throws IndexOutOfBoundsException if
	 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
	 */
	public static boolean isSortedAndDistinct(LongBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);

		if (toIndex - fromIndex <= 1)
			return true;

		long prev = b.get(fromIndex);
		for (int i = fromIndex + 1; i < toIndex; i++) {
			long value = b.get(i);
			if (prev >= value)
				return false;
			prev = value;
		}

		return true;
	}

	/**
	 * Determines if the specified buffer is sorted inside the specified range. That
	 * is: {@code buffer[i] <= buffer[i + 1]} for all elements in the range. A range
	 * of length zero or one is considered sorted. The comparison of two values is
	 * consistent with {@link Float#compareTo(Float)}.
	 * 
	 * @param b         - the buffer to be checked
	 * @param fromIndex - the index of the first element (inclusive) to be checked
	 * @param toIndex   - the index of the last element (exclusive) to be checked
	 * 
	 * @return true if the buffer is sorted inside the specified range
	 * 
	 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
	 * @throws IndexOutOfBoundsException if
	 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
	 */
	public static boolean isSorted(FloatBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);

		if (toIndex - fromIndex <= 1)
			return true;

		float prev = b.get(fromIndex);
		for (int i = fromIndex + 1; i < toIndex; i++) {
			float value = b.get(i);
			if (Float.compare(prev, value) > 0)
				return false;
			prev = value;
		}

		return true;
	}

	/**
	 * Determines if the specified buffer is sorted and distinct inside the
	 * specified range. That is: {@code buffer[i] < buffer[i + 1]} for all elements
	 * in the range. A range of length zero or one is considered sorted and
	 * distinct. The comparison of two values is consistent with
	 * {@link Float#compareTo(Float)}.
	 * 
	 * @param b         - the buffer to be checked
	 * @param fromIndex - the index of the first element (inclusive) to be checked
	 * @param toIndex   - the index of the last element (exclusive) to be checked
	 * 
	 * @return true if the buffer is sorted and distinct inside the specified range
	 * 
	 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
	 * @throws IndexOutOfBoundsException if
	 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
	 */
	public static boolean isSortedAndDistinct(FloatBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);

		if (toIndex - fromIndex <= 1)
			return true;

		float prev = b.get(fromIndex);
		for (int i = fromIndex + 1; i < toIndex; i++) {
			float value = b.get(i);
			if (Float.compare(prev, value) >= 0)
				return false;
			prev = value;
		}

		return true;
	}

	/**
	 * Determines if the specified buffer is sorted inside the specified range. That
	 * is: {@code buffer[i] <= buffer[i + 1]} for all elements in the range. A range
	 * of length zero or one is considered sorted. The comparison of two values is
	 * consistent with {@link Double#compareTo(Double)}.
	 * 
	 * @param b         - the buffer to be checked
	 * @param fromIndex - the index of the first element (inclusive) to be checked
	 * @param toIndex   - the index of the last element (exclusive) to be checked
	 * 
	 * @return true if the buffer is sorted inside the specified range
	 * 
	 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
	 * @throws IndexOutOfBoundsException if
	 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
	 */
	public static boolean isSorted(DoubleBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);

		if (toIndex - fromIndex <= 1)
			return true;

		double prev = b.get(fromIndex);
		for (int i = fromIndex + 1; i < toIndex; i++) {
			double value = b.get(i);
			if (Double.compare(prev, value) > 0)
				return false;
			prev = value;
		}

		return true;
	}

	/**
	 * Determines if the specified buffer is sorted and distinct inside the
	 * specified range. That is: {@code buffer[i] < buffer[i + 1]} for all elements
	 * in the range. A range of length zero or one is considered sorted and
	 * distinct. The comparison of two values is consistent with
	 * {@link Double#compareTo(Double)}.
	 * 
	 * @param b         - the buffer to be checked
	 * @param fromIndex - the index of the first element (inclusive) to be checked
	 * @param toIndex   - the index of the last element (exclusive) to be checked
	 * 
	 * @return true if the buffer is sorted and distinct inside the specified range
	 * 
	 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
	 * @throws IndexOutOfBoundsException if
	 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
	 */
	public static boolean isSortedAndDistinct(DoubleBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);

		if (toIndex - fromIndex <= 1)
			return true;

		double prev = b.get(fromIndex);
		for (int i = fromIndex + 1; i < toIndex; i++) {
			double value = b.get(i);
			if (Double.compare(prev, value) >= 0)
				return false;
			prev = value;
		}

		return true;
	}

	/**
	 * Checks that {@code fromIndex} and {@code toIndex} are in the range and throws
	 * an exception if they aren't.
	 * 
	 * @param bufferCapacity - capacity of the buffer being checked
	 * @param fromIndex      - the index of the first element (inclusive) to be
	 *                       checked
	 * @param toIndex        - the index of the last element (exclusive) to be
	 *                       checked
	 */
	static void rangeCheck(int bufferCapacity, int fromIndex, int toIndex) {
		if (fromIndex > toIndex) {
			throw new IllegalArgumentException("fromIndex(" + fromIndex + ") > toIndex(" + toIndex + ")");
		}
		if (fromIndex < 0) {
			throw new IndexOutOfBoundsException("fromIndex(" + fromIndex + ") < 0");
		}
		if (toIndex > bufferCapacity) {
			throw new IndexOutOfBoundsException("toIndex(" + toIndex + ") > " + bufferCapacity);
		}
	}
}
