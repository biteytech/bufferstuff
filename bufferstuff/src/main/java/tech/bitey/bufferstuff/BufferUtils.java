package tech.bitey.bufferstuff;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;

/**
 * Utility methods for working with nio buffers.
 * 
 * @author biteytech@protonmail.com
 */
public enum BufferUtils {
	; // static methods only, enum prevents instantiation

	private static final boolean DIRECT = "true".equalsIgnoreCase(System.getProperty("tech.bitey.allocateDirect"));

	/**
	 * An empty, read-only {@link ByteBuffer} which has
	 * {@link ByteOrder#nativeOrder() native order}
	 */
	public static final ByteBuffer EMPTY_BUFFER = asReadOnlyBuffer(allocate(0));

	/**
	 * Allocates a new {@link ByteBuffer} with the specified capacity. The buffer
	 * will be direct if the {@code tech.bitey.allocateDirect} system property is
	 * set to "true", and will have {@link ByteOrder#nativeOrder() native order}.
	 * 
	 * @param capacity the new buffer's capacity, in bytes
	 * 
	 * @return the new native order byte buffer
	 */
	public static ByteBuffer allocate(int capacity) {
		return (DIRECT ? ByteBuffer.allocateDirect(capacity) : ByteBuffer.allocate(capacity))
				.order(ByteOrder.nativeOrder());
	}

	/**
	 * Duplicate a {@link ByteBuffer} and preserve the order. Equivalent to:
	 * 
	 * <pre>
	 * b.duplicate().order(b.order())
	 * </pre>
	 * 
	 * @param b - the buffer to be duplicated
	 * 
	 * @return duplicated buffer with order preserved
	 * 
	 * @see ByteBuffer#duplicate()
	 * @see ByteBuffer#order()
	 */
	public static ByteBuffer duplicate(ByteBuffer b) {
		return b.duplicate().order(b.order());
	}

	/**
	 * Slice a {@link ByteBuffer} and preserve the order. Equivalent to:
	 * 
	 * <pre>
	 * b.slice().order(b.order())
	 * </pre>
	 * 
	 * @param b - the buffer to be sliced
	 * 
	 * @return sliced buffer with order preserved
	 * 
	 * @see ByteBuffer#slice()
	 * @see ByteBuffer#order()
	 */
	public static ByteBuffer slice(ByteBuffer b) {
		return b.slice().order(b.order());
	}

	/**
	 * Creates a new, read-only byte buffer that shares the specified buffer's
	 * content, and preserves it's order. Equivalent to:
	 * 
	 * <pre>
	 * b.asReadOnlyBuffer().order(b.order())
	 * </pre>
	 * 
	 * @param b - the buffer to be made read-only
	 * 
	 * @return read-only byte buffer that shares the specified buffer's content
	 * 
	 * @see ByteBuffer#asReadOnlyBuffer()
	 * @see ByteBuffer#order()
	 */
	public static ByteBuffer asReadOnlyBuffer(ByteBuffer b) {
		return b.asReadOnlyBuffer().order(b.order());
	}

	/**
	 * Slice a range from the specified {@link ByteBuffer}. The buffer's order is
	 * preserved.
	 * 
	 * @param b         - the buffer to be sliced
	 * @param fromIndex - the index of the first element in the range (inclusive)
	 * @param toIndex   - the index of the last element in the range (exclusive)
	 * 
	 * @return sliced buffer with order preserved
	 * 
	 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
	 * @throws IndexOutOfBoundsException if
	 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
	 * 
	 * @see ByteBuffer#slice()
	 * @see ByteBuffer#order()
	 */
	public static ByteBuffer slice(ByteBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);

		ByteBuffer dup = duplicate(b);
		dup.limit(toIndex);
		dup.position(fromIndex);
		return slice(dup);
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
	 * Returns a copy of a range from the specified buffer. The new buffer will be
	 * direct iff the specified buffer is direct, and will have the same byte order.
	 * The capacity will be equal to the size of the specified range. The limit will
	 * be set to the capacity, and the position will be set to zero.
	 * 
	 * @param b         - the buffer to be checked
	 * @param fromIndex - the index of the first element (inclusive) to be checked
	 * @param toIndex   - the index of the last element (exclusive) to be checked
	 * 
	 * @return a copy of a range of data from the specified buffer
	 * 
	 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
	 * @throws IndexOutOfBoundsException if
	 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
	 */
	public static ByteBuffer copy(ByteBuffer b, int fromIndex, int toIndex) {

		ByteBuffer slice = slice(b, fromIndex, toIndex);

		ByteBuffer copy = b.isDirect() ? ByteBuffer.allocateDirect(slice.capacity())
				: ByteBuffer.allocate(slice.capacity());
		copy.order(b.order());

		copy.put(slice);
		copy.flip();

		return copy;
	}

	/**
	 * Returns a copy of a range from the specified buffer. The new buffer will be
	 * direct iff the specified buffer is direct, and will have the same byte order.
	 * The capacity will be equal to the size of the specified range. The limit will
	 * be set to the capacity, and the position will be set to zero.
	 * 
	 * @param b         - the buffer to be checked
	 * @param fromIndex - the index of the first element (inclusive) to be checked
	 * @param toIndex   - the index of the last element (exclusive) to be checked
	 * 
	 * @return a copy of a range of data from the specified buffer
	 * 
	 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
	 * @throws IndexOutOfBoundsException if
	 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
	 */
	public static IntBuffer copy(IntBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);

		IntBuffer dup = b.duplicate();
		dup.limit(toIndex);
		dup.position(fromIndex);

		ByteBuffer copy = b.isDirect() ? ByteBuffer.allocateDirect(dup.remaining() * 4)
				: ByteBuffer.allocate(dup.remaining() * 4);
		copy.order(b.order());

		IntBuffer view = copy.asIntBuffer();
		view.put(dup);
		view.flip();

		return view;
	}

	/**
	 * Returns a copy of a range from the specified buffer. The new buffer will be
	 * direct iff the specified buffer is direct, and will have the same byte order.
	 * The capacity will be equal to the size of the specified range. The limit will
	 * be set to the capacity, and the position will be set to zero.
	 * 
	 * @param b         - the buffer to be checked
	 * @param fromIndex - the index of the first element (inclusive) to be checked
	 * @param toIndex   - the index of the last element (exclusive) to be checked
	 * 
	 * @return a copy of a range of data from the specified buffer
	 * 
	 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
	 * @throws IndexOutOfBoundsException if
	 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
	 */
	public static LongBuffer copy(LongBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);

		LongBuffer dup = b.duplicate();
		dup.limit(toIndex);
		dup.position(fromIndex);

		ByteBuffer copy = b.isDirect() ? ByteBuffer.allocateDirect(dup.remaining() * 8)
				: ByteBuffer.allocate(dup.remaining() * 8);
		copy.order(b.order());

		LongBuffer view = copy.asLongBuffer();
		view.put(dup);
		view.flip();

		return view;
	}

	/**
	 * Returns a copy of a range from the specified buffer. The new buffer will be
	 * direct iff the specified buffer is direct, and will have the same byte order.
	 * The capacity will be equal to the size of the specified range. The limit will
	 * be set to the capacity, and the position will be set to zero.
	 * 
	 * @param b         - the buffer to be checked
	 * @param fromIndex - the index of the first element (inclusive) to be checked
	 * @param toIndex   - the index of the last element (exclusive) to be checked
	 * 
	 * @return a copy of a range of data from the specified buffer
	 * 
	 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
	 * @throws IndexOutOfBoundsException if
	 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
	 */
	public static FloatBuffer copy(FloatBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);

		FloatBuffer dup = b.duplicate();
		dup.limit(toIndex);
		dup.position(fromIndex);

		ByteBuffer copy = b.isDirect() ? ByteBuffer.allocateDirect(dup.remaining() * 4)
				: ByteBuffer.allocate(dup.remaining() * 4);
		copy.order(b.order());

		FloatBuffer view = copy.asFloatBuffer();
		view.put(dup);
		view.flip();

		return view;
	}

	/**
	 * Returns a copy of a range from the specified buffer. The new buffer will be
	 * direct iff the specified buffer is direct, and will have the same byte order.
	 * The capacity will be equal to the size of the specified range. The limit will
	 * be set to the capacity, and the position will be set to zero.
	 * 
	 * @param b         - the buffer to be checked
	 * @param fromIndex - the index of the first element (inclusive) to be checked
	 * @param toIndex   - the index of the last element (exclusive) to be checked
	 * 
	 * @return a copy of a range of data from the specified buffer
	 * 
	 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
	 * @throws IndexOutOfBoundsException if
	 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
	 */
	public static DoubleBuffer copy(DoubleBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);

		DoubleBuffer dup = b.duplicate();
		dup.limit(toIndex);
		dup.position(fromIndex);

		ByteBuffer copy = b.isDirect() ? ByteBuffer.allocateDirect(dup.remaining() * 8)
				: ByteBuffer.allocate(dup.remaining() * 8);
		copy.order(b.order());

		DoubleBuffer view = copy.asDoubleBuffer();
		view.put(dup);
		view.flip();

		return view;
	}

	/**
	 * Deduplicates a range of the specified {@link IntBuffer}. The range must be
	 * sorted in ascending order prior to making this call. If it is not sorted, the
	 * results are undefined.
	 * <p>
	 * This method is useful as a post-processing step after a sort on a buffer
	 * which contains duplicate elements.
	 *
	 * @param b         the buffer to be deduplicated
	 * @param fromIndex - the index of the first element (inclusive) to be
	 *                  deduplicated
	 * @param toIndex   - the index of the last element (exclusive) to be
	 *                  deduplicated
	 * 
	 * @return the (exclusive) highest index in use after deduplicating
	 * 
	 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
	 * @throws IndexOutOfBoundsException if
	 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
	 */
	public static int deduplicate(IntBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);

		if (toIndex - fromIndex < 2)
			return toIndex;

		int prev = b.get(fromIndex);
		int highest = fromIndex + 1;

		for (int i = fromIndex + 1; i < toIndex; i++) {
			int value = b.get(i);

			if (value != prev) {
				if (highest < i)
					b.put(highest, value);

				highest++;
				prev = value;
			}
		}

		return highest;
	}

	/**
	 * Deduplicates a range of the specified {@link LongBuffer}. The range must be
	 * sorted in ascending order prior to making this call. If it is not sorted, the
	 * results are undefined.
	 * <p>
	 * This method is useful as a post-processing step after a sort on a buffer
	 * which contains duplicate elements.
	 *
	 * @param b         the buffer to be deduplicated
	 * @param fromIndex - the index of the first element (inclusive) to be
	 *                  deduplicated
	 * @param toIndex   - the index of the last element (exclusive) to be
	 *                  deduplicated
	 * 
	 * @return the (exclusive) highest index in use after deduplicating
	 * 
	 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
	 * @throws IndexOutOfBoundsException if
	 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
	 */
	public static int deduplicate(LongBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);

		if (toIndex - fromIndex < 2)
			return toIndex;

		long prev = b.get(fromIndex);
		int highest = fromIndex + 1;

		for (int i = fromIndex + 1; i < toIndex; i++) {
			long value = b.get(i);

			if (value != prev) {
				if (highest < i)
					b.put(highest, value);

				highest++;
				prev = value;
			}
		}

		return highest;
	}

	/**
	 * Deduplicates a range of the specified {@link FloatBuffer}. The range must be
	 * sorted in ascending order prior to making this call. If it is not sorted, the
	 * results are undefined. This method considers all NaN values to be equivalent
	 * and equal.
	 * <p>
	 * This method is useful as a post-processing step after a sort on a buffer
	 * which contains duplicate elements.
	 *
	 * @param b         the buffer to be deduplicated
	 * @param fromIndex - the index of the first element (inclusive) to be
	 *                  deduplicated
	 * @param toIndex   - the index of the last element (exclusive) to be
	 *                  deduplicated
	 * 
	 * @return the (exclusive) highest index in use after deduplicating
	 * 
	 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
	 * @throws IndexOutOfBoundsException if
	 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
	 */
	public static int deduplicate(FloatBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);

		if (toIndex - fromIndex < 2)
			return toIndex;

		float prev = b.get(fromIndex);
		int highest = fromIndex + 1;

		for (int i = fromIndex + 1; i < toIndex; i++) {
			float value = b.get(i);

			if (Float.compare(value, prev) != 0) {
				if (highest < i)
					b.put(highest, value);

				highest++;
				prev = value;
			}
		}

		return highest;
	}

	/**
	 * Deduplicates a range of the specified {@link DoubleBuffer}. The range must be
	 * sorted in ascending order prior to making this call. If it is not sorted, the
	 * results are undefined. This method considers all NaN values to be equivalent
	 * and equal.
	 * <p>
	 * This method is useful as a post-processing step after a sort on a buffer
	 * which contains duplicate elements.
	 *
	 * @param b         the buffer to be deduplicated
	 * @param fromIndex - the index of the first element (inclusive) to be
	 *                  deduplicated
	 * @param toIndex   - the index of the last element (exclusive) to be
	 *                  deduplicated
	 * 
	 * @return the (exclusive) highest index in use after deduplicating
	 * 
	 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
	 * @throws IndexOutOfBoundsException if
	 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
	 */
	public static int deduplicate(DoubleBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);

		if (toIndex - fromIndex < 2)
			return toIndex;

		double prev = b.get(fromIndex);
		int highest = fromIndex + 1;

		for (int i = fromIndex + 1; i < toIndex; i++) {
			double value = b.get(i);

			if (Double.compare(value, prev) != 0) {
				if (highest < i)
					b.put(highest, value);

				highest++;
				prev = value;
			}
		}

		return highest;
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

	/**
	 * Checks that {@code fromIndex} and {@code toIndex} are in the range and throws
	 * an exception if they aren't.
	 * 
	 * @param bufferCapacity - capacity of the buffer being checked
	 * @param fromIndex      - the index of the first element (inclusive) to be
	 *                       checked
	 * @param toIndex        - the index of the last element (inclusive) to be
	 *                       checked
	 */
	static void rangeCheckInclusive(int bufferCapacity, int fromIndex, int toIndex) {
		if (fromIndex > toIndex) {
			throw new IllegalArgumentException("fromIndex(" + fromIndex + ") > toIndex(" + toIndex + ")");
		}
		if (fromIndex < 0) {
			throw new IndexOutOfBoundsException("fromIndex(" + fromIndex + ") < 0");
		}
		if (toIndex >= bufferCapacity) {
			throw new IndexOutOfBoundsException("toIndex(" + toIndex + ") >= " + bufferCapacity);
		}
	}
}
