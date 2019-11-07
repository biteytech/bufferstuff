package tech.bitey.bufferstuff;

import static tech.bitey.bufferstuff.BufferUtils.rangeCheck;

import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;

/**
 * Sorting algorithms for nio buffers.
 * <p>
 * <u>Supported Buffer Types</u>
 * <ul>
 * <li>{@link IntBuffer}
 * <li>{@link LongBuffer}
 * <li>{@link FloatBuffer}
 * <li>{@link DoubleBuffer}
 * </ul>
 * 
 * @author biteytech@protonmail.com
 */
public class BufferSort {

	/**
	 * Sorts a range of the specified {@link IntBuffer} in ascending order (lowest
	 * first). The sort is guaranteed to be:
	 * <ul>
	 * <li>in-place
	 * <li>{@code O(n*log(n))} in the worst case
	 * </ul>
	 *
	 * @param b         the buffer to be sorted
	 * @param fromIndex the index of the first element (inclusive) to be sorted
	 * @param toIndex   the index of the last element (exclusive) to be sorted
	 * 
	 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
	 * @throws IndexOutOfBoundsException if
	 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
	 */
	public static void inplaceSort(IntBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);
		heapSort(b, fromIndex, toIndex);
	}

	// based on https://www.programiz.com/dsa/heap-sort
	private static void heapSort(IntBuffer b, int fromIndex, int toIndex) {

		int n = toIndex - fromIndex;
		if (n <= 1)
			return;

		// Build max heap
		for (int i = fromIndex + n / 2 - 1; i >= fromIndex; i--)
			heapify(b, toIndex, i, fromIndex);

		// Heap sort
		for (int i = toIndex - 1; i >= fromIndex; i--) {
			swap(b, fromIndex, i);

			// Heapify root element
			heapify(b, i, fromIndex, fromIndex);
		}
	}

	// based on https://www.programiz.com/dsa/heap-sort
	private static void heapify(IntBuffer b, int n, int i, int offset) {
		// Find largest among root, left child and right child
		int largest = i;
		int l = 2 * i + 1 - offset;
		int r = l + 1;

		if (l < n && b.get(l) > b.get(largest))
			largest = l;

		if (r < n && b.get(r) > b.get(largest))
			largest = r;

		// Swap and continue heapifying if root is not largest
		if (largest != i) {
			swap(b, i, largest);
			heapify(b, n, largest, offset);
		}
	}

	private static void swap(IntBuffer b, int i, int j) {
		int swap = b.get(i);
		b.put(i, b.get(j));
		b.put(j, swap);
	}

	/**
	 * Sorts a range of the specified {@link LongBuffer} in ascending order (lowest
	 * first). The sort is guaranteed to be:
	 * <ul>
	 * <li>in-place
	 * <li>{@code O(n*log(n))} in the worst case
	 * </ul>
	 *
	 * @param b         the buffer to be sorted
	 * @param fromIndex the index of the first element (inclusive) to be sorted
	 * @param toIndex   the index of the last element (exclusive) to be sorted
	 * 
	 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
	 * @throws IndexOutOfBoundsException if
	 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
	 */
	public static void inplaceSort(LongBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);
		heapSort(b, fromIndex, toIndex);
	}

	// based on https://www.programiz.com/dsa/heap-sort
	private static void heapSort(LongBuffer b, int fromIndex, int toIndex) {

		int n = toIndex - fromIndex;
		if (n <= 1)
			return;

		// Build max heap
		for (int i = fromIndex + n / 2 - 1; i >= fromIndex; i--)
			heapify(b, toIndex, i, fromIndex);

		// Heap sort
		for (int i = toIndex - 1; i >= fromIndex; i--) {
			swap(b, fromIndex, i);

			// Heapify root element
			heapify(b, i, fromIndex, fromIndex);
		}
	}

	// based on https://www.programiz.com/dsa/heap-sort
	private static void heapify(LongBuffer b, int n, int i, int offset) {
		// Find largest among root, left child and right child
		int largest = i;
		int l = 2 * i + 1 - offset;
		int r = l + 1;

		if (l < n && b.get(l) > b.get(largest))
			largest = l;

		if (r < n && b.get(r) > b.get(largest))
			largest = r;

		// Swap and continue heapifying if root is not largest
		if (largest != i) {
			swap(b, i, largest);
			heapify(b, n, largest, offset);
		}
	}

	private static void swap(LongBuffer b, int i, int j) {
		long swap = b.get(i);
		b.put(i, b.get(j));
		b.put(j, swap);
	}

	/**
	 * Sorts a range of the specified {@link FloatBuffer} in ascending order (lowest
	 * first). The sort is guaranteed to be:
	 * <ul>
	 * <li>in-place
	 * <li>{@code O(n*log(n))} in the worst case
	 * </ul>
	 * <p>
	 * This method considers all NaN values to be equal to each other and greater
	 * than all other values (including {@code Float.POSITIVE_INFINITY}).
	 *
	 * @param b         the buffer to be sorted
	 * @param fromIndex the index of the first element (inclusive) to be sorted
	 * @param toIndex   the index of the last element (exclusive) to be sorted
	 * 
	 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
	 * @throws IndexOutOfBoundsException if
	 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
	 */
	public static void inplaceSort(FloatBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);
		heapSort(b, fromIndex, toIndex);
	}

	// based on https://www.programiz.com/dsa/heap-sort
	private static void heapSort(FloatBuffer b, int fromIndex, int toIndex) {

		int n = toIndex - fromIndex;
		if (n <= 1)
			return;

		// Build max heap
		for (int i = fromIndex + n / 2 - 1; i >= fromIndex; i--)
			heapify(b, toIndex, i, fromIndex);

		// Heap sort
		for (int i = toIndex - 1; i >= fromIndex; i--) {
			swap(b, fromIndex, i);

			// Heapify root element
			heapify(b, i, fromIndex, fromIndex);
		}
	}

	// based on https://www.programiz.com/dsa/heap-sort
	private static void heapify(FloatBuffer b, int n, int i, int offset) {
		// Find largest among root, left child and right child
		int largest = i;
		int l = 2 * i + 1 - offset;
		int r = l + 1;

		if (l < n && Float.compare(b.get(l), b.get(largest)) > 0)
			largest = l;

		if (r < n && Float.compare(b.get(r), b.get(largest)) > 0)
			largest = r;

		// Swap and continue heapifying if root is not largest
		if (largest != i) {
			swap(b, i, largest);
			heapify(b, n, largest, offset);
		}
	}

	private static void swap(FloatBuffer b, int i, int j) {
		float swap = b.get(i);
		b.put(i, b.get(j));
		b.put(j, swap);
	}

	/**
	 * Sorts a range of the specified {@link DoubleBuffer} in ascending order (lowest
	 * first). The sort is guaranteed to be:
	 * <ul>
	 * <li>in-place
	 * <li>{@code O(n*log(n))} in the worst case
	 * </ul>
	 * <p>
	 * This method considers all NaN values to be equal to each other and greater
	 * than all other values (including {@code Double.POSITIVE_INFINITY}).
	 *
	 * @param b         the buffer to be sorted
	 * @param fromIndex the index of the first element (inclusive) to be sorted
	 * @param toIndex   the index of the last element (exclusive) to be sorted
	 * 
	 * @throws IllegalArgumentException  if {@code fromIndex > toIndex}
	 * @throws IndexOutOfBoundsException if
	 *                                   {@code fromIndex < 0 or toIndex > b.capacity()}
	 */
	public static void inplaceSort(DoubleBuffer b, int fromIndex, int toIndex) {
		rangeCheck(b.capacity(), fromIndex, toIndex);
		heapSort(b, fromIndex, toIndex);
	}

	// based on https://www.programiz.com/dsa/heap-sort
	private static void heapSort(DoubleBuffer b, int fromIndex, int toIndex) {

		int n = toIndex - fromIndex;
		if (n <= 1)
			return;

		// Build max heap
		for (int i = fromIndex + n / 2 - 1; i >= fromIndex; i--)
			heapify(b, toIndex, i, fromIndex);

		// Heap sort
		for (int i = toIndex - 1; i >= fromIndex; i--) {
			swap(b, fromIndex, i);

			// Heapify root element
			heapify(b, i, fromIndex, fromIndex);
		}
	}

	// based on https://www.programiz.com/dsa/heap-sort
	private static void heapify(DoubleBuffer b, int n, int i, int offset) {
		// Find largest among root, left child and right child
		int largest = i;
		int l = 2 * i + 1 - offset;
		int r = l + 1;

		if (l < n && Double.compare(b.get(l), b.get(largest)) > 0)
			largest = l;

		if (r < n && Double.compare(b.get(r), b.get(largest)) > 0)
			largest = r;

		// Swap and continue heapifying if root is not largest
		if (largest != i) {
			swap(b, i, largest);
			heapify(b, n, largest, offset);
		}
	}

	private static void swap(DoubleBuffer b, int i, int j) {
		double swap = b.get(i);
		b.put(i, b.get(j));
		b.put(j, swap);
	}
}
