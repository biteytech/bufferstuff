package tech.bitey.bufferbitset;

import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;

import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import tech.bitey.bufferbitset.BufferUtils;

public class TestBufferUtils {

	private static final int IMIN = Integer.MIN_VALUE;
	private static final int IMAX = Integer.MAX_VALUE;

	private static final int[][] SORTED_INT = { {}, { 0 }, { 0, 0 }, { IMIN, IMIN }, { IMAX, IMAX }, { IMIN, IMAX },
			{ IMIN, 0, IMAX }, { -2, -1, 1, 2 }, };
	private static final int[][] NOT_SORTED_INT = { { 1, 0 }, { IMAX, IMIN }, { 3, 2, 1 }, };

	@Test
	public void isSortedInt() {
		Assertions.assertTrue(BufferUtils.isSorted(IntBuffer.allocate(0), 0, 0));
		Assertions.assertTrue(BufferUtils.isSorted(IntBuffer.allocate(1), 0, 0));
		Assertions.assertTrue(BufferUtils.isSorted(IntBuffer.allocate(1), 0, 1));

		for (int[] array : SORTED_INT) {
			IntBuffer b = IntBuffer.wrap(array);
			Assertions.assertTrue(BufferUtils.isSorted(b, 0, array.length));
			if (array.length > 0) {
				Assertions.assertTrue(BufferUtils.isSorted(b, 0, array.length - 1));
				Assertions.assertTrue(BufferUtils.isSorted(b, 1, array.length));
			}
		}

		for (int[] array : NOT_SORTED_INT)
			Assertions.assertFalse(BufferUtils.isSorted(IntBuffer.wrap(array), 0, array.length));
	}

	private static final int[][] SORTED_DISTINCT_INT = { {}, { 0 }, { IMIN, IMAX }, { IMIN, 0, IMAX },
			{ -2, -1, 1, 2 }, };
	private static final int[][] NOT_SORTED_DISTINCT_INT = { { 1, 0 }, { IMAX, IMIN }, { 3, 2, 1 }, { 0, 0 },
			{ IMIN, IMIN }, { IMAX, IMAX }, { -2, -1, -1 } };

	@Test
	public void isSortedAndDistinctInt() {
		Assertions.assertTrue(BufferUtils.isSortedAndDistinct(IntBuffer.allocate(0), 0, 0));
		Assertions.assertTrue(BufferUtils.isSortedAndDistinct(IntBuffer.allocate(1), 0, 0));
		Assertions.assertTrue(BufferUtils.isSortedAndDistinct(IntBuffer.allocate(1), 0, 1));

		for (int[] array : SORTED_DISTINCT_INT)
			Assertions.assertTrue(BufferUtils.isSortedAndDistinct(IntBuffer.wrap(array), 0, array.length));

		for (int[] array : NOT_SORTED_DISTINCT_INT)
			Assertions.assertFalse(BufferUtils.isSortedAndDistinct(IntBuffer.wrap(array), 0, array.length));
	}

	private static final long LMIN = Long.MIN_VALUE;
	private static final long LMAX = Long.MAX_VALUE;

	private static final long[][] SORTED_LONG = { {}, { 0 }, { 0, 0 }, { LMIN, LMIN }, { LMAX, LMAX }, { LMIN, LMAX },
			{ LMIN, 0, LMAX }, { -2, -1, 1, 2 }, };
	private static final long[][] NOT_SORTED_LONG = { { 1, 0 }, { LMAX, LMIN }, { 3, 2, 1 }, };

	@Test
	public void isSortedLong() {
		Assertions.assertTrue(BufferUtils.isSorted(LongBuffer.allocate(0), 0, 0));
		Assertions.assertTrue(BufferUtils.isSorted(LongBuffer.allocate(1), 0, 0));
		Assertions.assertTrue(BufferUtils.isSorted(LongBuffer.allocate(1), 0, 1));

		for (long[] array : SORTED_LONG) {
			LongBuffer b = LongBuffer.wrap(array);
			Assertions.assertTrue(BufferUtils.isSorted(b, 0, array.length));
			if (array.length > 0) {
				Assertions.assertTrue(BufferUtils.isSorted(b, 0, array.length - 1));
				Assertions.assertTrue(BufferUtils.isSorted(b, 1, array.length));
			}
		}

		for (long[] array : NOT_SORTED_LONG)
			Assertions.assertFalse(BufferUtils.isSorted(LongBuffer.wrap(array), 0, array.length));
	}

	private static final long[][] SORTED_DISTINCT_LONG = { {}, { 0 }, { LMIN, LMAX }, { LMIN, 0, LMAX },
			{ -2, -1, 1, 2 }, };
	private static final long[][] NOT_SORTED_DISTINCT_LONG = { { 1, 0 }, { LMAX, LMIN }, { 3, 2, 1 }, { 0, 0 },
			{ LMIN, LMIN }, { LMAX, LMAX }, { -2, -1, -1 } };

	@Test
	public void isSortedAndDistinctLong() {
		Assertions.assertTrue(BufferUtils.isSortedAndDistinct(LongBuffer.allocate(0), 0, 0));
		Assertions.assertTrue(BufferUtils.isSortedAndDistinct(LongBuffer.allocate(1), 0, 0));
		Assertions.assertTrue(BufferUtils.isSortedAndDistinct(LongBuffer.allocate(1), 0, 1));

		for (long[] array : SORTED_DISTINCT_LONG)
			Assertions.assertTrue(BufferUtils.isSortedAndDistinct(LongBuffer.wrap(array), 0, array.length));

		for (long[] array : NOT_SORTED_DISTINCT_LONG)
			Assertions.assertFalse(BufferUtils.isSortedAndDistinct(LongBuffer.wrap(array), 0, array.length));
	}

	private static final float FMIN = Float.NEGATIVE_INFINITY;
	private static final float FMAX = Float.POSITIVE_INFINITY;
	private static final float FNAN = Float.NaN;

	private static final float[][] SORTED_FLOAT = { {}, { 0 }, { 0, 0 }, { FMIN, FMIN }, { FMAX, FMAX }, { FMIN, FMAX },
			{ FMIN, 0, FMAX }, { -2, -1, 1, 2 }, { FNAN }, { 0, FNAN }, { 0, 0, FNAN }, { FMIN, FMIN, FNAN },
			{ FMAX, FMAX, FNAN }, { FMIN, FMAX, FNAN }, { FMIN, 0, FMAX, FNAN }, { -2, -1, 1, 2, FNAN } };
	private static final float[][] NOT_SORTED_FLOAT = { { 1, 0 }, { FMAX, FMIN }, { 3, 2, 1 }, { FNAN, FMAX } };

	@Test
	public void isSortedFloat() {
		Assertions.assertTrue(BufferUtils.isSorted(FloatBuffer.allocate(0), 0, 0));
		Assertions.assertTrue(BufferUtils.isSorted(FloatBuffer.allocate(1), 0, 0));
		Assertions.assertTrue(BufferUtils.isSorted(FloatBuffer.allocate(1), 0, 1));

		for (float[] array : SORTED_FLOAT) {
			FloatBuffer b = FloatBuffer.wrap(array);
			Assertions.assertTrue(BufferUtils.isSorted(b, 0, array.length));
			if (array.length > 0) {
				Assertions.assertTrue(BufferUtils.isSorted(b, 0, array.length - 1));
				Assertions.assertTrue(BufferUtils.isSorted(b, 1, array.length));
			}
		}

		for (float[] array : NOT_SORTED_FLOAT)
			Assertions.assertFalse(BufferUtils.isSorted(FloatBuffer.wrap(array), 0, array.length));
	}

	private static final float[][] SORTED_DISTINCT_FLOAT = { {}, { 0 }, { IMIN, IMAX }, { IMIN, 0, IMAX },
			{ -2, -1, 1, 2 }, };
	private static final float[][] NOT_SORTED_DISTINCT_FLOAT = { { 1, 0 }, { IMAX, IMIN }, { 3, 2, 1 }, { 0, 0 },
			{ IMIN, IMIN }, { IMAX, IMAX }, { -2, -1, -1 } };

	@Test
	public void isSortedAndDistinctFloat() {
		Assertions.assertTrue(BufferUtils.isSortedAndDistinct(FloatBuffer.allocate(0), 0, 0));
		Assertions.assertTrue(BufferUtils.isSortedAndDistinct(FloatBuffer.allocate(1), 0, 0));
		Assertions.assertTrue(BufferUtils.isSortedAndDistinct(FloatBuffer.allocate(1), 0, 1));

		for (float[] array : SORTED_DISTINCT_FLOAT)
			Assertions.assertTrue(BufferUtils.isSortedAndDistinct(FloatBuffer.wrap(array), 0, array.length));

		for (float[] array : NOT_SORTED_DISTINCT_FLOAT)
			Assertions.assertFalse(BufferUtils.isSortedAndDistinct(FloatBuffer.wrap(array), 0, array.length));
	}

	private static final double DMIN = Double.NEGATIVE_INFINITY;
	private static final double DMAX = Double.POSITIVE_INFINITY;
	private static final double DNAN = Double.NaN;

	private static final double[][] SORTED_DOUBLE = { {}, { 0 }, { 0, 0 }, { DMIN, DMIN }, { DMAX, DMAX },
			{ DMIN, DMAX }, { DMIN, 0, DMAX }, { -2, -1, 1, 2 }, { DNAN }, { 0, DNAN }, { 0, 0, DNAN },
			{ DMIN, DMIN, DNAN }, { DMAX, DMAX, DNAN }, { DMIN, DMAX, DNAN }, { DMIN, 0, DMAX, DNAN },
			{ -2, -1, 1, 2, DNAN } };
	private static final double[][] NOT_SORTED_DOUBLE = { { 1, 0 }, { DMAX, DMIN }, { 3, 2, 1 }, { DNAN, DMAX } };

	@Test
	public void isSortedDouble() {
		Assertions.assertTrue(BufferUtils.isSorted(DoubleBuffer.allocate(0), 0, 0));
		Assertions.assertTrue(BufferUtils.isSorted(DoubleBuffer.allocate(1), 0, 0));
		Assertions.assertTrue(BufferUtils.isSorted(DoubleBuffer.allocate(1), 0, 1));

		for (double[] array : SORTED_DOUBLE) {
			DoubleBuffer b = DoubleBuffer.wrap(array);
			Assertions.assertTrue(BufferUtils.isSorted(b, 0, array.length));
			if (array.length > 0) {
				Assertions.assertTrue(BufferUtils.isSorted(b, 0, array.length - 1));
				Assertions.assertTrue(BufferUtils.isSorted(b, 1, array.length));
			}
		}

		for (double[] array : NOT_SORTED_DOUBLE)
			Assertions.assertFalse(BufferUtils.isSorted(DoubleBuffer.wrap(array), 0, array.length));
	}

	private static final double[][] SORTED_DISTINCT_DOUBLE = { {}, { 0 }, { IMIN, IMAX }, { IMIN, 0, IMAX },
			{ -2, -1, 1, 2 }, };
	private static final double[][] NOT_SORTED_DISTINCT_DOUBLE = { { 1, 0 }, { IMAX, IMIN }, { 3, 2, 1 }, { 0, 0 },
			{ IMIN, IMIN }, { IMAX, IMAX }, { -2, -1, -1 } };

	@Test
	public void isSortedAndDistinctDouble() {
		Assertions.assertTrue(BufferUtils.isSortedAndDistinct(DoubleBuffer.allocate(0), 0, 0));
		Assertions.assertTrue(BufferUtils.isSortedAndDistinct(DoubleBuffer.allocate(1), 0, 0));
		Assertions.assertTrue(BufferUtils.isSortedAndDistinct(DoubleBuffer.allocate(1), 0, 1));

		for (double[] array : SORTED_DISTINCT_DOUBLE)
			Assertions.assertTrue(BufferUtils.isSortedAndDistinct(DoubleBuffer.wrap(array), 0, array.length));

		for (double[] array : NOT_SORTED_DISTINCT_DOUBLE)
			Assertions.assertFalse(BufferUtils.isSortedAndDistinct(DoubleBuffer.wrap(array), 0, array.length));
	}

	@Test
	public void rangeCheck() {
		try {
			BufferUtils.rangeCheck(10, 5, 3);
			throw new RuntimeException("Expected IllegalArgumentException");
		} catch (IllegalArgumentException e) {
			// good
		}
		try {
			BufferUtils.rangeCheck(10, -1, 3);
			throw new RuntimeException("Expected IndexOutOfBoundsException");
		} catch (IndexOutOfBoundsException e) {
			// good
		}
		try {
			BufferUtils.rangeCheck(10, 5, 11);
			throw new RuntimeException("Expected IndexOutOfBoundsException");
		} catch (IndexOutOfBoundsException e) {
			// good
		}
	}
}
