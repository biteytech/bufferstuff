package tech.bitey.bufferstuff;

import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.util.Arrays;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestBufferSort {

	private final int[] isorted = { 1, 2, 3 };
	private final int[] ireverse = { 3, 2, 1 };

	private final int[] irandom = { -311, -509, -74, -128, -695, 859, 852, -888, -149, -431, 589, -354, 71, -110, 236,
			74, 976, -653, -80, 420, -340, -686, -275, 740, 265, -937, 118, -948, 667, -743, -194, 186, -498, -830, 995,
			-847, -334, 922, -521, -786, -179, 117, -971, -823, 593, -235, 344, -827, -246, 324, -662, -489, 153, 969,
			-593, -214, 75, -643, 26, -188, 2, 640, -799, -231, 299, -927, -870, 473, 388, -96, -505, -891, 423, -660,
			140, 64, -364, -636, 280, 930, 701, 278, 180, 554, 113, 910, -883, 924, 986, 374, 4, 616, 443, 444, 261,
			-843, -25, -252, -837, -43, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE };

	@Test
	public void sortInt() {

		sort(new int[] {}, 0, 0);
		sort(new int[] { 0 }, 0, 1);
		sort(new int[] { 0, 0 }, 0, 2);
		sort(isorted, 0, 3);
		sort(ireverse, 0, 3);

		for (int l = 2; l <= irandom.length; l++) {
			for (int fromIndex = 0;; fromIndex++) {
				int toIndex = fromIndex + l;
				if (toIndex > irandom.length)
					break;
				sort(irandom, fromIndex, toIndex);
			}
		}
	}

	public void sort(int[] array, int fromIndex, int toIndex) {

		int[] expected = Arrays.copyOf(array, array.length);
		Arrays.sort(expected, fromIndex, toIndex);

		IntBuffer actual = IntBuffer.wrap(Arrays.copyOf(array, array.length));
		BufferSort.inplaceSort(actual, fromIndex, toIndex);

		Assertions.assertArrayEquals(Arrays.copyOfRange(expected, fromIndex, toIndex),
				Arrays.copyOfRange(actual.array(), fromIndex, toIndex));
	}

	private final long[] lsorted = { 1, 2, 3 };
	private final long[] lreverse = { 3, 2, 1 };

	private final long[] lrandom = { -311, -509, -74, -128, -695, 859, 852, -888, -149, -431, 589, -354, 71, -110, 236,
			74, 976, -653, -80, 420, -340, -686, -275, 740, 265, -937, 118, -948, 667, -743, -194, 186, -498, -830, 995,
			-847, -334, 922, -521, -786, -179, 117, -971, -823, 593, -235, 344, -827, -246, 324, -662, -489, 153, 969,
			-593, -214, 75, -643, 26, -188, 2, 640, -799, -231, 299, -927, -870, 473, 388, -96, -505, -891, 423, -660,
			140, 64, -364, -636, 280, 930, 701, 278, 180, 554, 113, 910, -883, 924, 986, 374, 4, 616, 443, 444, 261,
			-843, -25, -252, -837, -43, Long.MAX_VALUE, Long.MAX_VALUE, Long.MIN_VALUE, Long.MIN_VALUE };

	@Test
	public void sortLong() {

		sort(new long[] {}, 0, 0);
		sort(new long[] { 0 }, 0, 1);
		sort(new long[] { 0, 0 }, 0, 2);
		sort(lsorted, 0, 3);
		sort(lreverse, 0, 3);

		for (int l = 2; l <= lrandom.length; l++) {
			for (int fromIndex = 0;; fromIndex++) {
				int toIndex = fromIndex + l;
				if (toIndex > lrandom.length)
					break;
				sort(lrandom, fromIndex, toIndex);
			}
		}
	}

	public void sort(long[] array, int fromIndex, int toIndex) {

		long[] expected = Arrays.copyOf(array, array.length);
		Arrays.sort(expected, fromIndex, toIndex);

		LongBuffer actual = LongBuffer.wrap(Arrays.copyOf(array, array.length));
		BufferSort.inplaceSort(actual, fromIndex, toIndex);

		Assertions.assertArrayEquals(Arrays.copyOfRange(expected, fromIndex, toIndex),
				Arrays.copyOfRange(actual.array(), fromIndex, toIndex));
	}

	private final float[] fsorted = { 1, 2, 3 };
	private final float[] freverse = { 3, 2, 1 };

	private final float[] frandom = { -311, -509, -74, -128, -695, 859, 852, -888, -149, -431, 589, -354, 71, -110, 236,
			74, 976, -653, -80, 420, -340, -686, -275, 740, 265, -937, 118, -948, 667, -743, -194, 186, -498, -830, 995,
			-847, -334, 922, -521, -786, -179, 117, -971, -823, 593, -235, 344, -827, -246, 324, -662, -489, 153, 969,
			-593, -214, 75, -643, 26, -188, 2, 640, -799, -231, 299, -927, -870, 473, 388, -96, -505, -891, 423, -660,
			140, 64, -364, -636, 280, 930, 701, 278, 180, 554, 113, 910, -883, 924, 986, 374, 4, 616, 443, 444, 261,
			-843, -25, -252, -837, -43, Float.POSITIVE_INFINITY, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY,
			Float.NEGATIVE_INFINITY };

	@Test
	public void sortFloat() {

		sort(new float[] {}, 0, 0);
		sort(new float[] { 0 }, 0, 1);
		sort(new float[] { 0, 0 }, 0, 2);
		sort(new float[] { Float.NaN, Float.POSITIVE_INFINITY, Float.NaN, Float.MAX_VALUE, 0, -0f, Float.NEGATIVE_INFINITY }, 0, 7);
		sort(fsorted, 0, 3);
		sort(freverse, 0, 3);

		for (int l = 2; l <= frandom.length; l++) {
			for (int fromIndex = 0;; fromIndex++) {
				int toIndex = fromIndex + l;
				if (toIndex > frandom.length)
					break;
				sort(frandom, fromIndex, toIndex);
			}
		}
	}

	public void sort(float[] array, int fromIndex, int toIndex) {

		float[] expected = Arrays.copyOf(array, array.length);
		Arrays.sort(expected, fromIndex, toIndex);

		FloatBuffer actual = FloatBuffer.wrap(Arrays.copyOf(array, array.length));
		BufferSort.inplaceSort(actual, fromIndex, toIndex);

		Assertions.assertArrayEquals(Arrays.copyOfRange(expected, fromIndex, toIndex),
				Arrays.copyOfRange(actual.array(), fromIndex, toIndex));
	}

	private final double[] dsorted = { 1, 2, 3 };
	private final double[] dreverse = { 3, 2, 1 };

	private final double[] drandom = { -311, -509, -74, -128, -695, 859, 852, -888, -149, -431, 589, -354, 71, -110, 236,
			74, 976, -653, -80, 420, -340, -686, -275, 740, 265, -937, 118, -948, 667, -743, -194, 186, -498, -830, 995,
			-847, -334, 922, -521, -786, -179, 117, -971, -823, 593, -235, 344, -827, -246, 324, -662, -489, 153, 969,
			-593, -214, 75, -643, 26, -188, 2, 640, -799, -231, 299, -927, -870, 473, 388, -96, -505, -891, 423, -660,
			140, 64, -364, -636, 280, 930, 701, 278, 180, 554, 113, 910, -883, 924, 986, 374, 4, 616, 443, 444, 261,
			-843, -25, -252, -837, -43, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY,
			Double.NEGATIVE_INFINITY };

	@Test
	public void sortDouble() {

		sort(new double[] {}, 0, 0);
		sort(new double[] { 0 }, 0, 1);
		sort(new double[] { 0, 0 }, 0, 2);
		sort(new double[] { Double.NaN, Double.POSITIVE_INFINITY, Double.NaN, Double.MAX_VALUE, 0, -0f, Double.NEGATIVE_INFINITY }, 0, 7);
		sort(dsorted, 0, 3);
		sort(dreverse, 0, 3);

		for (int l = 2; l <= drandom.length; l++) {
			for (int fromIndex = 0;; fromIndex++) {
				int toIndex = fromIndex + l;
				if (toIndex > drandom.length)
					break;
				sort(drandom, fromIndex, toIndex);
			}
		}
	}

	public void sort(double[] array, int fromIndex, int toIndex) {

		double[] expected = Arrays.copyOf(array, array.length);
		Arrays.sort(expected, fromIndex, toIndex);

		DoubleBuffer actual = DoubleBuffer.wrap(Arrays.copyOf(array, array.length));
		BufferSort.inplaceSort(actual, fromIndex, toIndex);

		Assertions.assertArrayEquals(Arrays.copyOfRange(expected, fromIndex, toIndex),
				Arrays.copyOfRange(actual.array(), fromIndex, toIndex));
	}
}
