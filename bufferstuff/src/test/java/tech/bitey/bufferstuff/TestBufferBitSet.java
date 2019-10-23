package tech.bitey.bufferstuff;

import static tech.bitey.bufferstuff.ResizeBehavior.ALLOCATE;
import static tech.bitey.bufferstuff.ResizeBehavior.ALLOCATE_DIRECT;
import static tech.bitey.bufferstuff.ResizeBehavior.NO_RESIZE;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.BitSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.ObjIntConsumer;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import tech.bitey.bufferstuff.BufferBitSet;
import tech.bitey.bufferstuff.ResizeBehavior;

public class TestBufferBitSet {

	private static final TreeSet<Integer> SAMPLE_INDICES = new TreeSet<>(
			Arrays.asList(1, 10, 38, 39, 40, 41, 42, 100, 9000));

	private static void populateWithSampleIndices(BufferBitSet bs) {
		for (int index : SAMPLE_INDICES)
			bs.set(index);
	}

	@Test
	public void basicGetAndSet() {
		Map<BufferBitSet, Boolean> setsAndIfThrows = new IdentityHashMap<>();
		setsAndIfThrows.put(new BufferBitSet(), false);
		setsAndIfThrows.put(new BufferBitSet(NO_RESIZE), true);
		setsAndIfThrows.put(new BufferBitSet(ALLOCATE), false);
		setsAndIfThrows.put(new BufferBitSet(ALLOCATE_DIRECT), false);
		setsAndIfThrows.put(new BufferBitSet(ByteBuffer.allocate(0)), true);
		setsAndIfThrows.put(new BufferBitSet(ByteBuffer.allocate(0), NO_RESIZE), true);
		setsAndIfThrows.put(new BufferBitSet(ByteBuffer.allocate(2000), NO_RESIZE), false);
		setsAndIfThrows.put(new BufferBitSet().withResizeBehavior(NO_RESIZE), true);
		setsAndIfThrows.put(BufferBitSet.valueOf(new byte[0]), false);
		setsAndIfThrows.put(BufferBitSet.valueOf(new byte[] { 0 }), false);

		for (Map.Entry<BufferBitSet, Boolean> e : setsAndIfThrows.entrySet()) {
			try {
				basicGetAndSet(e.getKey());
				if (e.getValue())
					throw new RuntimeException("Expected IndexOutOfBoundsException");
			} catch (IndexOutOfBoundsException ex) {
				if (!e.getValue())
					throw ex;
			}
		}
	}

	private void basicGetAndSet(BufferBitSet bs) {

		populateWithSampleIndices(bs);
		Assertions.assertEquals(SAMPLE_INDICES.toString(), bs.toString());

		for (int i = 0; i < 20000; i++)
			Assertions.assertEquals(SAMPLE_INDICES.contains(i), bs.get(i));
	}

	@Test
	public void toFromByteArray() {

		BufferBitSet bs = new BufferBitSet();
		populateWithSampleIndices(bs);

		BufferBitSet rebuilt = BufferBitSet.valueOf(bs.toByteArray());
		Assertions.assertEquals(SAMPLE_INDICES.toString(), rebuilt.toString());
	}

	@Test
	public void toFromBitSet() {

		BitSet bs = new BitSet();
		for (int i : SAMPLE_INDICES)
			bs.set(i);

		BufferBitSet bbs = BufferBitSet.valueOf(bs);
		Assertions.assertEquals(SAMPLE_INDICES.toString(), bbs.toString());

		BitSet rebuilt = bbs.toBitSet();
		Assertions.assertEquals(bs, rebuilt);
	}

	private void basicFlipOrClear(ObjIntConsumer<BufferBitSet> op) {

		BufferBitSet bs = new BufferBitSet();
		populateWithSampleIndices(bs);

		Set<Integer> expected = new TreeSet<>(SAMPLE_INDICES);

		expected.remove(9000);
		op.accept(bs, 9000);

		expected.remove(38);
		op.accept(bs, 38);

		Assertions.assertEquals(expected.toString(), bs.toString());
	}

	@Test
	public void basicFlip() {
		basicFlipOrClear(BufferBitSet::flip);
	}

	@Test
	public void basicClear() {
		basicFlipOrClear(BufferBitSet::clear);

		BufferBitSet bs = new BufferBitSet();
		populateWithSampleIndices(bs);
		bs.clear(Integer.MAX_VALUE);
		Assertions.assertEquals(SAMPLE_INDICES.toString(), bs.toString());
	}

	@Test
	public void setRange() {
		setRange(0, 0);
		setRange(1, 1);
		setRange(1, 2);
		setRange(0, 7);
		setRange(0, 9);
		setRange(8, 16);
		setRange(5, 20);
		setRange(1000, 2000);
	}

	private void setRange(int fromIndex, int toIndex) {

		BufferBitSet bs = new BufferBitSet();
		bs.set(fromIndex, toIndex);

		Set<Integer> expected = new TreeSet<>();
		for (int i = fromIndex; i < toIndex; i++)
			expected.add(i);

		Assertions.assertEquals(expected.toString(), bs.toString());
	}

	@Test
	public void clearRange() {
		clearRange(0, 0);
		clearRange(1, 1);
		clearRange(1, 2);
		clearRange(0, 7);
		clearRange(0, 9);
		clearRange(8, 16);
		clearRange(5, 20);
		clearRange(1000, 2000);
	}

	private void clearRange(int fromIndex, int toIndex) {

		BufferBitSet bs = new BufferBitSet();
		bs.set(0, toIndex + 1);
		bs.clear(fromIndex, toIndex);

		Set<Integer> expected = new TreeSet<>();
		for (int i = 0; i < fromIndex; i++)
			expected.add(i);
		expected.add(toIndex);

		Assertions.assertEquals(expected.toString(), bs.toString());
	}

	@Test
	public void flipRange() {
		BufferBitSet bs = new BufferBitSet();
		populateWithSampleIndices(bs);

		bs.flip(0, 10000);

		Set<Integer> expected = new TreeSet<>();
		for (int i = 0; i < 10000; i++)
			if (!SAMPLE_INDICES.contains(i))
				expected.add(i);

		Assertions.assertEquals(expected.toString(), bs.toString());
	}

	@Test
	public void getRange() {
		BufferBitSet bs = new BufferBitSet();
		populateWithSampleIndices(bs);

		Assertions.assertEquals("[]", bs.get(0, 0).toString());
		Assertions.assertEquals("[]", bs.get(10000, 20000).toString());
		Assertions.assertEquals("[0, 28, 29]", bs.get(10, 40).toString());
		Assertions.assertEquals(SAMPLE_INDICES.toString(), bs.get(0, 9001).toString());
		Assertions.assertEquals(SAMPLE_INDICES.toString(), bs.get(0, 10000).toString());

		for (int shift = 1; shift <= 100; shift++) {
			final int s = shift;
			Set<Integer> expected = new TreeSet<>(
					SAMPLE_INDICES.stream().map(i -> i - s).filter(i -> i >= 0).collect(Collectors.toSet()));

			Assertions.assertEquals(expected.toString(), bs.get(s, 9001).toString());
		}
	}

	@Test
	public void nextSetBit() {
		BufferBitSet bs = new BufferBitSet();
		populateWithSampleIndices(bs);

		Set<Integer> actual = new TreeSet<>();
		for (int bit = bs.nextSetBit(0); bit != -1; bit = bs.nextSetBit(bit + 1))
			actual.add(bit);

		Assertions.assertEquals(SAMPLE_INDICES, actual);

		Assertions.assertEquals(-1, bs.nextSetBit(10000));
	}

	@Test
	public void nextClearBit() {
		BufferBitSet bs = new BufferBitSet();
		populateWithSampleIndices(bs);
		bs.flip(0, 10000);

		Set<Integer> actual = new TreeSet<>();
		for (int bit = bs.nextClearBit(0); bit <= 9000; bit = bs.nextClearBit(bit + 1))
			actual.add(bit);

		Assertions.assertEquals(SAMPLE_INDICES, actual);

		Assertions.assertEquals(20000, bs.nextClearBit(20000));
	}

	@Test
	public void previousSetBit() {
		BufferBitSet bs = new BufferBitSet();
		populateWithSampleIndices(bs);

		Set<Integer> actual = new TreeSet<>();
		for (int bit = bs.previousSetBit(10000); bit != -1; bit = bs.previousSetBit(bit - 1))
			actual.add(bit);

		Assertions.assertEquals(SAMPLE_INDICES, actual);

		Assertions.assertEquals(-1, bs.previousSetBit(0));
	}

	@Test
	public void previousClearBit() {
		BufferBitSet bs = new BufferBitSet();
		populateWithSampleIndices(bs);
		bs.flip(0, 10000);

		Set<Integer> actual = new TreeSet<>();
		for (int bit = SAMPLE_INDICES.last(); bit != -1; bit = bs.previousClearBit(bit - 1))
			actual.add(bit);

		Assertions.assertEquals(SAMPLE_INDICES, actual);

		Assertions.assertEquals(-1, bs.previousClearBit(0));
	}

	@Test
	public void and() {
		BufferBitSet bs1 = new BufferBitSet();
		populateWithSampleIndices(bs1);
		bs1.clear(1);

		BufferBitSet bs2 = new BufferBitSet();
		populateWithSampleIndices(bs2);
		bs2.clear(9000);

		Set<Integer> expected = new TreeSet<>(SAMPLE_INDICES);
		expected.remove(1);
		expected.remove(9000);

		bs1.and(bs2);
		Assertions.assertEquals(expected.toString(), bs1.toString());

		bs1.and(bs1);
		Assertions.assertEquals(expected.toString(), bs1.toString());
	}

	@Test
	public void or() {
		BufferBitSet bs1 = new BufferBitSet();
		populateWithSampleIndices(bs1);
		bs1.clear(1);

		BufferBitSet bs2 = new BufferBitSet();
		populateWithSampleIndices(bs2);
		bs2.clear(9000);
		bs2.set(20000);

		Set<Integer> expected = new TreeSet<>(SAMPLE_INDICES);
		expected.add(20000);

		bs1.or(bs2);
		Assertions.assertEquals(expected.toString(), bs1.toString());

		bs1.or(bs1);
		Assertions.assertEquals(expected.toString(), bs1.toString());
	}

	@Test
	public void xor() {
		BufferBitSet bs1 = new BufferBitSet();
		populateWithSampleIndices(bs1);

		BufferBitSet bs2 = new BufferBitSet();
		populateWithSampleIndices(bs2);
		bs2.set(20000);

		Set<Integer> expected = new TreeSet<>();
		expected.add(20000);

		bs1.xor(bs2);
		Assertions.assertEquals(expected.toString(), bs1.toString());

		bs1.xor(bs1);
		Assertions.assertEquals(0, bs1.length());
	}

	@Test
	public void andNot() {
		BufferBitSet bs1 = new BufferBitSet();
		populateWithSampleIndices(bs1);

		BufferBitSet bs2 = new BufferBitSet();
		bs2.set(20000);

		bs1.andNot(bs2);
		Assertions.assertEquals(SAMPLE_INDICES.toString(), bs1.toString());

		bs2.set(9000);
		bs1.andNot(bs2);
		Set<Integer> expected = new TreeSet<>(SAMPLE_INDICES);
		expected.remove(9000);
		Assertions.assertEquals(expected.toString(), bs1.toString());
	}

	@Test
	public void length() {
		BufferBitSet bs = new BufferBitSet();
		Assertions.assertEquals(0, bs.length());

		bs.set(0);
		Assertions.assertEquals(1, bs.length());

		populateWithSampleIndices(bs);
		Assertions.assertEquals(9001, bs.length());
	}

	@Test
	public void size() {
		BufferBitSet bs = new BufferBitSet();
		populateWithSampleIndices(bs);

		bs = bs.withResizeBehavior(ResizeBehavior.NO_RESIZE);

		bs.set(bs.size() - 1);
		try {
			bs.set(bs.size());
			throw new RuntimeException("Expected IndexOutOfBoundsException");
		} catch (IndexOutOfBoundsException ex) {
			// good
		}
	}

	@Test
	public void isEmpty() {
		BufferBitSet bs = new BufferBitSet();
		populateWithSampleIndices(bs);

		bs.xor(bs);
		Assertions.assertTrue(bs.isEmpty());

		Assertions.assertTrue(new BufferBitSet().isEmpty());
		Assertions.assertTrue(BufferBitSet.valueOf(new byte[0]).isEmpty());
		Assertions.assertTrue(BufferBitSet.valueOf(new byte[] { 0 }).isEmpty());
		Assertions.assertTrue(new BufferBitSet(ByteBuffer.allocate(0)).isEmpty());
		Assertions.assertTrue(new BufferBitSet(ByteBuffer.allocate(1)).isEmpty());
	}

	@Test
	public void cardinality() {
		BufferBitSet bs = new BufferBitSet();
		populateWithSampleIndices(bs);

		Assertions.assertEquals(SAMPLE_INDICES.size(), bs.cardinality());
		Assertions.assertEquals(0, new BufferBitSet().cardinality());
	}

	@Test
	public void hashCodeTest() {
		BufferBitSet bs = new BufferBitSet();
		populateWithSampleIndices(bs);

		int expected = Arrays.hashCode(bs.getBuffer().array());

		Assertions.assertEquals(expected, bs.hashCode());
		Assertions.assertEquals(0, new BufferBitSet().hashCode());
	}

	@Test
	public void equals() {
		Assertions.assertFalse(new BufferBitSet().equals(null));
		Assertions.assertTrue(new BufferBitSet().equals(new BufferBitSet()));

		BufferBitSet bs = new BufferBitSet();
		populateWithSampleIndices(bs);

		Assertions.assertTrue(bs.equals(bs));
		Assertions.assertFalse(new BufferBitSet().equals(bs));
		Assertions.assertFalse(bs.equals(new BufferBitSet()));

		BufferBitSet bs2 = new BufferBitSet();
		populateWithSampleIndices(bs2);
		Assertions.assertTrue(bs.equals(bs2));

		bs2.set(0);
		Assertions.assertFalse(bs.equals(bs2));
		Assertions.assertFalse(bs2.equals(bs));
	}

	@Test
	public void emptyToString() {
		Assertions.assertEquals("[]", new BufferBitSet().toString());
	}

	@Test
	public void shiftRight() {
		BufferBitSet bs = new BufferBitSet();
		populateWithSampleIndices(bs);

		for (int shift = 0; shift <= 100; shift++) {
			final int s = shift;
			Set<Integer> expected = new TreeSet<>(SAMPLE_INDICES.stream().map(i -> i + s).collect(Collectors.toSet()));

			Assertions.assertEquals(expected.toString(), bs.shiftRight(shift).toString());
		}
	}

	@Test
	public void badIndices() {

		BufferBitSet bs = new BufferBitSet();

		try {
			bs.get(-1);
			throw new RuntimeException("Expected IndexOutOfBoundsException");
		} catch (IndexOutOfBoundsException ex) {
			// good
		}

		try {
			bs.set(-1);
			throw new RuntimeException("Expected IndexOutOfBoundsException");
		} catch (IndexOutOfBoundsException ex) {
			// good
		}

		try {
			bs.flip(-1);
			throw new RuntimeException("Expected IndexOutOfBoundsException");
		} catch (IndexOutOfBoundsException ex) {
			// good
		}

		try {
			bs.clear(-1);
			throw new RuntimeException("Expected IndexOutOfBoundsException");
		} catch (IndexOutOfBoundsException ex) {
			// good
		}

		try {
			bs.set(-1, 0);
			throw new RuntimeException("Expected IndexOutOfBoundsException");
		} catch (IndexOutOfBoundsException ex) {
			// good
		}

		try {
			bs.set(0, -1);
			throw new RuntimeException("Expected IndexOutOfBoundsException");
		} catch (IndexOutOfBoundsException ex) {
			// good
		}

		try {
			bs.set(7, 3);
			throw new RuntimeException("Expected IndexOutOfBoundsException");
		} catch (IndexOutOfBoundsException ex) {
			// good
		}

		try {
			bs.clear(-1, 0);
			throw new RuntimeException("Expected IndexOutOfBoundsException");
		} catch (IndexOutOfBoundsException ex) {
			// good
		}

		try {
			bs.clear(0, -1);
			throw new RuntimeException("Expected IndexOutOfBoundsException");
		} catch (IndexOutOfBoundsException ex) {
			// good
		}

		try {
			bs.clear(7, 3);
			throw new RuntimeException("Expected IndexOutOfBoundsException");
		} catch (IndexOutOfBoundsException ex) {
			// good
		}

		try {
			bs.nextSetBit(-1);
			throw new RuntimeException("Expected IndexOutOfBoundsException");
		} catch (IndexOutOfBoundsException ex) {
			// good
		}

		try {
			bs.nextClearBit(-1);
			throw new RuntimeException("Expected IndexOutOfBoundsException");
		} catch (IndexOutOfBoundsException ex) {
			// good
		}

		try {
			bs.previousSetBit(-2);
			throw new RuntimeException("Expected IndexOutOfBoundsException");
		} catch (IndexOutOfBoundsException ex) {
			// good
		}

		try {
			bs.previousClearBit(-2);
			throw new RuntimeException("Expected IndexOutOfBoundsException");
		} catch (IndexOutOfBoundsException ex) {
			// good
		}

		try {
			bs.shiftRight(-1);
			throw new RuntimeException("Expected IllegalArgumentException");
		} catch (IllegalArgumentException ex) {
			// good
		}
	}
}
