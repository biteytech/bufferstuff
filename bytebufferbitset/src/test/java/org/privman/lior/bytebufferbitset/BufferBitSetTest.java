package org.privman.lior.bytebufferbitset;

import static org.privman.lior.bytebufferbitset.ResizeBehavior.ALLOCATE;
import static org.privman.lior.bytebufferbitset.ResizeBehavior.ALLOCATE_DIRECT;
import static org.privman.lior.bytebufferbitset.ResizeBehavior.NO_RESIZE;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.ObjIntConsumer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class BufferBitSetTest {
	
	private static final TreeSet<Integer> SAMPLE_INDICES = new TreeSet<>(Arrays.asList(1, 10, 38, 39, 100, 9000));
	
	private static void populateWithSampleIndices(BufferBitSet bs) {
		for(int index : SAMPLE_INDICES)
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
		setsAndIfThrows.put(BufferBitSet.valueOf(new byte[] {0}), false);
		
		for(Map.Entry<BufferBitSet, Boolean> e : setsAndIfThrows.entrySet()) {			
			try {
				basicGetAndSet(e.getKey());
				if(e.getValue())
					throw new RuntimeException("Expected IndexOutOfBoundsException");
			}
			catch(IndexOutOfBoundsException ex) {
				if(!e.getValue())
					throw ex;
			}
		}
	}
	
	private void basicGetAndSet(BufferBitSet bs) {
		
		populateWithSampleIndices(bs);
		Assertions.assertEquals(SAMPLE_INDICES.toString(), bs.toString());
		
		for(int i = 0; i < 20000; i++)
			Assertions.assertEquals(SAMPLE_INDICES.contains(i), bs.get(i));
	}
	
	@Test
	public void toFromByteArray() {
		
		BufferBitSet bs = new BufferBitSet();
		populateWithSampleIndices(bs);
		
		BufferBitSet rebuilt = BufferBitSet.valueOf(bs.toByteArray());
		Assertions.assertEquals(SAMPLE_INDICES.toString(), rebuilt.toString());
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
		for(int i = fromIndex; i < toIndex; i++)
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
		bs.set(0, toIndex+1);
		bs.clear(fromIndex, toIndex);
		
		Set<Integer> expected = new TreeSet<>();
		for(int i = 0; i < fromIndex; i++)
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
		for(int i = 0; i < 10000; i++)
			if(!SAMPLE_INDICES.contains(i))
				expected.add(i);
		
		Assertions.assertEquals(expected.toString(), bs.toString());
	}
	
	@Test
	public void nextSetBit() {
		BufferBitSet bs = new BufferBitSet();
		populateWithSampleIndices(bs);
		
		Set<Integer> actual = new TreeSet<>();
		for(int bit = bs.nextSetBit(0); bit != -1; bit = bs.nextSetBit(bit+1))
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
		for(int bit = bs.nextClearBit(0); bit <= 9000; bit = bs.nextClearBit(bit+1))
			actual.add(bit);
		
		Assertions.assertEquals(SAMPLE_INDICES, actual);
		
		Assertions.assertEquals(20000, bs.nextClearBit(20000));
	}
	
	@Test
	public void previousSetBit() {
		BufferBitSet bs = new BufferBitSet();
		populateWithSampleIndices(bs);
		
		Set<Integer> actual = new TreeSet<>();
		for(int bit = bs.previousSetBit(10000); bit != -1; bit = bs.previousSetBit(bit-1))
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
		for(int bit = SAMPLE_INDICES.last(); bit != -1; bit = bs.previousClearBit(bit-1))
			actual.add(bit);
		
		Assertions.assertEquals(SAMPLE_INDICES, actual);
		
		Assertions.assertEquals(-1, bs.previousClearBit(0));
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
	public void emptyToString() {
		Assertions.assertEquals("[]", new BufferBitSet().toString());
	}
	
	@Test
	public void badIndices() {
		
		BufferBitSet bs = new BufferBitSet();
		
		try {
			bs.get(-1);
			throw new RuntimeException("Expected IndexOutOfBoundsException");
		}
		catch(IndexOutOfBoundsException ex) {
			// good
		}
		
		try {
			bs.set(-1);
			throw new RuntimeException("Expected IndexOutOfBoundsException");
		}
		catch(IndexOutOfBoundsException ex) {
			// good
		}
		
		try {
			bs.flip(-1);
			throw new RuntimeException("Expected IndexOutOfBoundsException");
		}
		catch(IndexOutOfBoundsException ex) {
			// good
		}
		
		try {
			bs.clear(-1);
			throw new RuntimeException("Expected IndexOutOfBoundsException");
		}
		catch(IndexOutOfBoundsException ex) {
			// good
		}
		
		try {
			bs.set(-1, 0);
			throw new RuntimeException("Expected IndexOutOfBoundsException");
		}
		catch(IndexOutOfBoundsException ex) {
			// good
		}
		
		try {
			bs.set(0, -1);
			throw new RuntimeException("Expected IndexOutOfBoundsException");
		}
		catch(IndexOutOfBoundsException ex) {
			// good
		}
		
		try {
			bs.set(7, 3);
			throw new RuntimeException("Expected IndexOutOfBoundsException");
		}
		catch(IndexOutOfBoundsException ex) {
			// good
		}
		
		try {
			bs.clear(-1, 0);
			throw new RuntimeException("Expected IndexOutOfBoundsException");
		}
		catch(IndexOutOfBoundsException ex) {
			// good
		}
		
		try {
			bs.clear(0, -1);
			throw new RuntimeException("Expected IndexOutOfBoundsException");
		}
		catch(IndexOutOfBoundsException ex) {
			// good
		}
		
		try {
			bs.clear(7, 3);
			throw new RuntimeException("Expected IndexOutOfBoundsException");
		}
		catch(IndexOutOfBoundsException ex) {
			// good
		}
		
		try {
			bs.nextSetBit(-1);
			throw new RuntimeException("Expected IndexOutOfBoundsException");
		}
		catch(IndexOutOfBoundsException ex) {
			// good
		}
		
		try {
			bs.nextClearBit(-1);
			throw new RuntimeException("Expected IndexOutOfBoundsException");
		}
		catch(IndexOutOfBoundsException ex) {
			// good
		}
		
		try {
			bs.previousSetBit(-2);
			throw new RuntimeException("Expected IndexOutOfBoundsException");
		}
		catch(IndexOutOfBoundsException ex) {
			// good
		}
		
		try {
			bs.previousClearBit(-2);
			throw new RuntimeException("Expected IndexOutOfBoundsException");
		}
		catch(IndexOutOfBoundsException ex) {
			// good
		}
	}
}
