package org.privman.lior.bytebufferbitset;

import static org.privman.lior.bytebufferbitset.ResizeBehavior.ALLOCATE;
import static org.privman.lior.bytebufferbitset.ResizeBehavior.ALLOCATE_DIRECT;
import static org.privman.lior.bytebufferbitset.ResizeBehavior.NO_RESIZE;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.TreeSet;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class BufferBitSetTest {
	
	private static final TreeSet<Integer> SAMPLE_INDICES = new TreeSet<>(Arrays.asList(1, 10, 39, 38, 100, 9000));
	
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
		setsAndIfThrows.put(new BufferBitSet(ByteBuffer.allocate(0)), false);
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
	
	@Test
	public void negativeIndices() {
		
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
	}
	
	@Test
	public void emptyToString() {
		Assertions.assertEquals("[]", new BufferBitSet().toString());
	}
}
