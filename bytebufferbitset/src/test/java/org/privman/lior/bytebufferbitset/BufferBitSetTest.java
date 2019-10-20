package org.privman.lior.bytebufferbitset;

import java.util.Set;
import java.util.TreeSet;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class BufferBitSetTest {

	@Test
	public void basicGetAndSet() {
		
		Set<Integer> indices = new TreeSet<>();
		indices.add(1);
		indices.add(10);
		indices.add(39);
		indices.add(100);
		indices.add(9000);
		
		BufferBitSet bs = new BufferBitSet();
		
		for(int index : indices)
			bs.set(index);		
		Assertions.assertEquals(indices.toString(), bs.toString());
		
		for(int i = 0; i < 20000; i++)
			Assertions.assertEquals(indices.contains(i), bs.get(i));
	}
}
