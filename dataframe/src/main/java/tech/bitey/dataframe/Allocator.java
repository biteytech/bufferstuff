package tech.bitey.dataframe;

import static tech.bitey.bufferstuff.ResizeBehavior.ALLOCATE;
import static tech.bitey.bufferstuff.ResizeBehavior.ALLOCATE_DIRECT;
import static tech.bitey.bufferstuff.ResizeBehavior.NO_RESIZE;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import tech.bitey.bufferstuff.BufferBitSet;

enum Allocator {;
	
	static final ByteBuffer EMPTY_BUFFER = ByteBuffer.wrap(new byte[0]).order(ByteOrder.nativeOrder());
	
	static final BufferBitSet EMPTY_BITSET = new BufferBitSet(NO_RESIZE);
	
	private static final boolean DIRECT = "true".equalsIgnoreCase(System.getProperty("tech.bitey.allocateDirect"));
	
	static ByteBuffer allocate(int capacity) {
		ByteBuffer buffer = DIRECT ? ByteBuffer.allocateDirect(capacity) : ByteBuffer.allocate(capacity);
		return buffer.order(ByteOrder.nativeOrder());
	}
	
	static BufferBitSet newBitSet() {
		return new BufferBitSet(DIRECT ? ALLOCATE_DIRECT : ALLOCATE);
	}
	
	static BufferBitSet randomBitSet(int n, int size) {
		return BufferBitSet.random(n, size, DIRECT ? ALLOCATE_DIRECT : ALLOCATE);
	}
}
