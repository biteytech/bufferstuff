/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tech.bitey.dataframe;

import static java.util.Spliterator.NONNULL;
import static tech.bitey.bufferstuff.BufferUtils.duplicate;
import static tech.bitey.bufferstuff.BufferUtils.slice;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public abstract class NonNullSingleBufferColumn<E, C extends NonNullSingleBufferColumn<E, C>> extends NonNullColumn<E, C> {

	final ByteBuffer buffer;
	
	abstract C construct(ByteBuffer buffer, int offset, int size, int characteristics);
	
	NonNullSingleBufferColumn(ByteBuffer buffer, int offset, int size, int characteristics) {
		super(offset, size, characteristics);
		
		validateBuffer(buffer);
		this.buffer = buffer;
	}
	
	abstract int elementSize();
	
	ByteBuffer allocate(int capacity) {
		return ByteBuffer.allocateDirect(capacity * elementSize()).order(ByteOrder.nativeOrder());
	}

	@Override
	C toHeap0() {
		return construct(buffer, offset, size, NONNULL);
	}

	@Override
	C subColumn0(int fromIndex, int toIndex) {
		return construct(buffer, fromIndex+offset, toIndex-fromIndex, characteristics);
	}

	@Override
	C appendNonNull(C tail) {
		
		final int size = size() + tail.size();
		
		ByteBuffer buffer = allocate(size);
						
		buffer.put(duplicate(this.buffer));
		buffer.put(duplicate(tail.buffer));
		
		buffer.flip();
			
		return construct(buffer, 0, size, characteristics);
	}

	@Override
	ByteOrder byteOrder() {
		return buffer.order();
	}

	@Override
	int byteLength() {
		return size() * elementSize();
	}

	@Override
	ByteBuffer[] asBuffers() {
		ByteBuffer buffer = duplicate(this.buffer);
		buffer.limit((offset + size) * elementSize());
		buffer.position(offset * elementSize());
		return new ByteBuffer[] {slice(buffer)};
	}
}
