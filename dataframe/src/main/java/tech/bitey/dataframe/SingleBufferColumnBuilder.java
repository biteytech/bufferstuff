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

import static tech.bitey.bufferstuff.BufferUtils.duplicate;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

abstract class SingleBufferColumnBuilder<E, F extends Buffer, C extends Column<E>, B extends SingleBufferColumnBuilder<E, F, C, B>> extends ColumnBuilder<E, C, B> {

	SingleBufferColumnBuilder(boolean sortedSet) {
		super(sortedSet);
	}

	ByteBuffer buffer = allocate(8);
	F elements = asBuffer(buffer);

	abstract F asBuffer(ByteBuffer buffer);
	abstract C buildNonNullColumn(ByteBuffer trim);
	abstract int elementSize();	
	
	private void resetElementBuffer() {
		ByteBuffer buffer = duplicate(this.buffer);
		buffer.clear();
		elements = asBuffer(buffer);
		elements.position(this.buffer.position() / elementSize());
	}
	
	private ByteBuffer allocate(int capacity) {
		return ByteBuffer.allocateDirect(capacity * elementSize()).order(ByteOrder.nativeOrder());
	}
	
	@Override
	protected void ensureAdditionalCapacity(int required) {
		if(elements.remaining() < required) {
			elements.flip();
			
			int additionalCapacity = elements.limit() >>> 1;
			additionalCapacity = Math.max(additionalCapacity, required);
			
			ByteBuffer extended = allocate(elements.limit() + additionalCapacity);
			buffer.position(elements.limit() * elementSize());
			buffer.flip();			
			extended.put(buffer);
			
			buffer = extended;
			resetElementBuffer();
		}
	}

	@Override
	protected int getNonNullSize() {
		return elements.position();
	}

	@Override
	protected C buildNonNullColumn() {
		ByteBuffer full = duplicate(buffer);
		full.flip();
		full.limit(elements.position() * elementSize());
		
		ByteBuffer trim = allocate(getNonNullSize());
		trim.put(full);
		trim.flip();
		
		return buildNonNullColumn(trim);
	}

	@Override
	protected void append0(B tail) {
		ensureAdditionalCapacity(tail.getNonNullSize());
		
		ByteBuffer tailBuffer = duplicate(tail.buffer);
		tailBuffer.flip();
		buffer.put(tailBuffer);
		resetElementBuffer();
	}
}