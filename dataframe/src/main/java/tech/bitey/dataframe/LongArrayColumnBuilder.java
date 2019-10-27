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

import static tech.bitey.bufferstuff.BufferUtils.isSortedAndDistinct;
import static tech.bitey.dataframe.guava.DfPreconditions.checkState;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;

abstract class LongArrayColumnBuilder<E, C extends Column<E>, B extends LongArrayColumnBuilder<E, C, B>> extends SingleBufferColumnBuilder<E, LongBuffer, C, B> {

	private final LongArrayPacker<E> packer;
	
	protected LongArrayColumnBuilder(boolean sortedSet, LongArrayPacker<E> packer) {
		super(sortedSet);
		this.packer = packer;
	}

	@Override
	protected void addNonNull(E element) {
		ensureAdditionalCapacity(1);
		elements.put(packer.pack(element));
		size++;
	}

	@Override
	protected void checkSortedAndDistinct() {
		checkState(isSortedAndDistinct(elements, 0, elements.position()),
				"column elements must be sorted and distinct");
	}

	@Override
	LongBuffer asBuffer(ByteBuffer buffer) {
		return buffer.asLongBuffer();
	}

	@Override
	int elementSize() {
		return 8;
	}
}