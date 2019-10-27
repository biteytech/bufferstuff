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

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static tech.bitey.bufferstuff.ResizeBehavior.ALLOCATE_DIRECT;
import static tech.bitey.dataframe.guava.DfPreconditions.checkElementIndex;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Comparator;

import org.eclipse.collections.api.list.primitive.MutableIntList;

import tech.bitey.bufferstuff.BufferBitSet;

class NonNullBooleanColumn extends NonNullColumn<Boolean, NonNullBooleanColumn> implements BooleanColumn {

	static final NonNullBooleanColumn EMPTY = new NonNullBooleanColumn(EMPTY_NO_RESIZE, 0, 0);
	
	private final BufferBitSet elements;
	
	NonNullBooleanColumn(BufferBitSet elements, int offset, int size) {
		super(offset, size, false);
		this.elements = elements;
	}

	@Override
	protected NonNullBooleanColumn toHeap0() {
		throw new IllegalStateException();
	}
	
	@Override
	protected Boolean getNoOffset(int index) {
		return elements.get(index) ? TRUE : FALSE;
	}

	@Override
	protected NonNullBooleanColumn subColumn0(int fromIndex, int toIndex) {
		return new NonNullBooleanColumn(elements, fromIndex+offset, toIndex-fromIndex);
	}

	@Override
	protected int search(Boolean value, boolean first) {
		if(sortedSet)
			throw new IllegalStateException();
		else if(first) {
			if(value)
				return elements.nextSetBit(offset);
			else {
				// special case - all bits are considered clear after last index
				int index = elements.nextClearBit(offset);
				return index > lastIndex() ? -1 : lastIndex();
			}
		}
		else {
			if(value)
				return elements.previousSetBit(lastIndex());
			else
				return elements.previousClearBit(lastIndex());
		}
	}

	@Override
	protected NonNullBooleanColumn empty() {
		return EMPTY;
	}

	@Override
	public Comparator<Boolean> comparator() {
		return Boolean::compareTo;
	}

	@Override
	public boolean getBoolean(int index) {
		checkElementIndex(index, size);
		return elements.get(index+offset);
	}
	
	@Override
	public ColumnType getType() {
		return ColumnType.BOOLEAN;
	}

	@Override
	public int hashCode(int fromIndex, int toIndex) {
		// from Arrays::hashCode
		int result = 1;
		for (int i = fromIndex; i <= toIndex; i++)
			result = 31 * result + (elements.get(i) ? 1231 : 1237);
		return result;
	}

	@Override
	protected boolean equals0(NonNullBooleanColumn rhs, int lStart, int rStart, int length) {
		for(int i = 0; i < length; i++)
			if(elements.get(lStart+i) != rhs.elements.get(rStart+i))
				return false;
		return true;
	}

	@Override
	protected NonNullBooleanColumn applyFilter0(BufferBitSet keep, int cardinality) {
		
		BufferBitSet elements = new BufferBitSet(ALLOCATE_DIRECT);
		for(int i = offset, j = 0; i <= lastIndex(); i++) {
			if(keep.get(i - offset)) {
				if(this.elements.get(i))
					elements.set(j);
				j++;
			}
		}
		
		return new NonNullBooleanColumn(elements, 0, cardinality);
	}

	@Override
	protected NonNullBooleanColumn select0(int[] indices) {
		
		BufferBitSet elements = new BufferBitSet(ALLOCATE_DIRECT);
		for(int i = 0; i < indices.length; i++)
			if(this.elements.get(indices[i] + offset))
				elements.set(i);
		
		return new NonNullBooleanColumn(elements, 0, indices.length);
	}

	@Override
	protected NonNullBooleanColumn appendNonNull(NonNullBooleanColumn tail) {
		
		BufferBitSet elements = tail.elements.shiftRight(size());
		elements.or(this.elements);
			
		return new NonNullBooleanColumn(elements, 0, this.size() + tail.size());
	}

	@Override
	protected int intersectBothSorted(NonNullBooleanColumn rhs, BufferBitSet keepLeft, BufferBitSet keepRight) {
		throw new UnsupportedOperationException("intersectBothSorted");
	}

	@Override
	protected void intersectLeftSorted(NonNullBooleanColumn rhs, MutableIntList indices, BufferBitSet keepRight) {
		throw new UnsupportedOperationException("intersectLeftSorted");	
	}

	@Override
	protected int compareValuesAt(NonNullBooleanColumn rhs, int l, int r) {
		throw new UnsupportedOperationException("compareValuesAt");
	}

	@Override
	protected boolean checkType(Object o) {
		return o instanceof Boolean;
	}

	@Override
	ByteOrder byteOrder() {
		return ByteOrder.BIG_ENDIAN;
	}

	@Override
	int byteLength() {
		return bufferBitSetLength(elements);
	}

	@Override
	ByteBuffer[] asBuffers() {
		return writeBufferBitSet(elements);
	}
}