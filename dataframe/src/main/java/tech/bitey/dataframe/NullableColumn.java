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

import static tech.bitey.bufferstuff.ResizeBehavior.ALLOCATE_DIRECT;
import static tech.bitey.dataframe.guava.DfPreconditions.checkElementIndex;
import static tech.bitey.dataframe.guava.DfPreconditions.checkPositionIndex;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Comparator;
import java.util.ListIterator;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.SortedSet;

import tech.bitey.bufferstuff.BufferBitSet;

abstract class NullableColumn<E, I extends Column<E>, C extends NonNullColumn<E, I, C>, N extends NullableColumn<E, I, C, N>> extends AbstractColumn<E, I, N> {

	final C column;
	final BufferBitSet nonNulls;
	
	NullableColumn(C column, BufferBitSet nonNulls, int offset, int size) {
		super(offset, size);
		
		this.column = column;
		this.nonNulls = nonNulls;
	}
	
	@Override
	public int characteristics() {
		return BASE_CHARACTERISTICS;
	}
	
	@Override
	ByteOrder byteOrder() {
		return column.byteOrder();
	}
	
	C nonNullSubColumn() {
		return column.subColumn(nonNullIndex(offset), nonNullIndex(offset+size));
	}
	
	@Override
	int byteLength() {
		return 4 + bufferBitSetLength(nonNulls) + nonNullSubColumn().byteLength();
	}

	@Override
	ByteBuffer[] asBuffers() {
		
		ByteBuffer[] columnBuffers = nonNullSubColumn().asBuffers();
		
		ByteBuffer[] buffers = new ByteBuffer[3 + columnBuffers.length];
		
		ByteBuffer bbsLength = buffers[0] = ByteBuffer.allocate(4).order(byteOrder());
		bbsLength.putInt(bufferBitSetLength(nonNulls));
		bbsLength.flip();
		
		ByteBuffer[] nonNullsBuffers = writeBufferBitSet(nonNulls);
		buffers[1] = nonNullsBuffers[0];
		buffers[2] = nonNullsBuffers[1];
		
		System.arraycopy(columnBuffers, 0, buffers, 3, columnBuffers.length);
		
		return buffers;
	}

	@Override
	public N toHeap() {
		@SuppressWarnings("unchecked")
		N cast = (N)this;
		return cast;
	}

	@Override
	public N toSorted() {
		throw new UnsupportedOperationException("columns with null values cannot be sorted");
	}

	@Override
	public N toDistinct() {
		throw new UnsupportedOperationException("columns with null values cannot be sorted");
	}
	
	@Override
	E getNoOffset(int index) {
		if(nonNulls.get(index))
			return column.getNoOffset(nonNullIndex(index));
		else
			return null;
	}
	
	@Override
	public boolean isNullNoOffset(int index) {
		return !nonNulls.get(index);
	}
	
	int nonNullIndex(int index) {
		
		// if value at index is null, jump to next non-null value 
		if(!nonNulls.get(index)) {
			index = nonNulls.nextSetBit(index+1);
			if(index == -1)
				index = offset+size;
		}
		
		// count null bits before index
		int count = 0;
		for(int i = 0; i < index; i++) {
			if(!nonNulls.get(i))
				count++;
		}
		
		return index - count;
	}
	
	private int nullIndex(int index) {
		
		int nullIndex = -1;
		for(int i = 0; i <= index; i++)
			nullIndex = nonNulls.nextSetBit(nullIndex+1);
		
		return nullIndex;
	}
	
	void checkGetPrimitive(int index) {
		checkElementIndex(index, size);
		
		if(isNullNoOffset(index+offset))
			throw new NullPointerException();
	}
	
	private int indexOf0(Object o, boolean first) {
		if(!checkType(o) || isEmpty())
			return -1;
		else if(o == null) {
			if(first) {
				int index = nonNulls.nextClearBit(offset);
				return index > lastIndex() ? -1 : index - offset;
			}
			else {
				int index = nonNulls.previousClearBit(lastIndex());
				return index < offset ? -1 : index - offset;
			}
		}
		
		@SuppressWarnings("unchecked")
		E value = (E)o;
		
		int index = column.search(value, first);
		index = nullIndex(index);
		
		return index < offset || index > lastIndex() ? -1 : index - offset;
	}
	
	@Override
	public int indexOf(Object o) {
		return indexOf0(o, true);
	}
	
	@Override
	public int lastIndexOf(Object o) {
		return indexOf0(o, false);
	}
	
	@Override
	public boolean contains(Object o) {
		return indexOf(o) != -1;
	}
	
	@Override
	public ListIterator<E> listIterator(final int idx) {
		
		checkPositionIndex(idx, size);
		
		return new ImmutableListIterator<E>() {
			
			int index = idx + offset;
			
			final ListIterator<E> iter = column.listIterator(nonNullIndex(index));

			@Override
			public boolean hasNext() {				
				return index <= lastIndex();
			}

			@Override
			public E next() {
				if(!hasNext())
					throw new NoSuchElementException("called next when hasNext is false");

				return nonNulls.get(index++) ? iter.next() : null;
			}

			@Override
			public boolean hasPrevious() {
				return index > offset;
			}

			@Override
			public E previous() {
				if(!hasPrevious())
					throw new NoSuchElementException("called previous when hasPrevious is false");
				
				return nonNulls.get(--index) ? iter.previous() : null;
			}

			@Override
			public int nextIndex() {
				return index-offset;
			}

			@Override
			public int previousIndex() {
				return index-offset-1;
			}
		};
	}


	@Override
	public Comparator<? super E> comparator() {
		return column.comparator();
	}
		
	@Override
	boolean equals0(@SuppressWarnings("rawtypes") NullableColumn rhs) {
		
		// check that values and nulls are in the same place
		int count = 0;
		for(int i = offset, j = rhs.offset; i <= lastIndex(); i++, j++) {
			if(nonNulls.get(i) != rhs.nonNulls.get(j))
				return false;
			count += nonNulls.get(i) ? 1 : 0;
		}
		
		if(count > 0) {
			// compare non-null values
			int lStart = nonNullIndex(offset);
			int rStart = rhs.nonNullIndex(rhs.offset);
			
			@SuppressWarnings("unchecked")
			C cast = (C)rhs.column;
			return column.equals0(cast, lStart, rStart, count);
		}
		else
			return true; // all nulls
	}
	
	@Override
	public int hashCode() {
		if(isEmpty()) return 1;
				
		// hash value positions
		int result = 1;
		for (int i = offset; i <= lastIndex(); i++)
			result = 31 * result + (nonNulls.get(i) ? 1231 : 1237);
		
		// hash actual values
		int fromIndex = nonNullIndex(offset);
		int toIndex = nonNullIndex(lastIndex());
		
		result = 31 * result + column.hashCode(fromIndex, toIndex);
		
		return result;
	}

	abstract N construct(C column, BufferBitSet nonNulls, int size);
	
	@Override
	Column<E> applyFilter0(BufferBitSet keep, int cardinality) {
		
		if(keep.equals(nonNulls))
			return column;
		
		BufferBitSet filteredNonNulls = new BufferBitSet(ALLOCATE_DIRECT);
		BufferBitSet keepNonNulls = new BufferBitSet(ALLOCATE_DIRECT);
		
		int nullCount = 0;
		for(int i = offset, j = 0; i <= lastIndex(); i++) {
			if(keep.get(i-offset)) {
				if(nonNulls.get(i)) {
					filteredNonNulls.set(j);
					keepNonNulls.set(nonNullIndex(i));
				}
				else
					nullCount++;
				j++;
			}
		}
		
		@SuppressWarnings("unchecked")
		C column = (C)this.column.applyFilter(keepNonNulls, keepNonNulls.cardinality());
		
		if(nullCount == 0)
			return column;
		else
			return construct(column, filteredNonNulls, column.size() + nullCount);
	}
	
	@Override
	Column<E> select0(IntColumn indices) {
		
		BufferBitSet decodedNonNulls = new BufferBitSet(ALLOCATE_DIRECT);
		int cardinality = 0;
		for(int i = 0; i < indices.size(); i++) {
			if(nonNulls.get(indices.getInt(i)+offset)) {
				decodedNonNulls.set(i);
				cardinality++;
			}
		}
		
		IntColumnBuilder nonNullIndices = IntColumn.builder().ensureCapacity(cardinality);
		for(int i = 0; i < indices.size(); i++) {
			int index = indices.getInt(i) + offset;
			if(nonNulls.get(index))
				nonNullIndices.add(nonNullIndex(index));
		}
		
		@SuppressWarnings("unchecked")
		C column = (C)this.column.select(nonNullIndices.build());
		
		if(cardinality == indices.size())
			return column;
		else
			return construct(column, decodedNonNulls, indices.size());
	}
		
	@SuppressWarnings("unchecked")
	I prependNonNull(C head) {
		
		BufferBitSet nonNulls = this.nonNulls.get(offset, offset+this.size());
		nonNulls = nonNulls.shiftRight(head.size());
		nonNulls.set(0, head.size());
		
		final C column;
		if(this.column.isEmpty())
			column = head;
		else
			column = head.appendNonNull(this.column);
		
		return (I)construct(column, nonNulls, head.size() + this.size());
	}
	
	@SuppressWarnings("unchecked")
	@Override
	I append0(Column<E> tail) {
		
		final int size = this.size() + tail.size();
		
		if(!tail.isNonnull()) {			
			// append nullable column
			N rhs = (N)tail;
			
			C column = this.column.appendNonNull(rhs.column);
			
			BufferBitSet nonNulls = this.nonNulls.get(offset, offset+this.size());
			BufferBitSet bothNonNulls = rhs.nonNulls.get(rhs.offset, rhs.offset+rhs.size());
			
			bothNonNulls = bothNonNulls.shiftRight(size());
			bothNonNulls.or(nonNulls);
			
			return (I)construct(column, bothNonNulls, size);
		}
		else {
			// append non-null column
			C rhs = (C)tail;
			
			C column = this.column.appendNonNull(rhs);
			
			BufferBitSet nonNulls = this.nonNulls.get(offset, offset+this.size());
			nonNulls.set(this.size(), size);
			
			return (I)construct(column, nonNulls, size);
		}
	}
	
	@Override
	public N copy() {		
		@SuppressWarnings("unchecked")
		C column = (C)nonNullSubColumn().copy();
		
		return construct(column, nonNulls.get(offset, offset+size), size);
	}
	
	@Override
	int intersectBothSorted(N rhs, BufferBitSet keepLeft, BufferBitSet keepRight) {
		throw new UnsupportedOperationException("intersectBothSorted");
	}
	
	@Override
	IntColumn intersectLeftSorted(N rhs, BufferBitSet keepRight) {
		throw new UnsupportedOperationException("intersectLeftSorted");
	}
	
	// does not implement navigableset methods

	@Override
	public E lower(E e) {
		throw new UnsupportedOperationException("lower");
	}

	@Override
	public E floor(E e) {
		throw new UnsupportedOperationException("floor");
	}

	@Override
	public E ceiling(E e) {
		throw new UnsupportedOperationException("ceiling");
	}

	@Override
	public E higher(E e) {
		throw new UnsupportedOperationException("higher");
	}

	@Override
	public NavigableSet<E> subSet(E fromElement, boolean fromInclusive, E toElement, boolean toInclusive) {
		throw new UnsupportedOperationException("subSet");
	}

	@Override
	public NavigableSet<E> headSet(E toElement, boolean inclusive) {
		throw new UnsupportedOperationException("headSet");
	}

	@Override
	public NavigableSet<E> tailSet(E fromElement, boolean inclusive) {
		throw new UnsupportedOperationException("tailSet");
	}

	@Override
	public SortedSet<E> subSet(E fromElement, E toElement) {
		throw new UnsupportedOperationException("subSet");
	}

	@Override
	public SortedSet<E> headSet(E toElement) {
		throw new UnsupportedOperationException("headSet");
	}

	@Override
	public SortedSet<E> tailSet(E fromElement) {
		throw new UnsupportedOperationException("tailSet");
	}
	
	// does not implement subColumn-by-element methods
	
	@Override
	public N subColumn(E fromElement, boolean fromInclusive, E toElement, boolean toInclusive) {
		throw new UnsupportedOperationException("subColumn");
	}
	
	@Override
	public N subColumn(E fromElement, E toElement) {
		throw new UnsupportedOperationException("subColumn");
	}
	
	@Override
	public N head(E toElement, boolean inclusive) {
		throw new UnsupportedOperationException("head");
	}
	
	@Override
	public N head(E toElement) {
		throw new UnsupportedOperationException("head");
	}
	
	@Override
	public N tail(E fromElement, boolean inclusive) {
		throw new UnsupportedOperationException("tail");
	}
	
	@Override
	public N tail(E fromElement) {
		throw new UnsupportedOperationException("tail");
	}
	
	@Override
	public ColumnType getType() {
		return column.getType();
	}
}
