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
import static tech.bitey.dataframe.guava.DfPreconditions.checkArgument;
import static tech.bitey.dataframe.guava.DfPreconditions.checkPositionIndex;

import java.nio.Buffer;
import java.util.ListIterator;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.RandomAccess;
import java.util.SortedSet;
import java.util.Spliterator;
import java.util.Spliterators;

import org.eclipse.collections.api.list.primitive.MutableIntList;
import org.eclipse.collections.impl.list.mutable.primitive.MutableIntListFactoryImpl;

import tech.bitey.bufferstuff.BufferBitSet;

abstract class NonNullColumn<E, C extends NonNullColumn<E, C>> extends AbstractColumn<E, C> implements RandomAccess
{	
	static final int NONNULL_CHARACTERISTICS = BASE_CHARACTERISTICS | NONNULL;
	
	final int characteristics;
	
	NonNullColumn(int offset, int size, int characteristics) {
		super(offset, size);

		this.characteristics = NONNULL_CHARACTERISTICS | characteristics;
	}
	
	@Override
	public int characteristics() {
		return characteristics;
	}
	
	@Override
	public boolean isNullNoOffset(int index) {
		return false;
	}
	
	abstract int search(E value, boolean first);
	abstract C toHeap0();
	
	@Override
	public C toHeap() {
		if(isSorted())
			return toHeap0();
		else {
			@SuppressWarnings("unchecked")
			C cast = (C)this;
			return cast;
		}
	}
	
	/*
	 * 
	 * @param fromIndex - inclusive
	 * @param toIndex - inclusive
	 * @return
	 */
	abstract int hashCode(int fromIndex, int toIndex);
	
	abstract boolean equals0(C rhs, int lStart, int rStart, int length);
	
	@Override
	boolean equals0(C rhs) {
		return equals0(rhs, offset, rhs.offset, size);
	}
	
	abstract C appendNonNull(C tail);
	
	@SuppressWarnings("unchecked")
	@Override
	Column<E> append0(Column<E> tail) {				
				
		if(!tail.isNonnull()) {
			NullableColumn<E,C,?> rhs = (NullableColumn<E,C,?>)tail;
			C lhs = (C)this;
			return rhs.prependNonNull(lhs);
		}
		else {
			checkArgument(characteristics == tail.characteristics(),
					"both columns must have the same characteristics");
			
			return appendNonNull((C)tail);
		}
	}
	
	abstract int compareValuesAt(C rhs, int l, int r);
	
	@Override
	int intersectBothSorted(C rhs, BufferBitSet keepLeft, BufferBitSet keepRight) {

		int l = 0;
		int r = 0;
		int cardinality = 0;

		OUTER_LOOP: while (l < this.size() && r < rhs.size()) {

			for (int direction; (direction = compareValuesAt(rhs, l, r)) != 0;) {

				if (direction < 0) {
					if (++l == this.size())
						break OUTER_LOOP;
				} else {
					if (++r == rhs.size())
						break OUTER_LOOP;
				}
			}

			keepLeft.set(l++);
			keepRight.set(r++);
			cardinality++;
		}

		return cardinality;
	}
	
	abstract void intersectLeftSorted(C rhs, MutableIntList indices, BufferBitSet keepRight);
	
	@Override
	int[] intersectLeftSorted(C rhs, BufferBitSet keepRight) {		
		MutableIntList indices = MutableIntListFactoryImpl.INSTANCE.empty();
		intersectLeftSorted(rhs, indices, keepRight);		
		return indices.toArray();
	}
	
	@Override
	public int hashCode() {
		if(isEmpty())
			return 1;
		else
			return hashCode(offset, lastIndex());
	}
	
	@Override
	public Spliterator<E> spliterator() {
		return Spliterators.spliterator(this, characteristics);
	}
	
	@Override
	public int indexOf(Object o) {
		if(!checkType(o)) return -1;
		@SuppressWarnings("unchecked")
		int index = search((E)o, true);
		return index < 0 ? -1 : index - offset;
	}
	
	@Override
	public int lastIndexOf(Object o) {
		if(!checkType(o)) return -1;
		@SuppressWarnings("unchecked")
		int index = search((E)o, false);
		return index < 0 ? -1 : index - offset;
	}
	
	int findIndexOrInsertionPoint(E value) {
		int index = search(value, true);
		
		if(index < 0) {
			// index is (-(insertion point) - 1)
			index = -(index + 1);
		}
		
		return index - offset;
	}
	
	@Override
	public boolean contains(Object o) {
		return o == null ? false : indexOf(o) != -1;
	}
	
	/*------------------------------------------------------------
	 *                WcdsImmutableList methods
	 *------------------------------------------------------------*/	
	@Override
	public ListIterator<E> listIterator(final int idx) {
		
		checkPositionIndex(idx, size);
		
		return new ImmutableListIterator<E>() {
			
			int index = idx+offset;

			@Override
			public boolean hasNext() {
				return index <= lastIndex();
			}

			@Override
			public E next() {
				if(!hasNext())
					throw new NoSuchElementException("called next when hasNext is false");
				
				return getNoOffset(index++);
			}

			@Override
			public boolean hasPrevious() {
				return index > offset;
			}

			@Override
			public E previous() {
				if(!hasPrevious())
					throw new NoSuchElementException("called previous when hasPrevious is false");
				
				return getNoOffset(--index);
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
	
	/*------------------------------------------------------------
	 *                ImmutableNavigableSet methods
	 *------------------------------------------------------------*/
	void checkDistinct() {
		if(!isDistinct())
			throw new UnsupportedOperationException("not a unique index");
	}
	
	@Override
	public E lower(E value) {
		if(value == null)
			return null;
		checkDistinct();
		
		int idx = lowerIndex(value);
		return idx == -1 ? null : getNoOffset(idx);
	}

	@Override
	public E higher(E value) {
		if(value == null)
			return null;
		checkDistinct();
		
		int idx = higherIndex(value);
		return idx == -1 ? null : getNoOffset(idx);
	}
	
	@Override
	public E floor(E value) {
		if(value == null)
			return null;
		checkDistinct();
		
		int idx = floorIndex(value);
		return idx == -1 ? null : getNoOffset(idx);
	}
	
	@Override
	public E ceiling(E value) {
		if(value == null)
			return null;
		checkDistinct();
		
		int idx = ceilingIndex(value);
		return idx == -1 ? null : getNoOffset(idx);
	}
	
	int lowerIndex(E value) {
		int idx = search(value, true);

		if (idx < 0)
			idx = -(idx + 1);

		if (idx <= offset)
			return -1;
		else if (idx > lastIndex())
			return lastIndex();
		else
			return idx - 1;
	}

	int higherIndex(E value) {
		int idx = search(value, true);

		if (idx < 0)
			if(isEmpty())
				return -1;
			else
				idx = -(idx + 1) - 1;

		if (idx < offset)
			return offset;
		else if (idx >= lastIndex())
			return -1;
		else
			return idx + 1;
	}

	int floorIndex(E value) {
		int idx = search(value, true);

		if (idx >= 0)
			return idx;
		else
			idx = -(idx + 1);

		if (idx <= offset)
			return -1;
		else if (idx > lastIndex())
			return lastIndex();
		else
			return idx - 1;
	}

	int ceilingIndex(E value) {
		int idx = search(value, true);

		if (idx >= 0)
			return idx;
		else if(isEmpty())
			return -1;
		else
			idx = -(idx + 1) - 1;

		if (idx < offset)
			return offset;
		else if (idx >= lastIndex())
			return -1;
		else
			return idx + 1;
	}
	
	@Override
	public NavigableSet<E> subSet(E fromElement, boolean fromInclusive, E toElement, boolean toInclusive) {
		return subColumn(fromElement, fromInclusive, toElement, toInclusive);
	}
	
	@Override
	public NavigableSet<E> tailSet(E fromElement, boolean inclusive) {		
		return tail(fromElement, inclusive);
	}

	@Override
	public NavigableSet<E> headSet(E toElement, boolean inclusive) {		
		return head(toElement, inclusive);
	}
	
	/*------------------------------------------------------------
	 *                subColumn methods
	 *------------------------------------------------------------*/	
	public C subColumn(E fromElement, boolean fromInclusive, E toElement, boolean toInclusive) {
		checkDistinct();
		
		int direction = comparator().compare(fromElement, toElement);
		
		checkArgument(direction <= 0, "from cannot be greater than to");
		
		if(isEmpty() || (direction == 0 && !(fromInclusive && toInclusive)))
			return empty();
		
		int fk = fromInclusive ? ceilingIndex(fromElement) : higherIndex(fromElement);
		if(fk == -1)
			return empty();
		
		int tk = toInclusive ? floorIndex(toElement) : lowerIndex(toElement);
		if(tk == -1)
			return empty();
				
		return subColumn(fk, tk+1);
	}
	
	public C subColumn(E fromElement, E toElement) {
		return subColumn(fromElement, true, toElement, true);
	}
	
	public C head(E toElement, boolean inclusive) {
		
		if(isEmpty())
			return empty();
		
		final E from = first();
		
		if(comparator().compare(toElement, from) < 0)
			return empty();
		
		return subColumn(from, true, toElement, inclusive);
	}
	
	public C head(E toElement) {
		return head(toElement, true);
	}
	
	public C tail(E fromElement, boolean inclusive) {
		
		if(isEmpty())
			return empty();
		
		final E to = last();
		
		if(comparator().compare(fromElement, to) > 0)
			return empty();
		
		return subColumn(fromElement, inclusive, to, true);
	}
	
	public C tail(E fromElement) {
		return tail(fromElement, true);
	}
	
	
	@Override
	public SortedSet<E> subSet(E fromElement, E toElement) {
		return subSet(fromElement, true, toElement, false);
	}

	@Override
	public SortedSet<E> headSet(E toElement) {
		return headSet(toElement, false);
	}

	@Override
	public SortedSet<E> tailSet(E fromElement) {
		return tailSet(fromElement, true);
	}
	
	void validateBuffer(Buffer buffer) {
		checkArgument(buffer.position() == 0, "buffer position must be zero");
		checkArgument(!buffer.isReadOnly(), "buffer must not be read-only");
	}
}
