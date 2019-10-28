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
import static tech.bitey.bufferstuff.ResizeBehavior.NO_RESIZE;
import static tech.bitey.dataframe.guava.DfPreconditions.checkArgument;
import static tech.bitey.dataframe.guava.DfPreconditions.checkElementIndex;
import static tech.bitey.dataframe.guava.DfPreconditions.checkPositionIndexes;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.AbstractCollection;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;

import tech.bitey.bufferstuff.BufferBitSet;

abstract class AbstractColumn<E, C extends AbstractColumn<E, C>> extends AbstractCollection<E> implements Column<E> {
	
	static final BufferBitSet EMPTY_NO_RESIZE = new BufferBitSet(NO_RESIZE);
	
	protected final int offset;
	protected final int size;
	
	protected AbstractColumn(int offset, int size) {
		this.offset = offset;
		this.size = size;
	}
	
	private C castThis() {
		@SuppressWarnings("unchecked")
		C cast = (C)this;
		return cast;
	}
	
	@Override
	public C subColumn(int fromIndex, int toIndex) {
		
		checkPositionIndexes(fromIndex, toIndex, size);
		
		final int subSize = toIndex - fromIndex;
		
		if(subSize == 0)
			return empty();
		else if(subSize == size)
			return castThis();
		else
			return subColumn0(fromIndex, toIndex);
	}
	
	protected abstract C empty();
	
	protected abstract C subColumn0(int fromIndex, int toIndex);
	
	protected abstract E getNoOffset(int index);
	protected abstract boolean isNullNoOffset(int index);	
	
	protected abstract boolean checkType(Object o);
	
	abstract ByteOrder byteOrder();
	abstract int byteLength();
	abstract ByteBuffer[] asBuffers();
	
	@Override
	public List<E> subList(int fromIndex, int toIndex) {
		return subColumn(fromIndex, toIndex);
	}
	
	protected abstract Column<E> applyFilter0(BufferBitSet keep, int cardinality);
		
	Column<E> applyFilter(BufferBitSet keep, int cardinality) {
		if(cardinality == 0)
			return empty();
		else if(cardinality == size())
			return this;
		else
			return applyFilter0(keep, cardinality);
	}
	
	protected abstract Column<E> select0(int[] indices);
	
	protected Column<E> select(int[] indices) {
		if(indices.length == 0)
			return empty();
		else
			return select0(indices);
	}
	
	protected abstract Column<E> append0(Column<E> tail);
	
	@Override
	public Column<E> append(Column<E> tail) {		
		checkArgument(getType() == tail.getType(), "columns must have the same type");
		checkArgument(isSorted() == tail.isSorted() && isDistinct() == tail.isDistinct(),
				"both columns must have same sorted & distinct characteristics");		
		
		if(isEmpty())
			return tail;
		else if(tail.isEmpty())
			return this;
		else {			
			if(isDistinct()) {
				checkArgument(
					comparator().compare(last(), tail.first()) < 0,
					"last item of this column must be less than first item of provided column");
			}			
			else if(isSorted()) {
				checkArgument(
					comparator().compare(last(), tail.first()) <= 0,
					"last item of this column must be <= first item of provided column");
			}
			
			return append0(tail);
		}
	}
	
	protected abstract int intersectBothSorted(C rhs, BufferBitSet keepLeft, BufferBitSet keepRight);
	protected abstract int[] intersectLeftSorted(C rhs, BufferBitSet keepRight);
	
	@Override
	public int size() {
		return size;
	}

	@Override
	public E get(int index) {
		checkElementIndex(index, size);
		
		return getNoOffset(index+offset);
	}
	
	@Override
	public boolean isNull(int index) {
		checkElementIndex(index, size);
		
		return isNullNoOffset(index+offset);
	}
	
	protected int lastIndex() {
		return offset + size - 1;
	}
	
	
	
	protected int indexOf(Object o, boolean first) {
		if(first) {
			Iterator<E> iter = iterator();

			for (int i = 0; iter.hasNext(); i++)
				if (Objects.equals(o, iter.next()))
					return i;
		}
		else {
			ListIterator<E> iter = listIterator(size);

			for (int i = size - 1; iter.hasPrevious(); i--)
				if (Objects.equals(o, iter.previous()))
					return i;
		}

		return -1;
	}
	
	@Override
	public int indexOf(Object o) {
		return indexOf(o, true);
	}

	@Override
	public int lastIndexOf(Object o) {
		return indexOf(o, false);
	}

	@Override
	public ListIterator<E> listIterator() {
		return listIterator(0);
	}

	@Override
	public Iterator<E> iterator() {
		return listIterator();
	}
	
	protected abstract boolean equals0(C rhs);
	
	@Override
	public boolean equals(Object o) {
		if (o == this)
            return true;
		
		if(o instanceof Column) {
			
			AbstractColumn<?,?> rhs = (AbstractColumn<?,?>)o;
			if(getType() != rhs.getType() || size != rhs.size || isNonnull() != rhs.isNonnull())
				return false;
			
			@SuppressWarnings("unchecked")
			C cast = (C)o;
			return equals0(cast);
		}
		if(isDistinct() && o instanceof Set) {
			// from AbstractSet

	        Collection<?> c = (Collection<?>) o;
	        if (c.size() != size())
	            return false;
	        try {
	            return containsAll(c);
	        } catch (ClassCastException unused)   {
	            return false;
	        } catch (NullPointerException unused) {
	            return false;
	        }
		}
		else if(o instanceof List) {
			// from AbstractList

	        ListIterator<E> e1 = listIterator();
	        ListIterator<?> e2 = ((List<?>) o).listIterator();
	        while (e1.hasNext() && e2.hasNext()) {
	            E o1 = e1.next();
	            Object o2 = e2.next();
	            if (!(o1==null ? o2==null : o1.equals(o2)))
	                return false;
	        }
	        return !(e1.hasNext() || e2.hasNext());
		}
		else
			return false;
	}

	@Override
	public Object[] toArray() {

		Object[] array = new Object[size()];
		Iterator<?> iter = iterator();

		for (int i = 0; i < array.length; i++)
			array[i] = iter.next();

		return array;
	}

	@Override
	@SuppressWarnings("unchecked")
	public <T> T[] toArray(T[] a) {

		final int size = size();
		final Iterator<E> iter = iterator();

		if (size <= a.length) {
			int i = 0;

			for (; i < size; i++)
				a[i] = (T) iter.next();

			if (i < a.length)
				Arrays.fill(a, i, a.length, null);

			return a;
		} else {
			T[] array = (T[]) Array.newInstance(a.getClass().getComponentType(), size);

			for (int i = 0; i < array.length; i++)
				array[i] = (T) iter.next();

			return array;
		}
	}
	
	// navigableset
	
	@Override
	public E first() {
		if(isEmpty())
			throw new NoSuchElementException();
		
		return get(0);
	}

	@Override
	public E last() {
		if(isEmpty())
			throw new NoSuchElementException();
		
		return get(size()-1);
	}
	
	@Override
	public Iterator<E> descendingIterator() {
		
		return new Iterator<E>() {
			
			int index = lastIndex();

			@Override
			public boolean hasNext() {
				return index >= offset;
			}

			@Override
			public E next() {
				if(!hasNext())
					throw new NoSuchElementException("called next when hasNext is false");
				
				return getNoOffset(index--);
			}			
		};
	}
	
	@Override
	public NavigableSet<E> descendingSet() {
		return new DescendingImmutableNavigableSet<E>(this);
	}
	
	/*------------------------------------------------------------
	 *  Reading & writing BufferBitSet
	 *------------------------------------------------------------*/	
	int bufferBitSetLength(BufferBitSet bbs) {
		
		int fromByte = offset >>> 3;
		int toByte = Math.min(bbs.getBuffer().position(), (lastIndex() >>> 3)+1);
		
		return 5 + toByte - fromByte;
	}
	
	ByteBuffer[] writeBufferBitSet(BufferBitSet bbs) {
		
		ByteBuffer[] buffers = new ByteBuffer[2];
		
		ByteBuffer header = buffers[0] = ByteBuffer.allocate(5).order(byteOrder());
		header.put((byte)(offset & 7));
		header.putInt(size());
		header.flip();
		
		ByteBuffer body = buffers[1] = duplicate(bbs.getBuffer());
		body.limit(Math.min(bbs.getBuffer().position(), (lastIndex() >>> 3)+1));
		body.position(offset >>> 3);
		
		return buffers;
	}
	
	static class BufferBitSetWrapper {
		final int offset;
		final int size;
		final BufferBitSet bbs;
		
		BufferBitSetWrapper(int offset, int size, BufferBitSet bbs) {
			this.offset = offset;
			this.size = size;
			this.bbs = bbs;
		}
	}
	
	static BufferBitSetWrapper readBufferBitSet(ByteBuffer buffer) {
		
		int offset = buffer.get();
		int size = buffer.getInt();
		BufferBitSet bbs = new BufferBitSet(buffer);
		
		if(offset > 0)
			bbs.clear(0, offset);
		if(size < bbs.size())
			bbs.clear(size, bbs.size());
		
		return new BufferBitSetWrapper(offset, size, bbs);
	}
	
	/*------------------------------------------------------------
	 *  Mutating operations throw UnsupportedOperationException
	 *------------------------------------------------------------*/
	@Override
	public E pollFirst() {
		throw new UnsupportedOperationException("pollFirst");
	}

	@Override
	public E pollLast() {
		throw new UnsupportedOperationException("pollLast");
	}

	/*------------------------------------------------------------
	 *  Mutating operations throw UnsupportedOperationException
	 *------------------------------------------------------------*/
	@Override
	public boolean add(E e) {
		throw new UnsupportedOperationException("add");
	}

	@Override
	public boolean remove(Object o) {
		throw new UnsupportedOperationException("remove");
	}

	@Override
	public boolean addAll(Collection<? extends E> c) {
		throw new UnsupportedOperationException("addAll");
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		throw new UnsupportedOperationException("removeAll");
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		throw new UnsupportedOperationException("retainAll");
	}

	@Override
	public void clear() {
		throw new UnsupportedOperationException("clear");
	}
	
	// from list
	@Override
	public boolean addAll(int index, Collection<? extends E> c) {
		throw new UnsupportedOperationException("addAll");
	}

	@Override
	public E set(int index, E element) {
		throw new UnsupportedOperationException("set");
	}

	@Override
	public void add(int index, E element) {
		throw new UnsupportedOperationException("add");
	}

	@Override
	public E remove(int index) {
		throw new UnsupportedOperationException("remove");
	}
}
