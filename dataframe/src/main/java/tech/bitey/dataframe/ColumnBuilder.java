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

import static java.util.Spliterator.DISTINCT;
import static java.util.Spliterator.NONNULL;
import static java.util.Spliterator.SORTED;
import static tech.bitey.bufferstuff.ResizeBehavior.ALLOCATE_DIRECT;
import static tech.bitey.dataframe.Column.BASE_CHARACTERISTICS;
import static tech.bitey.dataframe.guava.DfPreconditions.checkArgument;

import java.util.Collection;
import java.util.Iterator;

import tech.bitey.bufferstuff.BufferBitSet;

public abstract class ColumnBuilder<E, C extends Column<E>, B extends ColumnBuilder<E, C, B>> {

	final int characteristics;
	
	BufferBitSet nulls;
	
	int size = 0;
	
	ColumnBuilder(int characteristics) {
		
		characteristics |= BASE_CHARACTERISTICS;
		
		if((characteristics & DISTINCT) != 0)
			characteristics |= SORTED;
		if((characteristics & SORTED) != 0)
			characteristics |= NONNULL;
		
		this.characteristics = characteristics;
	}
	
	public abstract ColumnType getType();
	abstract C empty();
	abstract int getNonNullSize();
	abstract void checkCharacteristics();
	abstract C buildNonNullColumn(int characteristics);
	abstract C wrapNullableColumn(C column, BufferBitSet nonNulls);
	abstract void append0(B tail);	
	
	B append(B tail) {
		checkArgument(this.characteristics == tail.characteristics, "incompatible characteristics");
		
		if(tail.nulls != null) {
			BufferBitSet bothNulls = tail.nulls.shiftRight(this.size);
			if(this.nulls != null)
				bothNulls.or(this.nulls);
			this.nulls = bothNulls;
		}
		
		append0(tail);
		
		this.size += tail.size;
		
		@SuppressWarnings("unchecked")
		B cast = (B)this;
		return cast;
	}
	
	public C build() {
		
		if(size == 0)
			return empty();
		
		final C column;
		
		if(getNonNullSize() == 0) {
			column = empty();
		}
		else {
			if(characteristics != Column.BASE_CHARACTERISTICS)
				checkCharacteristics();			
		
			column = buildNonNullColumn(characteristics | NONNULL);
		}
		
		if(nulls == null)
			return column;
		else {
			BufferBitSet nonNulls = new BufferBitSet(ALLOCATE_DIRECT);
			
			for(int i = 0; i < size; i++)
				if(!nulls.get(i))
					nonNulls.set(i);
			
			C nullable = wrapNullableColumn(column, nonNulls);
			return nullable;
		}
	}
	
	abstract void addNonNull(E element);
	
	public B addNulls(int count) {
		if((characteristics & NONNULL) != 0)
			throw new NullPointerException("cannot add null when NONNULL is set");
		
		if(nulls == null)
			nulls = new BufferBitSet(ALLOCATE_DIRECT);
		
		nulls.set(size, size+=count);
		
		@SuppressWarnings("unchecked")
		B b = (B)this;
		return b; 
	}
	
	public B addNull() {
		return addNulls(1);
	}
	
	public B add(E element) {
		
		if(element == null)
			return addNull();
		
		addNonNull(element);
	
		@SuppressWarnings("unchecked")
		B b = (B)this;
		return b;
	}
	
	@SuppressWarnings("unchecked")
	public B add(E element, E... rest) {
		add(element);
		return addAll(rest);
	}
	
	abstract void ensureAdditionalCapacity(int required);
	
	public B addAll(E[] elements) {
		
		ensureAdditionalCapacity(elements.length);
		
		for(E e : elements)
			add(e);
		
		@SuppressWarnings("unchecked")
		B b = (B)this;
		return b;
	}

	public B addAll(Collection<E> elements) {
		
		ensureAdditionalCapacity(elements.size());
		
		return addAll(elements.iterator());
	}

	public B addAll(Iterator<E> elements) {
		
		while(elements.hasNext())
			add(elements.next());
		
		@SuppressWarnings("unchecked")
		B b = (B)this;
		return b;
	}
	
	public B addAll(Iterable<E> elements) {
		if(elements instanceof Collection)
			return addAll((Collection<E>)elements);
		else
			return addAll(elements.iterator());
	}
	
	public int size() {
		return size;
	}
}
