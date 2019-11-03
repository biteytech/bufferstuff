package tech.bitey.dataframe;

import java.util.Collection;
import java.util.Iterator;

public interface ColumnBuilder<E> {

	ColumnType getType();
	
	Column<E> build();
	
	ColumnBuilder<E> addNulls(int count);	
	ColumnBuilder<E> addNull();
		
	ColumnBuilder<E> add(E element);	
	@SuppressWarnings("unchecked")
	ColumnBuilder<E> add(E element, E... rest);
	
	ColumnBuilder<E> addAll(E[] elements);
	ColumnBuilder<E> addAll(Collection<E> elements);
	ColumnBuilder<E> addAll(Iterator<E> elements);
	ColumnBuilder<E> addAll(Iterable<E> elements);
	
	ColumnBuilder<E> ensureCapacity(int minCapacity);
	
	int size();
}
