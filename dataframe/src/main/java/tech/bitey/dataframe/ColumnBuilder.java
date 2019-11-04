package tech.bitey.dataframe;

import java.util.Collection;
import java.util.Iterator;

/**
 * A builder for creating {@link Column} instances. See concrete implementations
 * for additional details:
 * <ul>
 * <li>{@link BooleanColumnBuilder}
 * <li>{@link DateColumnBuilder}
 * <li>{@link DateTimeColumnBuilder}
 * <li>{@link DoubleColumnBuilder}
 * <li>{@link FloatColumnBuilder}
 * <li>{@link IntColumnBuilder}
 * <li>{@link LongColumnBuilder}
 * <li>{@link StringColumnBuilder}
 * </ul>
 *
 * @author Lior Privman
 *
 * @param <E> the type of elements in this column builder
 */
public interface ColumnBuilder<E> {

	/**
	 * The {@link ColumnType type} of the {@link Column} created by this builder.
	 * 
	 * @return the type of the column created by this builder.
	 */
	ColumnType getType();

	/**
	 * Returns a newly-created {@code Column} based on the contents of this builder.
	 * 
	 * @return a newly-created {@code Column} based on the contents of this builder.
	 */
	Column<E> build();

	/**
	 * Adds a single {@code null} element to the column.
	 * 
	 * @return this builder
	 */
	ColumnBuilder<E> addNull();

	/**
	 * Adds the specified number of nulls to the column.
	 * 
	 * @param count - the number of nulls to add
	 * 
	 * @return this builder
	 * 
	 * @throws IllegalArgumentException if count is negative
	 */
	ColumnBuilder<E> addNulls(int count);

	/**
	 * Adds {@code element} to the column.
	 *
	 * @param element the element to add
	 * 
	 * @return this builder
	 */
	ColumnBuilder<E> add(E element);

	/**
	 * Adds a sequence of {@code elements} to the column.
	 *
	 * @param element an element to add to the column
	 * @param rest    additional elements to add to the column
	 * 
	 * @return this builder
	 */
	@SuppressWarnings("unchecked")
	ColumnBuilder<E> add(E element, E... rest);

	/**
	 * Adds each element of {@code elements} to the column.
	 *
	 * @param elements the elements to be added to the column
	 * 
	 * @return this builder
	 */
	ColumnBuilder<E> addAll(E[] elements);

	/**
	 * Adds each element of {@code elements} to the column.
	 *
	 * @param elements the elements to be added to the column
	 * 
	 * @return this builder
	 */
	ColumnBuilder<E> addAll(Collection<E> elements);

	/**
	 * Adds each element of {@code elements} to the column.
	 *
	 * @param elements the elements to be added to the column
	 * 
	 * @return this builder
	 */
	ColumnBuilder<E> addAll(Iterator<E> elements);

	/**
	 * Adds each element of {@code elements} to the column.
	 *
	 * @param elements the elements to be added to the column
	 * 
	 * @return this builder
	 */
	ColumnBuilder<E> addAll(Iterable<E> elements);

	/**
	 * Increases the capacity of this ColumnBuilder instance, if necessary, to
	 * ensure that it can hold at least the number of elements specified by the
	 * minimum capacity argument.
	 *
	 * @param minCapacity the desired minimum capacity
	 * 
	 * @return this builder
	 */
	ColumnBuilder<E> ensureCapacity(int minCapacity);

	/**
	 * Returns the number of elements added to this builder so far.
	 * 
	 * @return the number of elements added to this builder so far.
	 */
	int size();
}
