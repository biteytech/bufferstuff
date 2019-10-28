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
import static java.util.Spliterator.IMMUTABLE;
import static java.util.Spliterator.NONNULL;
import static java.util.Spliterator.ORDERED;
import static java.util.Spliterator.SIZED;
import static java.util.Spliterator.SORTED;
import static java.util.Spliterator.SUBSIZED;

import java.util.List;
import java.util.NavigableSet;
import java.util.Spliterator;
import java.util.Spliterators;

/**
 * An immutable collection backed by a primitive array. Elements of type
 * {@code E} are packed/unpacked to and from the backing array. Columns
 * implement both {@link List} and {@link NavigableSet}. {@code List} is the
 * primary interface, and all {@code List} methods are always available. There
 * are (up to) three concrete implementations of a {@code Column} for each
 * element type, with different tradeoffs between performance and functionality.
 * The methods {@link #isNullable()} and {@link #isSorted()} can be used to
 * determine the characteristics of a particular column:
 * <table border=1 cellpadding=3>
 * <caption><b>Column Implementation Overview</b></caption>
 * <tr>
 * <th>Database<br>
 * Terminology</th>
 * <th>Implementation</th>
 * <th>isNullable</th>
 * <th>isSorted</th>
 * <th>{@link Spliterator}<br>
 * characteristics</th>
 * <th>Get by Index /<br>
 * {@link java.util.RandomAccess RandomAccess}</th>
 * <th>Find by Value/<br>
 * Binary Search</th>
 * </tr>
 * 
 * <tr>
 * <td>Heap, NULL</td>
 * <td>{@link NullableColumn}</td>
 * <td>TRUE</td>
 * <td>FALSE</td>
 * <td>{@link Spliterator#ORDERED ORDERED}, {@link Spliterator#IMMUTABLE
 * IMMUTABLE}</td>
 * <td>O(n) / FALSE</td>
 * <td>O(n) / FALSE</td>
 * </tr>
 * 
 * <tr>
 * <td>Heap, NOT NULL</td>
 * <td>{@link NonNullColumn}<br>
 * characteristics=FALSE</td>
 * <td>FALSE</td>
 * <td>FALSE</td>
 * <td>{@link Spliterator#ORDERED ORDERED}, {@link Spliterator#IMMUTABLE
 * IMMUTABLE},<br>
 * {@link Spliterator#NONNULL NONNULL}</td>
 * <td>O(1) / TRUE</td>
 * <td>O(n) / FALSE</td>
 * </tr>
 * 
 * <tr>
 * <td>Unique Index</td>
 * <td>{@link NonNullColumn}<br>
 * characteristics=TRUE</td>
 * <td>FALSE</td>
 * <td>TRUE</td>
 * <td>{@link Spliterator#ORDERED ORDERED}, {@link Spliterator#IMMUTABLE
 * IMMUTABLE},<br>
 * {@link Spliterator#NONNULL NONNULL},<br>
 * {@link Spliterator#SORTED SORTED}, {@link Spliterator#DISTINCT DISTINCT}</td>
 * <td>O(1) / TRUE</td>
 * <td>O(log(n)) / TRUE</td>
 * </tr>
 * </table>
 * <p>
 * <b>Note:</b> <em>{@code NavigableSet} operations are only available for
 * unique indices</em> (i.e., when {@code isSorted() -> true})! They will
 * throw {@link UnsupportedOperationException} otherwise.
 * <p>
 * All concrete implementations of {@code Column} must be done via
 * {@link AbstractColumn}. It is not enough to simply implement this interface.
 * 
 * @author Lior Privman
 *
 * @param <E> the type of elements in this list
 */
public interface Column<E> extends List<E>, NavigableSet<E> {

	static int BASE_CHARACTERISTICS = SIZED | SUBSIZED | IMMUTABLE | ORDERED;
	
	int characteristics();
	
	default boolean isNonnull() {
		return (characteristics() & NONNULL) != 0;
	}
	default boolean isSorted() {
		return (characteristics() & SORTED) != 0;
	}
	default boolean isDistinct() {
		return (characteristics() & DISTINCT) != 0;
	}

	/**
	 * Converts a unique index into a heap.
	 * 
	 * @return a column equal to this one, but which reports {@link #isSorted} as
	 *         false
	 */
	Column<E> toHeap();

	/**
	 * @return this column's {@link ColumnType type}.
	 */
	ColumnType getType();

	/**
	 * Test if a value is null at a given index.
	 * 
	 * @param index the index to test
	 * 
	 * @return true iff the value at the given index is null
	 */
	boolean isNull(int index);

	/**
	 * Returns a view of the portion of this column between the specified
	 * <tt>fromIndex</tt>, inclusive, and <tt>toIndex</tt>, exclusive. (If
	 * <tt>fromIndex</tt> and <tt>toIndex</tt> are equal, the returned list is
	 * empty.) The returned column is backed by this column.
	 * 
	 * @param fromIndex low endpoint (inclusive) of the subList
	 * @param toIndex   high endpoint (exclusive) of the subList
	 * @return a view of the specified range within this column
	 * @throws IndexOutOfBoundsException for an illegal endpoint index value
	 *                                   (<tt>fromIndex &lt; 0 || toIndex &gt; size ||
	 *         fromIndex &gt; toIndex</tt>)
	 */
	Column<E> subColumn(int fromIndex, int toIndex);

	/**
	 * Returns a view of the portion of this column whose elements range from
	 * {@code fromElement} to {@code toElement}. If {@code fromElement} and
	 * {@code toElement} are equal, the returned column is empty unless {@code
	 * fromInclusive} and {@code toInclusive} are both true. The returned column is
	 * backed by this column.
	 * <p>
	 * <em>This method is only available when {@link #isDistinct()} returns
	 * true.</em>
	 *
	 * @param fromElement   low endpoint of the returned column
	 * @param fromInclusive true if the low endpoint is to be included in the
	 *                      returned view
	 * @param toElement     high endpoint of the returned column
	 * @param toInclusive   true if the high endpoint is to be included in the
	 *                      returned view
	 * @return a view of the portion of this column whose elements range from
	 *         {@code fromElement} to {@code toElement}
	 * @throws UnsupportedOperationException if {@link #isDistinct()} return false
	 * @throws ClassCastException            if {@code fromElement} and
	 *                                       {@code toElement} do not match type
	 *                                       {@code E}
	 * @throws NullPointerException          if {@code fromElement} or
	 *                                       {@code toElement} is null
	 * @throws IllegalArgumentException      if {@code fromElement} is greater than
	 *                                       {@code toElement}
	 */
	Column<E> subColumn(E fromElement, boolean fromInclusive, E toElement, boolean toInclusive);

	/**
	 * Same behavior as {@code Column#subColumn(Object, boolean, Object, Boolean)},
	 * with {@code fromInclusive} set to true and {@code toInclusive} set to false.
	 * <p>
	 * <em>This method is only available when {@link #isDistinct()} returns
	 * true.</em>
	 * 
	 * @param fromElement low endpoint of the returned column, inclusive
	 * @param toElement   high endpoint of the returned column, exclusive
	 * 
	 * @return view of the portion of this column whose elements range from
	 *         {@code fromElement}, inclusive, to {@code toElement}, exclusive
	 */
	Column<E> subColumn(E fromElement, E toElement);

	/**
	 * Returns a view of the portion of this column whose elements are less than (or
	 * equal to, if {@code inclusive} is true) {@code toElement}. The returned
	 * column is backed by this column.
	 * <p>
	 * <em>This method is only available when {@link #isDistinct()} returns
	 * true.</em>
	 *
	 * @param toElement high endpoint of the returned column
	 * @param inclusive {@code true} if the high endpoint is to be included in the
	 *                  returned view
	 * @return a view of the portion of this column whose elements are less than (or
	 *         equal to, if {@code inclusive} is true) {@code toElement}
	 * @throws UnsupportedOperationException if {@link #isDistinct()} return false
	 * @throws ClassCastException            if {@code toElement} is not compatible
	 *                                       with this column's element type.
	 * @throws NullPointerException          if {@code toElement} is null
	 */
	Column<E> head(E toElement, boolean inclusive);

	/**
	 * Same behavior as {@link #head(Object, boolean)}, with {@code inclusive} set
	 * to false.
	 * <p>
	 * <em>This method is only available when {@link #isDistinct()} returns
	 * true.</em>
	 * 
	 * @param toElement high endpoint of the returned column
	 * 
	 * @return a view of the portion of this column whose elements are less than
	 *         {@code toElement}
	 */
	Column<E> head(E toElement);

	/**
	 * Returns a view of the portion of this column whose elements are greater than
	 * (or equal to, if {@code inclusive} is true) {@code fromElement}. The returned
	 * column is backed by this column.
	 * <p>
	 * <em>This method is only available when {@link #isDistinct()} returns
	 * true.</em>
	 *
	 * @param fromElement low endpoint of the returned column
	 * @param inclusive   {@code true} if the low endpoint is to be included in the
	 *                    returned view
	 * @return a view of the portion of this column whose elements are greater than
	 *         or equal to {@code fromElement}
	 * @throws UnsupportedOperationException if {@link #isDistinct()} return false
	 * @throws ClassCastException            if {@code fromElement} is not
	 *                                       compatible with this column's element
	 *                                       type.
	 * @throws NullPointerException          if {@code fromElement} is null
	 */
	Column<E> tail(E fromElement, boolean inclusive);

	/**
	 * Same behavior as {@link #tail(Object, boolean)}, with {@code inclusive} set
	 * to true.
	 * <p>
	 * <em>This method is only available when {@link #isDistinct()} returns
	 * true.</em>
	 * 
	 * @param fromElement low endpoint of the returned column
	 * 
	 * @return a view of the portion of this column whose elements are greater than
	 *         or equal to {@code fromElement}
	 */
	Column<E> tail(E fromElement);

	/**
	 * Creates a {@link Spliterator} over the elements in this list.
	 * <p>
	 * The {@code Spliterator} reports {@link Spliterator#SIZED SIZED},
	 * {@link Spliterator#ORDERED ORDERED}, and {@link Spliterator#IMMUTABLE
	 * IMMUTABLE}.
	 *
	 * @return a {@code Spliterator} over the elements in this column
	 */
	@Override
	default Spliterator<E> spliterator() {
		return Spliterators.spliterator(this, ORDERED | IMMUTABLE);
	}

	/**
	 * Appends two columns with the same element type.
	 * <p>
	 * Both columns must either be unique indices or not. If they're both unique
	 * indices then the first value of the provided column must be greater than the
	 * last value of this column.
	 * 
	 * @param tail - the column to be appended to the end of this column
	 * 
	 * @return the provided column appended to the end of this column
	 */
	Column<E> append(Column<E> tail);

	/**
	 * Appends two columns with the same element type.
	 * <p>
	 * If coerce is true and exactly one of the columns is a unique index, the
	 * unique index will be converted to a heap before appending. Otherwise this
	 * method behaves like {@link #append(Column)}
	 * 
	 * @param tail   - the column to be appended to the end of this column
	 * @param coerce - specifies if the sole unique index should be converted to a
	 *               heap
	 * 
	 * @return the provided column appended to the end of this column
	 */
	default Column<E> append(Column<E> tail, boolean coerce) {
		if (coerce) {
			if (isSorted() && !tail.isSorted())
				return toHeap().append(tail);
			else if (!isSorted() && tail.isSorted())
				return append(tail.toHeap());
		}

		return append(tail);
	}
}
