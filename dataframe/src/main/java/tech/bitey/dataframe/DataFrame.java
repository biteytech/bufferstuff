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

import static java.util.Spliterator.IMMUTABLE;
import static java.util.Spliterator.NONNULL;
import static java.util.Spliterator.ORDERED;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.RandomAccess;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;

public interface DataFrame extends List<Row>, RandomAccess {

//	static final long MAGIC_NUMBER = ((long) 'd') << 56 | ((long) 'a') << 48 | ((long) 't') << 40 | ((long) 'a') << 32
//			| 'f' << 24 | 'r' << 16 | 'a' << 8 | 'm';

	/*--------------------------------------------------------------------------------
	 *	Object, Collection, and List style Methods
	 *--------------------------------------------------------------------------------*/
	/**
	 * Tests this DataFrame against the specified one for equality. Only compares
	 * the column data if {@code dataOnly} is true, otherwise compares column names
	 * and {@link #keyColumnIndex()} as well.
	 * 
	 * @param df       - the dataframe to compare against
	 * @param dataOnly - only compares column data if true, otherwise compares
	 *                 column names and {@link #keyColumnIndex()} as well
	 * 
	 * @return true if all column comparisons are true, and either {@code dataOnly}
	 *         is true or the column names and {@link #keyColumnIndex()} are the
	 *         same as well.
	 */
	boolean equals(DataFrame df, boolean dataOnly);

	/**
	 * Returns a deep copy of this dataframe. All column data will be copied into
	 * newly allocated buffers.
	 * 
	 * @return a deep copy of this dataframe.
	 */
	DataFrame copy();

	/*--------------------------------------------------------------------------------
	 *	Miscellaneous Methods
	 *--------------------------------------------------------------------------------*/
	@Override
	default Spliterator<Row> spliterator() {
		return Spliterators.spliterator(this, ORDERED | IMMUTABLE | NONNULL);
	}

	/**
	 * Returns a {@link Cursor} at the specified row index.
	 * 
	 * @param rowIndex - the row index
	 * 
	 * @return a {@code Cursor} at the specified row index.
	 * 
	 * @throws IndexOutOfBoundsException if {@code rowIndex} is negative or is not
	 *                                   less than {@link #size()}
	 */
	Cursor cursor(int rowIndex);

	/**
	 * Returns a {@link Cursor} pointing at the first row in this dataframe.
	 * 
	 * @return a {@code Cursor} pointing at the first row in this dataframe.
	 * 
	 * @throws IndexOutOfBoundsException if this dataframe is empty
	 */
	default Cursor cursor() {
		return cursor(0);
	}

	/**
	 * Returns a map keyed by the elements in the key column, with values from the
	 * specified index.
	 * 
	 * @param <K>         - the key type. Must match the key column type.
	 * @param <V>         - the value type. Must match the specified column type.
	 * 
	 * @param columnIndex - index of a column in this dataframe
	 * 
	 * @return a map keyed by the elements in the key column, with values from the
	 *         specified index.
	 * 
	 * @throws IndexOutOfBoundsException if {@code columnIndex} is negative or is
	 *                                   not less than {@link #columnCount()}
	 */
	<K, V> Map<K, V> toMap(int columnIndex);

	/**
	 * Returns a map keyed by the elements in the key column, with values from the
	 * specified index.
	 * 
	 * @param <K>        - the key type. Must match the key column type.
	 * @param <V>        - the value type. Must match the specified column type.
	 * 
	 * @param columnName - name of a column in this dataframe
	 * 
	 * @return a map keyed by the elements in the key column, with values from the
	 *         specified index.
	 * 
	 * @throws IllegalArgumentException if {@code columnName} is not a recognized
	 *                                  column name in this dataframe
	 */
	<K, V> Map<K, V> toMap(String columnName);

	/*--------------------------------------------------------------------------------
	 *	Key Column Methods
	 *--------------------------------------------------------------------------------*/
	/**
	 * Returns true if this dataframe has a key column.
	 * 
	 * @return true if this dataframe has a key column.
	 */
	boolean hasKeyColumn();

	/**
	 * Returns the index of the key column, or null if no key column has been
	 * specified.
	 * 
	 * @return the index of the key column, or null if no key column has been
	 *         specified.
	 */
	Integer keyColumnIndex();

	/**
	 * Returns the key column name, or null if no key column has been specified.
	 * 
	 * @return the key column name, or null if no key column has been specified.
	 */
	String keyColumnName();

	/**
	 * Returns the key {@link ColumnType column type}, or null if no key column has
	 * been specified.
	 * 
	 * @return the key column type, or null if no key column has been specified.
	 */
	ColumnType keyColumnType();

	/**
	 * Returns a new dataframe which is a shallow copy of this one, but with the key
	 * column set to the specified index.
	 * 
	 * @param columnIndex - the key column index
	 * 
	 * @return a new dataframe with the key column set to the specified index.
	 * 
	 * @throws IndexOutOfBoundsException if {@code columnIndex} is negative or is
	 *                                   not less than {@link #columnCount()}
	 */
	DataFrame withKeyColumn(int columnIndex);

	/**
	 * Returns a new dataframe which is a shallow copy of this one, but with the key
	 * column set to the specified index.
	 * 
	 * @param columnName - name of the column in this dataframe
	 * 
	 * @return a new dataframe with the key column set to the specified index.
	 * 
	 * @throws IllegalArgumentException if {@code columnName} is not a recognized
	 *                                  column name in this dataframe
	 */
	DataFrame withKeyColumn(String columnName);

	/*--------------------------------------------------------------------------------
	 *	Column Methods
	 *--------------------------------------------------------------------------------*/
	/**
	 * Returns the number of columns in this dataframe.
	 * 
	 * @return the number of columns in this dataframe.
	 */
	int columnCount();

	/**
	 * Returns the index of the specified column.
	 * 
	 * @param columnName - name of the column in this dataframe
	 * 
	 * @return the index of the specified column.
	 * 
	 * @throws IllegalArgumentException if {@code columnName} is not a recognized
	 *                                  column name in this dataframe
	 */
	int columnIndex(String columnName);

	/**
	 * Returns the column name at the specified index.
	 * 
	 * @param columnIndex - the column index
	 * 
	 * @return the column name at the specified index.
	 * 
	 * @throws IndexOutOfBoundsException if {@code columnIndex} is negative or is
	 *                                   not less than {@link #columnCount()}
	 */
	String columnName(int columnIndex);

	/**
	 * Returns the {@link ColumnType column type} at the specified index.
	 * 
	 * @param columnIndex - the column index
	 * 
	 * @return the column type at the specified index.
	 * 
	 * @throws IndexOutOfBoundsException if {@code columnIndex} is negative or is
	 *                                   not less than {@link #columnCount()}
	 */
	ColumnType columnType(int columnIndex);

	/**
	 * Returns the {@link ColumnType column type} at the specified index.
	 * 
	 * @param columnName - name of the column in this dataframe
	 * 
	 * @return the column type at the specified index.
	 * 
	 * @throws IllegalArgumentException if {@code columnName} is not a recognized
	 *                                  column name in this dataframe
	 */
	ColumnType columnType(String columnName);

	/**
	 * Returns an ordered map where the entries are (name, column) pairs.
	 * 
	 * @return an ordered map where the entries are (name, column) pairs.
	 */
	LinkedHashMap<String, Column<?>> columnMap();

	/**
	 * Returns a list of columns in this dataframe.
	 * 
	 * @return a list of columns in this dataframe.
	 */
	List<Column<?>> columns();

	/**
	 * Returns a list of column names in this dataframe.
	 * 
	 * @return a list of column names in this dataframe.
	 */
	List<String> columnNames();

	/**
	 * Returns a list of column types in this dataframe.
	 * 
	 * @return a list of column types in this dataframe.
	 */
	List<ColumnType> columnTypes();

	DataFrame withColumn(String columnName, Column<?> column);

	DataFrame withColumns(String[] columnNames, Column<?>[] columns);

	DataFrame withColumns(LinkedHashMap<String, Column<?>> columns);

	DataFrame withColumns(DataFrame df);

	DataFrame selectColumns(List<String> columnNames);

	DataFrame selectColumns(String... columnNames);

	DataFrame selectColumns(int... columnIndices);

	DataFrame dropColumns(Collection<String> columnNames);

	DataFrame dropColumns(String... columnNames);

	DataFrame dropColumns(int... columnIndices);

	<T> Column<T> column(int columnIndex);

	StringColumn stringColumn(int columnIndex);

	BooleanColumn booleanColumn(int columnIndex);

	IntColumn intColumn(int columnIndex);

	LongColumn longColumn(int columnIndex);

	DoubleColumn doubleColumn(int columnIndex);

	FloatColumn floatColumn(int columnIndex);

	DateColumn dateColumn(int columnIndex);

	DateTimeColumn dateTimeColumn(int columnIndex);

	<T> Column<T> column(String columnName);

	StringColumn stringColumn(String columnName);

	BooleanColumn booleanColumn(String columnName);

	IntColumn intColumn(String columnName);

	LongColumn longColumn(String columnName);

	DoubleColumn doubleColumn(String columnName);

	FloatColumn floatColumn(String columnName);

	DateColumn dateColumn(String columnName);

	DateTimeColumn dateTimeColumn(String columnName);

	<T> Column<T> deriveColumn(ColumnType type, Function<Row, T> function);

	IntColumn deriveColumn(ToIntFunction<Row> function);

	LongColumn deriveColumn(ToLongFunction<Row> function);

	DoubleColumn deriveColumn(ToDoubleFunction<Row> function);

	FloatColumn deriveColumn(ToFloatFunction<Row> function);

	BooleanColumn deriveColumn(Predicate<Row> function);

	/*--------------------------------------------------------------------------------
	 *	Row Selection Methods
	 *--------------------------------------------------------------------------------*/
	DataFrame sampleN(int size);

	DataFrame sampleX(double proportion);

	DataFrame head(int count);

	default DataFrame empty() {
		return head(0);
	}

	default DataFrame head() {
		return head(10);
	}

	DataFrame tail(int count);

	default DataFrame tail() {
		return tail(10);
	}

	DataFrame subFrame(int fromIndex, int toIndex);

	DataFrame headTo(Object to);

	DataFrame tailFrom(Object from);

	DataFrame subFrameByValue(Object from, Object to);

	DataFrame filter(Predicate<Row> criteria);

	/*--------------------------------------------------------------------------------
	 *	Database-like Methods
	 *--------------------------------------------------------------------------------*/
	DataFrame append(DataFrame df); // union all

	DataFrame append(DataFrame df, boolean coerce);

	DataFrame join(DataFrame df); // one-to-one, inner O(n)

	DataFrame joinSingleIndex(DataFrame df, String columnName); // one-to-many, inner O(n*log(n))

	DataFrame joinSingleIndex(DataFrame df, boolean leftIndex, String nonIndexColumnName); // one-to-many, inner
																							// O(n*log(n))

	DataFrame leftJoinSingleIndex(DataFrame df, String columnName); // one-to-many, left O(n*log(n))

	DataFrame joinHash(DataFrame df, String[] leftColumnNames, String[] rightColumnNames); // one-to-many, inner, O(n),
																							// large overhead

	/*--------------------------------------------------------------------------------
	 *	Cell Accessors
	 *--------------------------------------------------------------------------------*/
	boolean isNull(int rowIndex, int columnIndex);

	boolean isNull(int rowIndex, String columnName);

	<T> T get(int rowIndex, int columnIndex);

	<T> T get(int rowIndex, String columnName);

	String getString(int rowIndex, int columnIndex);

	String getString(int rowIndex, String columnName);

	boolean getBoolean(int rowIndex, int columnIndex);

	boolean getBoolean(int rowIndex, String columnName);

	int getInt(int rowIndex, int columnIndex);

	int getInt(int rowIndex, String columnName);

	default int getOrDefaultInt(int rowIndex, int columnIndex, int defaultValue) {
		return isNull(rowIndex, columnIndex) ? defaultValue : getInt(rowIndex, columnIndex);
	}

	default int getOrDefaultInt(int rowIndex, String columnName, int defaultValue) {
		return isNull(rowIndex, columnName) ? defaultValue : getInt(rowIndex, columnName);
	}

	long getLong(int rowIndex, int columnIndex);

	long getLong(int rowIndex, String columnName);

	default long getOrDefaultLong(int rowIndex, int columnIndex, long defaultValue) {
		return isNull(rowIndex, columnIndex) ? defaultValue : getLong(rowIndex, columnIndex);
	}

	default long getOrDefaultLong(int rowIndex, String columnName, long defaultValue) {
		return isNull(rowIndex, columnName) ? defaultValue : getLong(rowIndex, columnName);
	}

	double getDouble(int rowIndex, int columnIndex);

	double getDouble(int rowIndex, String columnName);

	default double getOrDefaultDouble(int rowIndex, int columnIndex, double defaultValue) {
		return isNull(rowIndex, columnIndex) ? defaultValue : getDouble(rowIndex, columnIndex);
	}

	default double getOrDefaultDouble(int rowIndex, String columnName, double defaultValue) {
		return isNull(rowIndex, columnName) ? defaultValue : getDouble(rowIndex, columnName);
	}

	float getFloat(int rowIndex, int columnIndex);

	float getFloat(int rowIndex, String columnName);

	default float getOrDefaultFloat(int rowIndex, int columnIndex, float defaultValue) {
		return isNull(rowIndex, columnIndex) ? defaultValue : getFloat(rowIndex, columnIndex);
	}

	default float getOrDefaultFloat(int rowIndex, String columnName, float defaultValue) {
		return isNull(rowIndex, columnName) ? defaultValue : getFloat(rowIndex, columnName);
	}

	LocalDate getDate(int rowIndex, int columnIndex);

	LocalDate getDate(int rowIndex, String columnName);

	int yyyymmdd(int rowIndex, int columnIndex);

	int yyyymmdd(int rowIndex, String columnName);

	LocalDateTime getDateTime(int rowIndex, int columnIndex);

	LocalDateTime getDateTime(int rowIndex, String columnName);
}
