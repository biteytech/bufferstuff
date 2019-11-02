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

import static tech.bitey.dataframe.guava.DfPreconditions.checkArgument;
import static tech.bitey.dataframe.guava.DfPreconditions.checkElementIndex;
import static tech.bitey.dataframe.guava.DfPreconditions.checkNotNull;
import static tech.bitey.dataframe.guava.DfPreconditions.checkPositionIndex;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;

import tech.bitey.bufferstuff.BufferBitSet;

public class DataFrameImpl extends AbstractList<Row> implements DataFrame {
	
	private static final DataFramePrinter DEFAULT_PRINTER = new DataFramePrinter(20);
	
	
	/*--------------------------------------------------------------------------------
	 *	Immutable State
	 *--------------------------------------------------------------------------------*/
	private final Integer keyIndex;
	
	private final String[] columnNames;
	private final Column<?>[] columns;	
	private final Map<String, Integer> columnToIndexMap;
	
	
	/*--------------------------------------------------------------------------------
	 *	Constructors
	 *--------------------------------------------------------------------------------*/
	private DataFrameImpl(Column<?>[] columns, String[] columnNames, Integer keyIndex, Map<String, Integer> columnToIndexMap) {
		
		this.columns = columns;
		this.columnNames = columnNames;
		this.keyIndex = keyIndex;
		this.columnToIndexMap = columnToIndexMap;
	}
	
	private DataFrameImpl(Column<?>[] columns, String[] columnNames, Integer keyIndex) {
		
		this.columns = columns;
		this.columnNames = columnNames;
		this.keyIndex = keyIndex;
		
		columnToIndexMap = new HashMap<>();
		for(int i = 0; i < columnNames.length; i++)
			columnToIndexMap.put(columnNames[i], i);
	}
	
	DataFrameImpl(LinkedHashMap<String, Column<?>> columnMap, String keyColumnName) {
		
		checkNotNull(columnMap, "columnMap cannot be null");
		checkArgument(!columnMap.isEmpty(), "columnMap cannot be empty");
		checkArgument(!columnMap.containsKey(null), "column name cannot be null");
		checkArgument(!columnMap.containsValue(null), "column cannot be null");
		
		if(keyColumnName != null) {
			checkArgument(columnMap.containsKey(keyColumnName), "no such column name: "+keyColumnName);
			checkArgument(columnMap.get(keyColumnName).isDistinct(), "key column must be a unique index");
		}
			
		this.columns = new Column[columnMap.size()];
		this.columnNames = new String[columnMap.size()];
		
		columnToIndexMap = new HashMap<>();
		
		int i = 0;
		for(Map.Entry<String, Column<?>> e : columnMap.entrySet()) {
			columns[i] = e.getValue();
			columnNames[i] = e.getKey();
			columnToIndexMap.put(e.getKey(), i);
			i++;
		}
		
		if(keyColumnName != null)
			keyIndex = columnToIndexMap.get(keyColumnName);
		else
			keyIndex = null;
		
		// validate that all columns have the same size
		int size = columns[0].size();
		for(i = 1; i < columns.length; i++)
			checkArgument(columns[i].size() == size, "all columns must have the same size");
	}
	
	
	/*--------------------------------------------------------------------------------
	 *	Precondition Utility Methods
	 *--------------------------------------------------------------------------------*/
	private int checkedColumnIndex(String columnName) {
		checkArgument(columnToIndexMap.containsKey(columnName), "no such column name: "+columnName);
		return columnToIndexMap.get(columnName);
	}
	
	private String checkedColumnName(int columnIndex) {
		checkElementIndex(columnIndex, columnNames.length);
		return columnNames[columnIndex];
	}
	
	private Column<?> checkedColumn(int columnIndex) {
		checkElementIndex(columnIndex, columns.length);
		return columns[columnIndex];
	}
	
	private Column<?> checkedColumn(String columnName) {
		int columnIndex = checkedColumnIndex(columnName);
		return columns[columnIndex];
	}
	
	@SuppressWarnings("rawtypes")
	private NonNullColumn checkedKeyColumn(String operation) {
		if(!hasKeyColumn())
			throw new UnsupportedOperationException(operation+", missing key column");
		
		NonNullColumn keyColumn = (NonNullColumn)columns[keyIndex];
		return keyColumn;
	}
	
	private String[] checkedIndicesToNames(int[] columnIndices) {
		
		String[] columnNames = new String[columnIndices.length];
		for(int i = 0; i < columnNames.length; i++)
			columnNames[i] = checkedColumnName(columnIndices[i]);
		
		return columnNames;
	}
	
	
	/*--------------------------------------------------------------------------------
	 *	Object, Collection, and List Methods
	 *--------------------------------------------------------------------------------*/
	@Override
	public String toString() {
		return DEFAULT_PRINTER.print(this);
	}
	
	@Override
	public int size() {
		return columns[0].size();
	}

	@Override
	public Row get(int rowIndex) {
		checkElementIndex(rowIndex, size());
		return new RowImpl(rowIndex);
	}
	
	@Override
	public boolean equals(DataFrame df, boolean dataOnly) {
		
		DataFrameImpl rhs = (DataFrameImpl)df;
		
		if(!Arrays.equals(columns, rhs.columns))
			return false;
		
		return dataOnly || (Arrays.equals(columnNames, rhs.columnNames)
				&& Objects.equals(keyIndex, rhs.keyIndex));
	}
	
	@Override
	public boolean equals(Object o) {
		if(!(o instanceof DataFrameImpl))
			return false;
		return equals((DataFrame)o, false);
	}
	
	@Override
	public DataFrame copy() {
		
		Column<?>[] columns = Arrays.stream(this.columns)
				.map(Column::copy)
				.toArray(Column<?>[]::new);
		
		return new DataFrameImpl(columns, columnNames, keyIndex);
	}
	
	/*--------------------------------------------------------------------------------
	 *	Miscellaneous Methods
	 *--------------------------------------------------------------------------------*/
	@Override
	public Cursor cursor(int index) {
		checkElementIndex(index, size());
		return new CursorImpl(index);
	}

	@Override
	public <K, V> Map<K, V> toMap(int columnIndex) {
		return toMap(checkedColumn(columnIndex));
	}

	@Override
	public <K, V> Map<K, V> toMap(String columnName) {		
		return toMap(checkedColumn(columnName));
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private <K, V> Map<K, V> toMap(Column<?> valueColumn) {
		
		Column<?> keyColumn = checkedKeyColumn("toMap");
		
		Map map = new ColumnBackedMap<>(keyColumn, valueColumn);
		return map;
	}
	

	/*--------------------------------------------------------------------------------
	 *	Key Column Methods
	 *--------------------------------------------------------------------------------*/
	@Override
	public boolean hasKeyColumn() {
		return keyIndex != null;
	}

	@Override
	public Integer keyColumnIndex() {
		return keyIndex;
	}

	@Override
	public String keyColumnName() {
		return keyIndex == null ? null : columnNames[keyIndex];
	}

	@Override
	public ColumnType keyColumnType() {
		return keyIndex == null ? null : columns[keyIndex].getType();
	}

	@Override
	public DataFrame withKeyColumn(int columnIndex) {
		
		String columnName = checkedColumnName(columnIndex);		
		return withKeyColumn(columnName);
	}

	@Override
	public DataFrame withKeyColumn(String columnName) {
		
		int keyIndex = checkedColumnIndex(columnName);
		
		if(hasKeyColumn() && keyIndex == this.keyIndex)
			return this;
		
		return new DataFrameImpl(columns, columnNames, keyIndex, columnToIndexMap);
	}
	
	
	/*--------------------------------------------------------------------------------
	 *	Column Methods
	 *--------------------------------------------------------------------------------*/
	@Override
	public int columnCount() {
		return columns.length;
	}
	
	@Override
	public int columnIndex(String columnName) {
		return checkedColumnIndex(columnName);
	}
	
	@Override
	public String columnName(int columnIndex) {
		return checkedColumnName(columnIndex);
	}

	@Override
	public ColumnType columnType(int columnIndex) {
		return checkedColumn(columnIndex).getType();
	}

	@Override
	public ColumnType columnType(String columnName) {
		return checkedColumn(columnName).getType();
	}

	@Override
	public LinkedHashMap<String, Column<?>> columnMap() {
		
		LinkedHashMap<String, Column<?>> map = new LinkedHashMap<>();
		
		for(int i = 0; i < columns.length; i++)
			map.put(columnNames[i], columns[i]);
		
		return map;
	}
	
	@Override
	public List<Column<?>> columns() {
		return new ArrayList<>(Arrays.asList(columns));
	}
	
	@Override
	public List<String> columnNames() {
		return new ArrayList<>(Arrays.asList(columnNames));
	}
	
	@Override
	public List<ColumnType> columnTypes() {
		return columns().stream()
			.map(Column::getType)
			.collect(Collectors.toList());
	}

	@Override
	public DataFrame withColumn(String columnName, Column<?> column) {		
		return withColumns(new String[] {columnName}, new Column<?>[] {column});
	}

	@Override
	public DataFrame withColumns(String[] columnNames, Column<?>[] columns) {
		checkArgument(columnNames.length == columns.length, "column arrays must have same length");		
		
		LinkedHashMap<String, Column<?>> columnMap = columnMap();
		
		for(int i = 0; i < columnNames.length; i++)
			columnMap.put(columnNames[i], columns[i]);
		
		return new DataFrameImpl(columnMap, keyColumnName());
	}

	@Override
	public DataFrame withColumns(LinkedHashMap<String, Column<?>> columns) {
		
		LinkedHashMap<String, Column<?>> columnMap = columnMap();
		columnMap.putAll(columns);
		
		return new DataFrameImpl(columnMap, keyColumnName());
	}

	@Override
	public DataFrame withColumns(DataFrame df) {
		return withColumns(df.columnMap());
	}

	@Override
	public DataFrame selectColumns(List<String> columnNames) {
		
		LinkedHashMap<String, Column<?>> columnMap = new LinkedHashMap<>();
		
		for(String columnName : columnNames) {
			Column<?> column = checkedColumn(columnName);
			columnMap.put(columnName, column);
		}
		
		return new DataFrameImpl(columnMap, keyColumnName());
	}

	@Override
	public DataFrame selectColumns(String... columnNames) {
		return selectColumns(Arrays.asList(columnNames));
	}
	
	@Override
	public DataFrame selectColumns(int... columnIndices) {
		return selectColumns(checkedIndicesToNames(columnIndices));
	}
	
	@Override
	public DataFrame dropColumns(Collection<String> columnNames) {
		// check that all column names are valid
		for(String columnName : columnNames)
			checkedColumn(columnName);
		
		Set<String> drop = new HashSet<>(columnNames);
		
		LinkedHashMap<String, Column<?>> columnMap = columnMap();
		for(String columnName : columnNames)
			if(drop.contains(columnName))
				columnMap.remove(columnName);
		
		String keyColumnName = keyColumnName();
		if(drop.contains(keyColumnName))
			keyColumnName = null;
		
		return new DataFrameImpl(columnMap, keyColumnName);		
	}
	
	@Override
	public DataFrame dropColumns(String... columnNames) {
		return dropColumns(Arrays.asList(columnNames));
	}
	
	@Override
	public DataFrame dropColumns(int... columnIndices) {		
		return dropColumns(checkedIndicesToNames(columnIndices));
	}
	
	@Override
	public <T> Column<T> column(int columnIndex) {		
		@SuppressWarnings("unchecked")
		Column<T> cast = (Column<T>)checkedColumn(columnIndex);		
		return cast;
	}

	@Override
	public StringColumn stringColumn(int columnIndex) {
		return (StringColumn)checkedColumn(columnIndex);
	}

	@Override
	public BooleanColumn booleanColumn(int columnIndex) {
		return (BooleanColumn)checkedColumn(columnIndex);
	}

	@Override
	public IntColumn intColumn(int columnIndex) {
		return (IntColumn)checkedColumn(columnIndex);
	}

	@Override
	public LongColumn longColumn(int columnIndex) {
		return (LongColumn)checkedColumn(columnIndex);
	}

	@Override
	public DoubleColumn doubleColumn(int columnIndex) {
		return (DoubleColumn)checkedColumn(columnIndex);
	}

	@Override
	public FloatColumn floatColumn(int columnIndex) {
		return (FloatColumn)checkedColumn(columnIndex);
	}

	@Override
	public DateColumn dateColumn(int columnIndex) {
		return (DateColumn)checkedColumn(columnIndex); 
	}

	@Override
	public DateTimeColumn dateTimeColumn(int columnIndex) {
		return (DateTimeColumn)checkedColumn(columnIndex); 
	}

	@Override
	public <T> Column<T> column(String columnName) {
		@SuppressWarnings("unchecked")
		Column<T> cast = (Column<T>)checkedColumn(columnName);		
		return cast;
	}

	@Override
	public StringColumn stringColumn(String columnName) {
		return (StringColumn)checkedColumn(columnName);
	}

	@Override
	public BooleanColumn booleanColumn(String columnName) {
		return (BooleanColumn)checkedColumn(columnName);
	}

	@Override
	public IntColumn intColumn(String columnName) {
		return (IntColumn)checkedColumn(columnName);
	}

	@Override
	public LongColumn longColumn(String columnName) {
		return (LongColumn)checkedColumn(columnName);
	}

	@Override
	public DoubleColumn doubleColumn(String columnName) {
		return (DoubleColumn)checkedColumn(columnName);
	}

	@Override
	public FloatColumn floatColumn(String columnName) {
		return (FloatColumn)checkedColumn(columnName);
	}

	@Override
	public DateColumn dateColumn(String columnName) {
		return (DateColumn)checkedColumn(columnName);
	}

	@Override
	public DateTimeColumn dateTimeColumn(String columnName) {
		return (DateTimeColumn)checkedColumn(columnName);
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public <T> Column<T> deriveColumn(ColumnType type, Function<Row, T> function) {
		
		@SuppressWarnings("rawtypes")
		ColumnBuilder builder = type.builder(0);
		
		for(Cursor cursor = cursor(); cursor.hasNext(); cursor.next())
			builder.add(function.apply(cursor));
		
		return builder.build();
	}

	@Override
	public IntColumn deriveColumn(ToIntFunction<Row> function) {
		
		IntColumnBuilder builder = IntColumn.builder();
		
		for(Cursor cursor = cursor(); cursor.hasNext(); cursor.next())
			builder.add(function.applyAsInt(cursor));
		
		return builder.build();
	}

	@Override
	public LongColumn deriveColumn(ToLongFunction<Row> function) {
		
		LongColumnBuilder builder = LongColumn.builder();
		
		for(Cursor cursor = cursor(); cursor.hasNext(); cursor.next())
			builder.add(function.applyAsLong(cursor));
		
		return builder.build();
	}

	@Override
	public DoubleColumn deriveColumn(ToDoubleFunction<Row> function) {
		
		DoubleColumnBuilder builder = DoubleColumn.builder();
		
		for(Cursor cursor = cursor(); cursor.hasNext(); cursor.next())
			builder.add(function.applyAsDouble(cursor));
		
		return builder.build();
	}

	@Override
	public FloatColumn deriveColumn(ToFloatFunction<Row> function) {
		
		FloatColumnBuilder builder = FloatColumn.builder();
		
		for(Cursor cursor = cursor(); cursor.hasNext(); cursor.next())
			builder.add(function.applyAsFloat(cursor));
		
		return builder.build();
	}

	@Override
	public BooleanColumn deriveColumn(Predicate<Row> function) {
		
		BooleanColumnBuilder builder = BooleanColumn.builder();
		
		for(Cursor cursor = cursor(); cursor.hasNext(); cursor.next())
			builder.add(function.test(cursor));
		
		return builder.build();
	}
	
	
	/*--------------------------------------------------------------------------------
	 *	Row Selection Methods
	 *--------------------------------------------------------------------------------*/
	@Override
	public DataFrame sampleN(int size) {
		checkPositionIndex(size, size());
		
		if(isEmpty())
			return this;
		else if(size == 0)
			return empty();
		else if(size == size())
			return this;
		
		int[] indices = new int[size()];
		for(int i = 0; i < indices.length; i++)
			indices[i] = i;
		
		ArrayUtils.shuffle(indices);
		
		BufferBitSet keep = Allocator.newBitSet();
		for(int i = 0; i < size; i++)
			keep.set(indices[i]);
		
		return filter(keep);
	}

	@Override
	public DataFrame sampleX(double proportion) {
		checkArgument(proportion >= 0d && proportion <= 1d, "proportion must be between 0 and 1 inclusive");
		return sampleN((int)Math.round(size() * proportion));
	}
	
	@Override
	public DataFrame head(int count) {
		count = Math.min(size(), count);
		return subFrame(0, count);
	}

	@Override
	public DataFrame tail(int count) {
		count = Math.min(size(), count);
		return subFrame(size()-count, size());
	}

	@Override
	public DataFrame subFrame(int fromIndex, int toIndex) {
		return modifyColumns(column -> column.subColumn(fromIndex, toIndex));
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public DataFrame headTo(Object to) {
		NonNullColumn keyColumn = checkedKeyColumn("headTo");
		
		int toIndex = keyColumn.findIndexOrInsertionPoint(to);
		return subFrame(0, toIndex);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public DataFrame tailFrom(Object from) {
		NonNullColumn keyColumn = checkedKeyColumn("tailFrom");
		
		int fromIndex = keyColumn.findIndexOrInsertionPoint(from);
		return subFrame(fromIndex, size());
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public DataFrame subFrameByValue(Object from, Object to) {
		NonNullColumn keyColumn = checkedKeyColumn("tailFrom");
		
		int fromIndex = keyColumn.findIndexOrInsertionPoint(from);
		int toIndex = keyColumn.findIndexOrInsertionPoint(to);
		
		return subFrame(fromIndex, toIndex);
	}
	
	@Override
	public DataFrame filter(Predicate<Row> criteria) {
		
		BufferBitSet keep = Allocator.newBitSet();
		
		int i = 0;
		for(Cursor cursor = cursor(); cursor.hasNext(); cursor.next(), i++)
			if(criteria.test(cursor))
				keep.set(i);
		
		return filter(keep);
	}
	
	
	/*--------------------------------------------------------------------------------
	 *	Database-like Methods
	 *--------------------------------------------------------------------------------*/
	@Override
	public DataFrame append(DataFrame df) {
		return append(df, false);
	}
	
	@Override
	public DataFrame append(DataFrame df, boolean coerce) {
		
		checkArgument(columnCount() == df.columnCount(), "mismatched column counts");		
		
		Column<?>[] columns = new Column[columnCount()];		
		for(int i = 0; i < columnCount(); i++) {
			checkArgument(columnType(i) == df.columnType(i), "mismatched column types");
			
			columns[i] = this.columns[i].append(df.column(i), coerce);
		}
		
		Integer keyIndex = this.keyIndex;
		if(coerce && keyIndex != null && !columns[keyIndex].isDistinct())
			keyIndex = null;
		
		return new DataFrameImpl(columns, columnNames, keyIndex, columnToIndexMap);
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public DataFrame join(DataFrame df) {		
		checkArgument(hasKeyColumn() && df.hasKeyColumn(), "both dataframes must have a key column");
		checkArgument(columnType(keyColumnIndex()) == df.columnType(df.keyColumnIndex()),
				"key columns must be of the same type");
		
		DataFrameImpl rhs = (DataFrameImpl)df;		
		AbstractColumn leftKey = (AbstractColumn)columns[keyIndex];
		AbstractColumn rightKey = (AbstractColumn)rhs.columns[rhs.keyIndex];
		
		BufferBitSet keepLeft = Allocator.newBitSet();
		BufferBitSet keepRight = Allocator.newBitSet();
		
		int cardinality = leftKey.intersectBothSorted(rightKey, keepLeft, keepRight);
		
		String[] columnNames = jointColumnNames(rhs, rhs.keyIndex);
		
		Column<?>[] columns = new Column<?>[columnCount() + rhs.columnCount() - 1];
		for(int i = 0; i < columnCount(); i++)
			columns[i] = ((AbstractColumn)this.columns[i]).applyFilter(keepLeft, cardinality);
		for(int i = 0, j = 0; i < rhs.columnCount(); i++) {
			if(i != rhs.keyIndex)
				columns[j++ + columnCount()] = ((AbstractColumn)rhs.columns[i]).applyFilter(keepRight, cardinality);
		}
		
		return new DataFrameImpl(columns, columnNames, keyIndex);
	}
	
	private static String nextColumnName(Set<String> names, String name) {		
		if(!names.contains(name))
			return name;
		
		for(int i = 2; ; i++) {
			String nameWithSuffix = name+"_"+i;
			if(!names.contains(nameWithSuffix))
				return nameWithSuffix;
		}
	}
	
	private String[] jointColumnNames(DataFrame rhs, int rightColumnIndex) {
		return jointColumnNames(rhs, new int[] {rightColumnIndex});
	}
	
	private String[] jointColumnNames(DataFrame rhs, int[] rightColumnIndices) {
		
		Set<String> names = new HashSet<>(Arrays.asList(this.columnNames));
		
		String[] columnNames = Arrays.copyOf(this.columnNames,
				columnCount() + rhs.columnCount() - rightColumnIndices.length);
		
		for(int i = 0, j = columnCount(), k = 0; i < rhs.columnCount(); i++) {
			if(i != rightColumnIndices[k])
				columnNames[j++] = nextColumnName(names, rhs.columnName(i));
			else {
				k++;
				if(k == rightColumnIndices.length)
					k = 0;
			}
		}
		
		return columnNames;
	}

	@Override
	public DataFrame joinSingleIndex(DataFrame df, String columnName) {
		return joinSingleIndex(df, true, columnName);
	}
	
	@Override
	public DataFrame joinSingleIndex(DataFrame df, boolean leftIndex, String nonIndexColumnName) {
		if(leftIndex)
			return (DataFrame)joinSingleIndex0(df, nonIndexColumnName)[0];
		else {
			DataFrameImpl backasswards = (DataFrameImpl)((DataFrameImpl)df).joinSingleIndex0(this, nonIndexColumnName)[0];
			
			Column<?>[] columns = new Column<?>[backasswards.columnCount()];
			System.arraycopy(backasswards.columns, df.columnCount(), columns, 0, this.columnCount()-1);
			System.arraycopy(backasswards.columns, 0, columns, this.columnCount()-1, df.columnCount());
			
			int idx = this.columnCount()-1+df.keyColumnIndex();
			Column<?> indexColumn = columns[idx];
			
			List<Column<?>> columnList = new ArrayList<>(Arrays.asList(columns));
			columnList.remove(idx);
			columnList.add(columnIndex(nonIndexColumnName), indexColumn);
			
			String[] columnNames = jointColumnNames(df, df.keyColumnIndex());
			
			return new DataFrameImpl(columnList.toArray(new Column<?>[0]), columnNames, null);
		}
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	// returns pair<DataFrame, int[] indices>
	private Object[] joinSingleIndex0(DataFrame df, String columnName) {
		
		checkArgument(hasKeyColumn(), "missing key column");
		
		DataFrameImpl rhs = (DataFrameImpl)df;		
		AbstractColumn leftKey = (AbstractColumn)columns[keyIndex];
		AbstractColumn rightColumn = (AbstractColumn)rhs.column(columnName);
		int rightColumnIndex = rhs.columnToIndexMap.get(columnName);
		
		checkArgument(leftKey.getType() == rightColumn.getType(), "columns being joined on must have the same type");
		
		if(!rightColumn.isNonnull()) {
			// remove nulls
			rhs = rhs.filter(((NullableColumn)rightColumn).nonNulls);
			rightColumn = (AbstractColumn)rhs.column(columnName);
		}
		
		BufferBitSet keepRight = Allocator.newBitSet();
		IntColumn indices = leftKey.intersectLeftSorted(rightColumn, keepRight);
		
		rhs = rhs.filter(keepRight);
		
		String[] columnNames = jointColumnNames(rhs, rightColumnIndex);
		
		Column<?>[] columns = new Column<?>[columnCount() + rhs.columnCount() - 1];
		for(int i = 0; i < columnCount(); i++)
			columns[i] = ((AbstractColumn)this.columns[i]).select(indices);
		for(int i = 0, j = 0; i < rhs.columnCount(); i++) {
			if(i != rightColumnIndex)
				columns[j++ + columnCount()] = rhs.columns[i];
		}
		
		Object[] pair = new Object[2];
		pair[0] = new DataFrameImpl(columns, columnNames, null);
		pair[1] = indices;
		return pair;
	}

	@Override
	public DataFrame leftJoinSingleIndex(DataFrame df, String columnName) {
		
		Object[] pair = joinSingleIndex0(df, columnName);
		DataFrameImpl inner = (DataFrameImpl)pair[0];
		IntColumn indices = (IntColumn)pair[1];
		
		BufferBitSet unmatchedLeft = Allocator.newBitSet();
		unmatchedLeft.set(0, this.size());
		for(int i = 0; i < indices.size(); i++)
			unmatchedLeft.set(indices.getInt(i), false);
		
		if(unmatchedLeft.cardinality() == 0)
			return inner;
		
		DataFrameImpl left = this.filter(unmatchedLeft);
		DataFrameImpl rhs = (DataFrameImpl)df;
		
		Column<?>[] columns = new Column<?>[columnCount() + df.columnCount() - 1];
		for(int i = 0; i < columnCount(); i++)
			columns[i] = left.columns[i];
		int rightColumnIndex = rhs.columnToIndexMap.get(columnName);
		for(int i = 0, j = 0; i < rhs.columnCount(); i++) {
			if(i != rightColumnIndex)
				columns[j++ + columnCount()] = rhs.columns[i].getType().nullColumn(left.size());
		}
		
		left = new DataFrameImpl(columns, inner.columnNames, null);
		
		return inner.append(left, true);
	}

	@Override
	public DataFrame joinHash(DataFrame df, String[] leftColumnNames, String[] rightColumnNames) {
		
		DataFrameImpl rhs = (DataFrameImpl)df;
		
		checkArgument(leftColumnNames.length == rightColumnNames.length,
				"left and right column name arrays must have the same length");
		checkArgument(leftColumnNames.length > 0,
				"column name arrays cannot be empty");
		
		int[] leftColumnIndices = new int[leftColumnNames.length];
		for(int i = 0; i < leftColumnNames.length; i++)
			leftColumnIndices[i] = checkedColumnIndex(leftColumnNames[i]);
		
		int[] rightColumnIndices = new int[rightColumnNames.length];
		for(int i = 0; i < rightColumnNames.length; i++)
			rightColumnIndices[i] = checkedColumnIndex(rightColumnNames[i]);
		
		for(int i = 0; i < leftColumnIndices.length; i++)
			checkArgument(columns[leftColumnIndices[i]].getType() == rhs.columns[rightColumnIndices[i]].getType(),
				"mismatched key column types");

		IntColumn indices;
		BufferBitSet keepRight = Allocator.newBitSet();
		
		{
			class SimpleTuple {
				final Object[] elements;
				
				SimpleTuple(Row row, int[] columnIndices) {					
					elements = new Object[columnIndices.length];
					for(int i = 0; i < columnIndices.length; i++)
						elements[i] = row.get(columnIndices[i]);					
				}
				
				@Override
				public int hashCode() {
					int result = 1;

			        for (Object element : elements)
			            result = 31 * result + (element == null ? 0 : element.hashCode());

			        return result;
				}
				
				@Override
				public boolean equals(Object o) {
					SimpleTuple rhs = (SimpleTuple) o;

					for (int i = 0; i < elements.length; i++) {
						Object o1 = elements[i];
						Object o2 = rhs.elements[i];
						if (!(o1 == null ? o2 == null : o1.equals(o2)))
							return false;
					}
					return true;
				}
			}
			
			Map<SimpleTuple, Integer> hashMap = new HashMap<>();
		
			for(Cursor cursor = cursor(); cursor.hasNext(); cursor.next()) {
				SimpleTuple tuple = new SimpleTuple(cursor, leftColumnIndices);
				if(hashMap.containsKey(tuple))
					throw new IllegalStateException("columns do not form a unqiue index");
				else
					hashMap.put(tuple, cursor.currentIndex());
			}
			
			IntColumnBuilder builder = IntColumn.builder();
			
			for(Cursor cursor = rhs.cursor(); cursor.hasNext(); cursor.next()) {
				SimpleTuple tuple = new SimpleTuple(cursor, rightColumnIndices);
				int leftRowIndex = hashMap.getOrDefault(tuple, -1);
				if(leftRowIndex >= 0) {
					builder.add(leftRowIndex);
					keepRight.set(cursor.currentIndex());
				}
			}
			
			hashMap = null;
			indices = builder.build();
			builder = null;
		}
		
		DataFrameImpl left = select(indices);
		DataFrameImpl right = rhs.filter(keepRight);
		
		String[] columnNames = jointColumnNames(right, rightColumnIndices);
		
		Column<?>[] columns = Arrays.copyOf(left.columns, columnNames.length);
		for(int i = 0, j = columnCount(), k = 0; i < right.columnCount(); i++) {
			if(i != rightColumnIndices[k])
				columns[j++] = right.columns[i];
			else {
				k++;
				if(k == rightColumnIndices.length)
					k = 0;
			}
		}
		
		return new DataFrameImpl(columns, columnNames, null);
	}

	/*--------------------------------------------------------------------------------
	 *	Cell Accessors
	 *--------------------------------------------------------------------------------*/
	@Override
	public boolean isNull(int rowIndex, int columnIndex) {
		Column<?> column = checkedColumn(columnIndex);
		return column.isNull(rowIndex);
	}

	@Override
	public boolean isNull(int rowIndex, String columnName) {
		Column<?> column = checkedColumn(columnName);
		return column.isNull(rowIndex);
	}

	@Override
	public <T> T get(int rowIndex, int columnIndex) {
		Column<?> column = checkedColumn(columnIndex);
		@SuppressWarnings("unchecked")
		T value = (T)column.get(rowIndex);
		return value;
	}

	@Override
	public <T> T get(int rowIndex, String columnName) {
		Column<?> column = checkedColumn(columnName);
		@SuppressWarnings("unchecked")
		T value = (T)column.get(rowIndex);
		return value;
	}

	@Override
	public String getString(int rowIndex, int columnIndex) {
		return stringColumn(columnIndex).get(rowIndex);
	}

	@Override
	public String getString(int rowIndex, String columnName) {
		return stringColumn(columnName).get(rowIndex);
	}

	@Override
	public boolean getBoolean(int rowIndex, int columnIndex) {
		return booleanColumn(columnIndex).getBoolean(rowIndex);
	}

	@Override
	public boolean getBoolean(int rowIndex, String columnName) {
		return booleanColumn(columnName).getBoolean(rowIndex);
	}

	@Override
	public int getInt(int rowIndex, int columnIndex) {
		return intColumn(columnIndex).getInt(rowIndex);
	}

	@Override
	public int getInt(int rowIndex, String columnName) {
		return intColumn(columnName).getInt(rowIndex);
	}

	@Override
	public long getLong(int rowIndex, int columnIndex) {
		return longColumn(columnIndex).getLong(rowIndex);
	}

	@Override
	public long getLong(int rowIndex, String columnName) {
		return longColumn(columnName).getLong(rowIndex);
	}

	@Override
	public double getDouble(int rowIndex, int columnIndex) {
		return doubleColumn(columnIndex).getDouble(rowIndex);
	}

	@Override
	public double getDouble(int rowIndex, String columnName) {
		return doubleColumn(columnName).getDouble(rowIndex);
	}

	@Override
	public float getFloat(int rowIndex, int columnIndex) {
		return floatColumn(columnIndex).getFloat(rowIndex);
	}

	@Override
	public float getFloat(int rowIndex, String columnName) {
		return floatColumn(columnName).getFloat(rowIndex);
	}

	@Override
	public LocalDate getDate(int rowIndex, int columnIndex) {
		return dateColumn(columnIndex).get(rowIndex);
	}

	@Override
	public LocalDate getDate(int rowIndex, String columnName) {
		return dateColumn(columnName).get(rowIndex);
	}

	@Override
	public int yyyymmdd(int rowIndex, int columnIndex) {
		return dateColumn(columnIndex).yyyymmdd(rowIndex);
	}

	@Override
	public int yyyymmdd(int rowIndex, String columnName) {
		return dateColumn(columnName).yyyymmdd(rowIndex);
	}

	@Override
	public LocalDateTime getDateTime(int rowIndex, int columnIndex) {
		return dateTimeColumn(columnIndex).get(rowIndex);
	}

	@Override
	public LocalDateTime getDateTime(int rowIndex, String columnName) {
		return dateTimeColumn(columnName).get(rowIndex);
	}
	
	
	/*--------------------------------------------------------------------------------
	 *	Methods which modify all columns
	 *--------------------------------------------------------------------------------*/
	@SuppressWarnings("rawtypes")
	private DataFrameImpl modifyColumns(Function<AbstractColumn, Column> transformation) {
		
		Column<?>[] columns = new Column[columnCount()];
		
		for(int i = 0; i < columns.length; i++) {
			AbstractColumn column = (AbstractColumn)this.columns[i];
			columns[i] = transformation.apply(column);
		}
			
		return new DataFrameImpl(columns, columnNames, keyIndex, columnToIndexMap);
	}
	
	private DataFrameImpl filter(BufferBitSet keep) {
		
		final int cardinality = keep.cardinality();
		if(cardinality == 0)
			return (DataFrameImpl)empty();
		else if(cardinality == size())
			return this;
		
		return modifyColumns(column -> column.applyFilter(keep, cardinality));
	}
	
	private DataFrameImpl select(IntColumn indices) {
		
		if(indices.size() == 0)
			return (DataFrameImpl)empty();
		
		return modifyColumns(column -> column.select(indices));
	}
	
	
	/*--------------------------------------------------------------------------------
	 *	Row Classes
	 *--------------------------------------------------------------------------------*/
	private abstract class AbstractRow implements Row {
		
		abstract int rowIndex();
		
		@Override
		public boolean isNull(int columnIndex) {
			return DataFrameImpl.this.isNull(rowIndex(), columnIndex);
		}

		@Override
		public boolean isNull(String columnName) {
			return DataFrameImpl.this.isNull(rowIndex(), columnName);
		}

		@Override
		public <T> T get(int columnIndex) {
			return DataFrameImpl.this.get(rowIndex(), columnIndex);
		}

		@Override
		public <T> T get(String columnName) {
			return DataFrameImpl.this.get(rowIndex(), columnName);
		}

		@Override
		public String getString(int columnIndex) {
			return DataFrameImpl.this.getString(rowIndex(), columnIndex);
		}

		@Override
		public String getString(String columnName) {
			return DataFrameImpl.this.getString(rowIndex(), columnName);
		}

		@Override
		public boolean getBoolean(int columnIndex) {
			return DataFrameImpl.this.getBoolean(rowIndex(), columnIndex);
		}

		@Override
		public boolean getBoolean(String columnName) {
			return DataFrameImpl.this.getBoolean(rowIndex(), columnName);
		}

		@Override
		public int getInt(int columnIndex) {
			return DataFrameImpl.this.getInt(rowIndex(), columnIndex);
		}

		@Override
		public int getInt(String columnName) {
			return DataFrameImpl.this.getInt(rowIndex(), columnName);
		}

		@Override
		public long getLong(int columnIndex) {
			return DataFrameImpl.this.getLong(rowIndex(), columnIndex);
		}

		@Override
		public long getLong(String columnName) {
			return DataFrameImpl.this.getLong(rowIndex(), columnName);
		}

		@Override
		public double getDouble(int columnIndex) {
			return DataFrameImpl.this.getDouble(rowIndex(), columnIndex);
		}

		@Override
		public double getDouble(String columnName) {
			return DataFrameImpl.this.getDouble(rowIndex(), columnName);
		}

		@Override
		public float getFloat(int columnIndex) {
			return DataFrameImpl.this.getFloat(rowIndex(), columnIndex);
		}

		@Override
		public float getFloat(String columnName) {
			return DataFrameImpl.this.getFloat(rowIndex(), columnName);
		}

		@Override
		public LocalDate getDate(int columnIndex) {
			return DataFrameImpl.this.getDate(rowIndex(), columnIndex);
		}

		@Override
		public LocalDate getDate(String columnName) {
			return DataFrameImpl.this.getDate(rowIndex(), columnName);
		}

		@Override
		public int yyyymmdd(int columnIndex) {
			return DataFrameImpl.this.yyyymmdd(rowIndex(), columnIndex);
		}

		@Override
		public int yyyymmdd(String columnName) {
			return DataFrameImpl.this.yyyymmdd(rowIndex(), columnName);
		}

		@Override
		public LocalDateTime getDateTime(int columnIndex) {
			return DataFrameImpl.this.getDateTime(rowIndex(), columnIndex);
		}

		@Override
		public LocalDateTime getDateTime(String columnName) {
			return DataFrameImpl.this.getDateTime(rowIndex(), columnName);
		}

		@Override
		public int columnCount() {
			return DataFrameImpl.this.columnCount();
		}
	}
	
	private class RowImpl extends AbstractRow {

		private final int rowIndex;
		
		private RowImpl(int rowIndex) {
			this.rowIndex = rowIndex;
		}

		@Override
		int rowIndex() {
			return rowIndex;
		}
	}
	
	private class CursorImpl extends AbstractRow implements Cursor {
		
		private int rowIndex;
		
		private CursorImpl(int rowIndex) {
			this.rowIndex = rowIndex;
		}

		@Override
		int rowIndex() {
			return rowIndex;
		}

		@Override
		public boolean hasNext() {
			return rowIndex < size();
		}

		@Override
		public void next() {
			if(!hasNext())
				throw new NoSuchElementException("called next when hasNext is false");
			
			rowIndex++;
		}

		@Override
		public boolean hasPrevious() {
			return rowIndex > 0;
		}

		@Override
		public void previous() {
			if(!hasPrevious())
				throw new NoSuchElementException("called previous when hasPrevious is false");
			
			rowIndex--;
		}

		@Override
		public int currentIndex() {
			return rowIndex;
		}
	}
	

	/*--------------------------------------------------------------------------------
	 *	org.apache.commons.lang3
	 *--------------------------------------------------------------------------------*/
	private static class ArrayUtils {
		/**
	     * Randomly permutes the elements of the specified array using the Fisher-Yates algorithm.
	     *
	     * @param array   the array to shuffle
	     * @see <a href="https://en.wikipedia.org/wiki/Fisher%E2%80%93Yates_shuffle">Fisher-Yates shuffle algorithm</a>
	     * @since 3.6
	     */
	    public static void shuffle(final int[] array) {
	        shuffle(array, new Random());
	    }

	    /**
	     * Randomly permutes the elements of the specified array using the Fisher-Yates algorithm.
	     *
	     * @param array   the array to shuffle
	     * @param random  the source of randomness used to permute the elements
	     * @see <a href="https://en.wikipedia.org/wiki/Fisher%E2%80%93Yates_shuffle">Fisher-Yates shuffle algorithm</a>
	     * @since 3.6
	     */
	    public static void shuffle(final int[] array, final Random random) {
	        for (int i = array.length; i > 1; i--) {
	            swap(array, i - 1, random.nextInt(i), 1);
	        }
	    }
	    
	    /**
	     * Swaps a series of elements in the given int array.
	     *
	     * <p>This method does nothing for a {@code null} or empty input array or
	     * for overflow indices. Negative indices are promoted to 0(zero). If any
	     * of the sub-arrays to swap falls outside of the given array, then the
	     * swap is stopped at the end of the array and as many as possible elements
	     * are swapped.</p>
	     *
	     * Examples:
	     * <ul>
	     *     <li>ArrayUtils.swap([1, 2, 3, 4], 0, 2, 1) -&gt; [3, 2, 1, 4]</li>
	     *     <li>ArrayUtils.swap([1, 2, 3, 4], 0, 0, 1) -&gt; [1, 2, 3, 4]</li>
	     *     <li>ArrayUtils.swap([1, 2, 3, 4], 2, 0, 2) -&gt; [3, 4, 1, 2]</li>
	     *     <li>ArrayUtils.swap([1, 2, 3, 4], -3, 2, 2) -&gt; [3, 4, 1, 2]</li>
	     *     <li>ArrayUtils.swap([1, 2, 3, 4], 0, 3, 3) -&gt; [4, 2, 3, 1]</li>
	     * </ul>
	     *
	     * @param array the array to swap, may be {@code null}
	     * @param offset1 the index of the first element in the series to swap
	     * @param offset2 the index of the second element in the series to swap
	     * @param len the number of elements to swap starting with the given indices
	     * @since 3.5
	     */
	    public static void swap(final int[] array,  int offset1, int offset2, int len) {
	        if (array == null || array.length == 0 || offset1 >= array.length || offset2 >= array.length) {
	            return;
	        }
	        if (offset1 < 0) {
	            offset1 = 0;
	        }
	        if (offset2 < 0) {
	            offset2 = 0;
	        }
	        len = Math.min(Math.min(len, array.length - offset1), array.length - offset2);
	        for (int i = 0; i < len; i++, offset1++, offset2++) {
	            final int aux = array[offset1];
	            array[offset1] = array[offset2];
	            array[offset2] = aux;
	        }
	    }
	}
}
