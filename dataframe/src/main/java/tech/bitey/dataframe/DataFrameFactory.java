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

import static java.nio.ByteOrder.BIG_ENDIAN;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static tech.bitey.dataframe.DataFrame.MAGIC_NUMBER;
import static tech.bitey.dataframe.guava.DfPreconditions.checkArgument;
import static tech.bitey.dataframe.guava.DfPreconditions.checkState;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;

public enum DataFrameFactory {
	$; // singleton instance
	
	public DataFrame create(LinkedHashMap<String, Column<?>> columnMap, String keyColumnName) {
		return new DataFrameImpl(columnMap, keyColumnName);
	}
	
	public DataFrame create(LinkedHashMap<String, Column<?>> columnMap) {
		return create(columnMap, null);
	}
	
	public DataFrame create(Column<?>[] columns, String[] columnNames, String keyColumnName) {
		checkArgument(columns.length == columnNames.length, "columns vs column names, mismatched lengths");
		checkArgument(new HashSet<>(Arrays.asList(columnNames)).size() == columnNames.length, "column names must be distinct");
		
		LinkedHashMap<String, Column<?>> columnMap = new LinkedHashMap<>();
		
		for(int i = 0; i < columns.length; i++)
			columnMap.put(columnNames[i], columns[i]);
		
		return create(columnMap, keyColumnName);
	}
	
	public DataFrame create(Column<?>[] columns, String[] columnNames) {
		return create(columns, columnNames, null);
	}
	
	public DataFrame create(List<Column<?>> columns, List<String> columnNames, String keyColumnName) {
		return create(columns.toArray(new Column<?>[0]), columnNames.toArray(new String[0]), keyColumnName);
	}
	
	public DataFrame create(List<Column<?>> columns, List<String> columnNames) {
		return create(columns, columnNames, null);
	}	
	
	public DataFrame readFrom(File file) {
		try (
			RandomAccessFile raf = new RandomAccessFile(file, "r");
			FileChannel channel = raf.getChannel();
		) {
			ByteBuffer header1 = ByteBuffer.allocate(8 + 4 + 4).order(BIG_ENDIAN);
			channel.read(header1);
			header1.flip();
			
			checkState(header1.getLong() == MAGIC_NUMBER, "file must begin with magic number");
			
			final int version = header1.get();
			checkState(version == 1, "unsupported version ("+version+")");
			
			// skip reserved bytes
			header1.position(header1.position()+3);
			
			// column count
			final int cc = header1.getInt();
//			System.out.println("column count : "+cc);
			
			ByteBuffer header2 = ByteBuffer.allocate(cc*2 + cc + cc*4 + 4 + 1).order(BIG_ENDIAN);
			channel.read(header2);
			header2.flip();
			
			// column types
			final ColumnType[] types = new ColumnType[cc];
			byte[] codeBytes = new byte[2];			
			for(int i = 0; i < cc; i++) {
				header2.get(codeBytes);
				types[i] = ColumnType.valueOf(codeBytes);
			}
//			System.out.println("types        : "+Arrays.toString(types));
			
			// column flags
			byte[] flags = new byte[cc];
			header2.get(flags);

			boolean[] isBigEndian = new boolean[cc];
			boolean[] isNullable = new boolean[cc];
			boolean[] isSortedSet = new boolean[cc];
			Integer keyColumn = null;
			for(int i = 0; i < cc; i++) {
				if((flags[i] & DataFrame.BIG_ENDIAN_FLAG) != 0)
					isBigEndian[i] = true;
				if((flags[i] & DataFrame.NULLABILITY_FLAG) != 0)
					isNullable[i] = true;
				if((flags[i] & DataFrame.SORTED_FLAG) != 0)
					isSortedSet[i] = true;
				if((flags[i] & DataFrame.KEY_COLUMN_FLAG) != 0)
					keyColumn = i;
			}
//			System.out.println("isBigEndian  : "+Arrays.toString(isBigEndian));
//			System.out.println("isNullable   : "+Arrays.toString(isNullable));
//			System.out.println("isSortedSet  : "+Arrays.toString(isSortedSet));
//			System.out.println("keyColumn    : "+keyColumn);
			
			// column lengths
			int[] lengths = new int[cc];
			for(int i = 0; i < cc; i++)
				lengths[i] = header2.getInt();
//			System.out.println("col lengths  : "+Arrays.toString(lengths));
			
			// column name length
			int colNameLength = header2.getInt();
//			System.out.println("col name len : "+colNameLength);
			
			// column name byte order
			ByteOrder nameOrder = header2.get() == 0 ? LITTLE_ENDIAN : BIG_ENDIAN;
//			System.out.println("col name endi: "+nameOrder);
			
			// column names
			ByteBuffer names = ByteBuffer.allocate(colNameLength).order(nameOrder);
			channel.read(names);
			StringColumn columnNames = NonNullStringColumn.fromBuffer(names, 0, colNameLength, false);
//			System.out.println("col names    : "+columnNames);
			
			// column data
			Column<?>[] columns = new Column<?>[cc];
			for(int i = 0; i < cc; i++) {
				ByteBuffer data = ByteBuffer.allocate(lengths[i]).order(isBigEndian[i] ? BIG_ENDIAN : LITTLE_ENDIAN);
				channel.read(data);
				data.flip();
				Column<?> column = types[i].fromBuffer(data, 0, lengths[i], isSortedSet[i], isNullable[i]);
				columns[i] = column;
			}
			
			return DataFrameFactory.$.create(columns, columnNames.toArray(new String[0]), 
					keyColumn == null ? null : columnNames.get(keyColumn));
		}
		catch(Exception e) {
			throw new RuntimeException("failed to read dataframe from: "+file, e);
		}
	}
}
