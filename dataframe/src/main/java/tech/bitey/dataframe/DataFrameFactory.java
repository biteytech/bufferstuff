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
}
