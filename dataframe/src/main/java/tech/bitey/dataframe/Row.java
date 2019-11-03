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

import java.time.LocalDate;
import java.time.LocalDateTime;

public interface Row {

	int columnCount();

	/**
	 * Returns row index in parent dataframe, ranging from zero to dataframe size
	 * (exclusive).
	 * 
	 * @return row index in parent dataframe, ranging from zero to dataframe size
	 *         (exclusive).
	 */
	int rowIndex();

	boolean isNull(int columnIndex);

	boolean isNull(String columnName);

	<T> T get(int columnIndex);

	<T> T get(String columnName);

	String getString(int columnIndex);

	String getString(String columnName);

	boolean getBoolean(int columnIndex);

	boolean getBoolean(String columnName);

	int getInt(int columnIndex);

	int getInt(String columnName);

	default int getOrDefaultInt(int columnIndex, int defaultValue) {
		return isNull(columnIndex) ? defaultValue : getInt(columnIndex);
	}

	default int getOrDefaultInt(String columnName, int defaultValue) {
		return isNull(columnName) ? defaultValue : getInt(columnName);
	}

	long getLong(int columnIndex);

	long getLong(String columnName);

	default long getOrDefaultLong(int columnIndex, long defaultValue) {
		return isNull(columnIndex) ? defaultValue : getLong(columnIndex);
	}

	default long getOrDefaultLong(String columnName, long defaultValue) {
		return isNull(columnName) ? defaultValue : getLong(columnName);
	}

	double getDouble(int columnIndex);

	double getDouble(String columnName);

	default double getOrDefaultDouble(int columnIndex, double defaultValue) {
		return isNull(columnIndex) ? defaultValue : getDouble(columnIndex);
	}

	default double getOrDefaultDouble(String columnName, double defaultValue) {
		return isNull(columnName) ? defaultValue : getDouble(columnName);
	}

	float getFloat(int columnIndex);

	float getFloat(String columnName);

	default float getOrDefaultFloat(int columnIndex, float defaultValue) {
		return isNull(columnIndex) ? defaultValue : getFloat(columnIndex);
	}

	default float getOrDefaultFloat(String columnName, float defaultValue) {
		return isNull(columnName) ? defaultValue : getFloat(columnName);
	}

	LocalDate getDate(int columnIndex);

	LocalDate getDate(String columnName);

	int yyyymmdd(int columnIndex);

	int yyyymmdd(String columnName);

	LocalDateTime getDateTime(int columnIndex);

	LocalDateTime getDateTime(String columnName);
}
