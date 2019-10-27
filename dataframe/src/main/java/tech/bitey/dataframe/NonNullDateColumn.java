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

import static tech.bitey.dataframe.IntArrayPacker.LOCAL_DATE;
import static tech.bitey.dataframe.guava.DfPreconditions.checkElementIndex;

import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.Comparator;

class NonNullDateColumn extends IntArrayColumn<LocalDate, NonNullDateColumn> implements DateColumn {

	static final NonNullDateColumn EMPTY_LIST = new NonNullDateColumn(ByteBuffer.allocate(0), 0, 0, false);
	static final NonNullDateColumn EMPTY_SET = new NonNullDateColumn(ByteBuffer.allocate(0), 0, 0, true);
	
	NonNullDateColumn(ByteBuffer buffer, int offset, int size, boolean sortedSet) {
		super(buffer, LOCAL_DATE, offset, size, sortedSet);
	}

	@Override
	NonNullDateColumn construct(ByteBuffer buffer, int offset, int size, boolean sortedSet) {
		return new NonNullDateColumn(buffer, offset, size, sortedSet);
	}

	@Override
	protected NonNullDateColumn empty() {
		return sortedSet ? EMPTY_SET : EMPTY_LIST;
	}
	
	@Override
	public Comparator<LocalDate> comparator() {
		return LocalDate::compareTo;
	}

	@Override
	public int yyyymmdd(int index) {
		checkElementIndex(index, size);
		int packed = at(index+offset);
		return (packed >>> 16)*10000 + ((packed & 0xFF00) >>> 8)*100 + (packed & 0xFF);
	}
	
	@Override
	public ColumnType getType() {
		return ColumnType.DATE;
	}

	@Override
	protected String oracleType() {
		return "DATE";
	}

	@Override
	protected boolean checkType(Object o) {
		return o instanceof LocalDate;
	}
}
