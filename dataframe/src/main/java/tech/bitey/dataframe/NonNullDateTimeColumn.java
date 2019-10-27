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

import static tech.bitey.dataframe.LongArrayPacker.LOCAL_DATE_TIME;

import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.Comparator;

class NonNullDateTimeColumn extends LongArrayColumn<LocalDateTime, NonNullDateTimeColumn> implements DateTimeColumn {

	static final NonNullDateTimeColumn EMPTY_LIST = new NonNullDateTimeColumn(ByteBuffer.allocate(0), 0, 0, false);
	static final NonNullDateTimeColumn EMPTY_SET = new NonNullDateTimeColumn(ByteBuffer.allocate(0), 0, 0, true);
	
	NonNullDateTimeColumn(ByteBuffer buffer, int offset, int size, boolean sortedSet) {
		super(buffer, LOCAL_DATE_TIME, offset, size, sortedSet);
	}

	@Override
	NonNullDateTimeColumn construct(ByteBuffer buffer, int offset, int size, boolean sortedSet) {
		return new NonNullDateTimeColumn(buffer, offset, size, sortedSet);
	}

	@Override
	protected NonNullDateTimeColumn empty() {
		return sortedSet ? EMPTY_SET : EMPTY_LIST;
	}
	
	@Override
	public Comparator<LocalDateTime> comparator() {
		return LocalDateTime::compareTo;
	}
	
	@Override
	public ColumnType getType() {
		return ColumnType.DATETIME;
	}

	@Override
	protected boolean checkType(Object o) {
		return o instanceof LocalDateTime;
	}
}
