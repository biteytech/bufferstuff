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

import static tech.bitey.dataframe.LongArrayPacker.LONG;
import static tech.bitey.dataframe.guava.DfPreconditions.checkElementIndex;

import java.nio.ByteBuffer;
import java.util.Comparator;

class NonNullLongColumn extends LongArrayColumn<Long, NonNullLongColumn> implements LongColumn {

	static final NonNullLongColumn EMPTY_LIST = new NonNullLongColumn(ByteBuffer.allocate(0), 0, 0, false);
	static final NonNullLongColumn EMPTY_SET = new NonNullLongColumn(ByteBuffer.allocate(0), 0, 0, false);
	
	NonNullLongColumn(ByteBuffer buffer, int offset, int size, boolean sortedSet) {
		super(buffer, LONG, offset, size, sortedSet);
	}

	@Override
	NonNullLongColumn construct(ByteBuffer buffer, int offset, int size, boolean sortedSet) {
		return new NonNullLongColumn(buffer, offset, size, sortedSet);
	}

	@Override
	public double mean() {
		long sum = 0;
		for(int i = 0; i < size; i++)
			sum += at(i+offset);
		return sum / (double)size;
	}

	@Override
	protected NonNullLongColumn empty() {
		return sortedSet ? EMPTY_SET : EMPTY_LIST;
	}
	
	@Override
	public Comparator<Long> comparator() {
		return Long::compareTo;
	}
	
	@Override
	public ColumnType getType() {
		return ColumnType.LONG;
	}

	@Override
	public long getLong(int index) {
		checkElementIndex(index, size);
		return at(index+offset);
	}

	@Override
	protected boolean checkType(Object o) {
		return o instanceof Long;
	}
}
