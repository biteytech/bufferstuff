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

import tech.bitey.bufferstuff.BufferBitSet;

class NullableLongColumn extends NullableColumn<Long, NonNullLongColumn, NullableLongColumn> implements LongColumn {
	
	static final NullableLongColumn EMPTY = new NullableLongColumn(NonNullLongColumn.EMPTY_LIST, EMPTY_NO_RESIZE, 0, 0);

	NullableLongColumn(NonNullLongColumn column, BufferBitSet nonNulls, int offset, int size) {
		super(column, nonNulls, offset, size);
	}

	@Override
	protected NullableLongColumn subColumn0(int fromIndex, int toIndex) {
		return new NullableLongColumn(column, nonNulls, fromIndex+offset, toIndex-fromIndex);
	}

	@Override
	protected NullableLongColumn empty() {
		return EMPTY;
	}

	@Override
	public double mean() {
		return nonNullSubColumn().mean();
	}

	@Override
	public long getLong(int index) {
		checkGetPrimitive(index);
		return column.getLong(nonNullIndex(index+offset));
	}

	@Override
	protected NullableLongColumn construct(NonNullLongColumn column, BufferBitSet nonNulls, int size) {
		return new NullableLongColumn(column, nonNulls, 0, size);
	}

	@Override
	protected boolean checkType(Object o) {
		return o instanceof Long;
	}
}