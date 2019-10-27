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

class NullableIntColumn extends NullableColumn<Integer, NonNullIntColumn, NullableIntColumn> implements IntColumn {
	
	static final NullableIntColumn EMPTY = new NullableIntColumn(NonNullIntColumn.EMPTY_LIST, EMPTY_NO_RESIZE, 0, 0);

	NullableIntColumn(NonNullIntColumn column, BufferBitSet nonNulls, int offset, int size) {
		super(column, nonNulls, offset, size);
	}

	@Override
	protected NullableIntColumn subColumn0(int fromIndex, int toIndex) {
		return new NullableIntColumn(column, nonNulls, fromIndex+offset, toIndex-fromIndex);
	}

	@Override
	protected NullableIntColumn empty() {
		return EMPTY;
	}

	@Override
	public double mean() {
		return nonNullSubColumn().mean();
	}

	@Override
	public int getInt(int index) {
		checkGetPrimitive(index);
		return column.getInt(nonNullIndex(index+offset));
	}

	@Override
	protected NullableIntColumn construct(NonNullIntColumn column, BufferBitSet nonNulls, int size) {
		return new NullableIntColumn(column, nonNulls, 0, size);
	}

	@Override
	protected boolean checkType(Object o) {
		return o instanceof Integer;
	}
}