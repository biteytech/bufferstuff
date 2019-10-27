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

class NullableFloatColumn extends NullableColumn<Float, NonNullFloatColumn, NullableFloatColumn> implements FloatColumn {
	
	static final NullableFloatColumn EMPTY = new NullableFloatColumn(NonNullFloatColumn.EMPTY_LIST, EMPTY_NO_RESIZE, 0, 0);

	NullableFloatColumn(NonNullFloatColumn column, BufferBitSet nonNulls, int offset, int size) {
		super(column, nonNulls, offset, size);
	}

	@Override
	protected NullableFloatColumn subColumn0(int fromIndex, int toIndex) {
		return new NullableFloatColumn(column, nonNulls, fromIndex+offset, toIndex-fromIndex);
	}

	@Override
	protected NullableFloatColumn empty() {
		return EMPTY;
	}

	@Override
	public double mean() {
		return nonNullSubColumn().mean();
	}

	@Override
	public float getFloat(int index) {
		checkGetPrimitive(index);
		return column.getFloat(nonNullIndex(index+offset));
	}

	@Override
	protected NullableFloatColumn construct(NonNullFloatColumn column, BufferBitSet nonNulls, int size) {
		return new NullableFloatColumn(column, nonNulls, 0, size);
	}

	@Override
	protected boolean checkType(Object o) {
		return o instanceof Float;
	}
}
