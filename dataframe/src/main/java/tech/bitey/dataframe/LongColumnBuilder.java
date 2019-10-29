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

import java.nio.ByteBuffer;

import tech.bitey.bufferstuff.BufferBitSet;

public class LongColumnBuilder extends LongArrayColumnBuilder<Long, LongColumn, LongColumnBuilder> {

	LongColumnBuilder(int characteristics) {
		super(characteristics, LongArrayPacker.LONG);
	}

	@Override
	LongColumn empty() {
		return NonNullLongColumn.EMPTY.get(characteristics);
	}

	@Override
	LongColumn buildNonNullColumn(ByteBuffer trim, int characteristics) {
		return new NonNullLongColumn(trim, 0, getNonNullSize(), characteristics);
	}

	@Override
	LongColumn wrapNullableColumn(LongColumn column, BufferBitSet nonNulls) {
		return new NullableLongColumn((NonNullLongColumn)column, nonNulls, 0, size);
	}
	
	public LongColumnBuilder add(long element) {
		ensureAdditionalCapacity(1);
		elements.put(element);
		size++;
		return this;
	}
	
	public LongColumnBuilder addAll(long... elements) {
		ensureAdditionalCapacity(elements.length);
		this.elements.put(elements);
		size += elements.length;
		return this;
	}

	@Override
	public ColumnType getType() {
		return ColumnType.LONG;
	}
}
