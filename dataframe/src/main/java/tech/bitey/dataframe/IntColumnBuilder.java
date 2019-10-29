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

public class IntColumnBuilder extends IntArrayColumnBuilder<Integer, IntColumn, IntColumnBuilder> {

	IntColumnBuilder(int characteristics) {
		super(characteristics, IntArrayPacker.INTEGER);
	}

	@Override
	IntColumn empty() {
		return NonNullIntColumn.EMPTY.get(characteristics);
	}

	@Override
	IntColumn buildNonNullColumn(ByteBuffer trim, int characteristics) {
		return new NonNullIntColumn(trim, 0, getNonNullSize(), characteristics);
	}

	@Override
	IntColumn wrapNullableColumn(IntColumn column, BufferBitSet nonNulls) {
		return new NullableIntColumn((NonNullIntColumn)column, nonNulls, 0, size);
	}
	
	public IntColumnBuilder add(int element) {
		ensureAdditionalCapacity(1);
		elements.put(element);
		size++;
		return this;
	}
	
	public IntColumnBuilder addAll(int... elements) {
		ensureAdditionalCapacity(elements.length);
		this.elements.put(elements);
		size += elements.length;
		return this;
	}

	@Override
	public ColumnType getType() {
		return ColumnType.INT;
	}
}
