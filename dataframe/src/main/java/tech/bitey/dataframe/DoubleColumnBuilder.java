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

import static tech.bitey.bufferstuff.BufferUtils.isSortedAndDistinct;
import static tech.bitey.dataframe.NonNullDoubleColumn.EMPTY_LIST;
import static tech.bitey.dataframe.NonNullDoubleColumn.EMPTY_SET;
import static tech.bitey.dataframe.guava.DfPreconditions.checkState;

import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;

import tech.bitey.bufferstuff.BufferBitSet;

public class DoubleColumnBuilder
		extends SingleBufferColumnBuilder<Double, DoubleBuffer, DoubleColumn, DoubleColumnBuilder> {

	DoubleColumnBuilder(boolean sortedSet) {
		super(sortedSet);
	}

	@Override
	protected void addNonNull(Double element) {
		add(element.doubleValue());
	}

	public DoubleColumnBuilder add(double element) {
		ensureAdditionalCapacity(1);
		elements.put(element);
		size++;
		return this;
	}

	public DoubleColumnBuilder addAll(double... elements) {
		ensureAdditionalCapacity(elements.length);
		this.elements.put(elements);
		size += elements.length;
		return this;
	}

	@Override
	protected DoubleColumn empty() {
		return sortedSet ? EMPTY_SET : EMPTY_LIST;
	}

	@Override
	protected void checkSortedAndDistinct() {
		checkState(isSortedAndDistinct(elements, 0, elements.position()),
				"column elements must be sorted and distinct");
	}

	@Override
	protected DoubleColumn wrapNullableColumn(DoubleColumn column, BufferBitSet nonNulls) {
		return new NullableDoubleColumn((NonNullDoubleColumn) column, nonNulls, 0, size);
	}

	@Override
	public ColumnType getType() {
		return ColumnType.DOUBLE;
	}

	@Override
	DoubleColumn buildNonNullColumn(ByteBuffer trim) {
		return new NonNullDoubleColumn(trim, 0, getNonNullSize(), sortedSet);
	}

	@Override
	DoubleBuffer asBuffer(ByteBuffer buffer) {
		return buffer.asDoubleBuffer();
	}

	@Override
	int elementSize() {
		return 8;
	}
}
