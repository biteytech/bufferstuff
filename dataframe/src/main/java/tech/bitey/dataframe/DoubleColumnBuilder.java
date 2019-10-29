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

import static java.util.Spliterator.DISTINCT;
import static java.util.Spliterator.SORTED;
import static tech.bitey.bufferstuff.BufferUtils.isSorted;
import static tech.bitey.bufferstuff.BufferUtils.isSortedAndDistinct;
import static tech.bitey.dataframe.guava.DfPreconditions.checkState;

import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;

import tech.bitey.bufferstuff.BufferBitSet;

public class DoubleColumnBuilder
		extends SingleBufferColumnBuilder<Double, DoubleBuffer, DoubleColumn, DoubleColumnBuilder> {

	DoubleColumnBuilder(int characteristics) {
		super(characteristics);
	}

	@Override
	void addNonNull(Double element) {
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
	DoubleColumn empty() {
		return NonNullDoubleColumn.EMPTY.get(characteristics);
	}

	@Override
	void checkCharacteristics() {
		if((characteristics & DISTINCT) != 0) {
			checkState(isSortedAndDistinct(elements, 0, elements.position()),
				"column elements must be sorted and distinct");
		}
		else if((characteristics & SORTED) != 0) {
			checkState(isSorted(elements, 0, elements.position()),
				"column elements must be sorted");
		}
	}

	@Override
	DoubleColumn wrapNullableColumn(DoubleColumn column, BufferBitSet nonNulls) {
		return new NullableDoubleColumn((NonNullDoubleColumn) column, nonNulls, 0, size);
	}

	@Override
	public ColumnType getType() {
		return ColumnType.DOUBLE;
	}

	@Override
	DoubleColumn buildNonNullColumn(ByteBuffer trim, int characteristics) {
		return new NonNullDoubleColumn(trim, 0, getNonNullSize(), characteristics);
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
