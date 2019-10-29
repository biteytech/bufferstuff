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

import static tech.bitey.dataframe.NonNullBooleanColumn.EMPTY;
import static tech.bitey.bufferstuff.ResizeBehavior.ALLOCATE_DIRECT;

import tech.bitey.bufferstuff.BufferBitSet;

public class BooleanColumnBuilder extends ColumnBuilder<Boolean, BooleanColumn, BooleanColumnBuilder> {

	BooleanColumnBuilder() { 
		super(0);
	}

	private int nonNullSize = 0;
	private BufferBitSet elements = new BufferBitSet(ALLOCATE_DIRECT);

	@Override
	void addNonNull(Boolean element) {
		if(element)
			elements.set(size);
		size++;
		nonNullSize++;
	}
	
	public BooleanColumnBuilder add(boolean element) {
		if(element)
			elements.set(size);
		size++;
		nonNullSize++;
		return this;
	}
	
	public BooleanColumnBuilder addAll(boolean... elements) {
		for(int i = 0; i < elements.length; i++) {
			if(elements[i])
				this.elements.set(size);
		}
		size += elements.length;
		nonNullSize += elements.length;
		return this;
	}

	@Override
	void ensureAdditionalCapacity(int size) {
		// noop
	}

	@Override
	BooleanColumn empty() {
		return EMPTY;
	}

	@Override
	int getNonNullSize() {
		return nonNullSize;
	}

	@Override
	void checkCharacteristics() {
		throw new IllegalStateException();
	}

	@Override
	BooleanColumn buildNonNullColumn(int characteristics) {
		return new NonNullBooleanColumn(elements, 0, nonNullSize);
	}

	@Override
	BooleanColumn wrapNullableColumn(BooleanColumn column, BufferBitSet nonNulls) {
		return new NullableBooleanColumn((NonNullBooleanColumn)column, nonNulls, 0, size);
	}

	@Override
	public ColumnType getType() {
		return ColumnType.BOOLEAN;
	}

	@Override
	void append0(BooleanColumnBuilder tail) {
		
		BufferBitSet elements = tail.elements.shiftRight(this.nonNullSize);				
		elements.or(this.elements);
		this.elements = elements;
		
		this.nonNullSize += tail.nonNullSize;
	}
}
